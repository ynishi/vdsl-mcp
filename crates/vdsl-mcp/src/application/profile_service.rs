//! Profile service: parse, validate, resolve secrets, and expand a
//! [`ProfileManifest`] into a [`BatchPlan`].
//!
//! The plan is then handed to `batch_service` (Subtask 2) for
//! execution against a [`VdslMcpServer`](crate::interface::mcp::VdslMcpServer).
//!
//! # Phase layout (design §4.2)
//!
//! | # | Phase              | Shape                 | Tool(s)                       |
//! |---|--------------------|-----------------------|-------------------------------|
//! | 1 | `system.apt`       | leaf                  | `exec`                        |
//! | 2 | ComfyUI install    | leaf                  | `exec`                        |
//! | 3 | `python.deps`      | leaf                  | `exec`                        |
//! | 4 | `custom_nodes`     | group (parallel 4)    | `exec`                        |
//! | 5 | sync routes        | group (parallel 4)    | `exec` (rclone for pull, marker append for push) |
//! | 6 | (unused)           | —                     | —                             |
//! | 7 | models             | group (parallel 1)    | `exec` (rclone for b2://, cp for file://) |
//! | 8 | `hooks.post_install` | leaf                | `exec`                        |
//! | 9 | ComfyUI restart    | leaf pair             | `task_run` + `task_status`    |
//! |10 | health check       | leaf                  | `comfy_api`                   |
//!
//! # Placeholder syntax (consumed by batch_service)
//!
//! Some step argument values cannot be known at plan-construction
//! time. Rather than pre-resolving, the plan embeds placeholder
//! strings that `batch_service` pattern-matches at dispatch time.
//! Two placeholder varieties exist:
//!
//! ```text
//! "__result:{step_id}.{field}"   — resolved from the accumulated
//!                                  results of earlier steps
//! "__secret:{NAME}"              — resolved from the secrets map
//!                                  (see `resolve_secrets`)
//! ```
//!
//! Subtask 1 produces both; Subtask 2 resolves both.
//!
//! # Secrets are never in the plan
//!
//! [`BatchPlan`] contains no plaintext secret values. Every secret
//! reference in `manifest.env` is emitted as the placeholder string
//! `"__secret:NAME"` in `args.env`; the resolved value is held
//! separately in a `HashMap<String, String>` that flows to
//! `batch_service` via the apply handler.
//!
//! Redaction of the plan for logging / `dry_run` output is
//! unnecessary because secret values are never embedded in it.
//! Missing-secret fail-fast is preserved: the apply handler calls
//! [`resolve_secrets`] before [`expand_phases`], so an unset env
//! variable surfaces as [`ProfileError::MissingSecrets`] at entry
//! rather than at step dispatch.
//!
//! # Cross-subtask contract (K-8)
//!
//! [`BatchPlan`], [`BatchStep`], [`StepEntry`], [`GroupBlock`],
//! [`ValidateBlock`], [`BatchResult`], [`BatchStepResult`],
//! [`StepStatus`], and [`PlanMode`] are the contract with
//! `batch_service` (Subtask 2). Field renames / removals here
//! require matching updates there.
//!
//! # No DSL-bypass: apply is the only allowed pod-op surface
//!
//! Every pod-side file operation that is part of apply / setup /
//! staging must be expressible in the Profile DSL and emitted as a
//! [`BatchStep`] by `expand_phases`. Callers (tools, higher-level
//! orchestration, scripts) must NOT reach past this module to
//! issue hand-rolled `mv` / `cp` / `ln` / `rclone` / `wget` /
//! `curl` against the pod via `vdsl_exec` or `vdsl_task_run`.
//!
//! When a concrete operation is not expressible here, the fix is
//! to extend the DSL (Lua `profile.lua` + this module in
//! lockstep) — not to work around it. See
//! `docs/profile-and-orchestration.md` §2.5 and the project
//! `vdsl/.claude/CLAUDE.md` "Profile Evaluation Bypass" section
//! for the rationale, concrete patterns (staging.push,
//! non-preset subdirs, new source schemes), and the 2026-04-21
//! accident record.

use crate::domain::profile::{
    EnvValue, LlmModel, Model, ProfileManifest, ServicePlatform, SyncRoute, PROFILE_SCHEMA,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::HashMap;

// =============================================================================
// Error type
// =============================================================================

/// Errors surfaced by [`parse_manifest`], [`resolve_secrets`], and
/// [`expand_phases`].
#[derive(Debug, thiserror::Error)]
pub enum ProfileError {
    #[error("unsupported schema: expected '{expected}', got '{got}'")]
    UnsupportedSchema { expected: &'static str, got: String },

    #[error("missing secret(s): {}", .0.join(", "))]
    MissingSecrets(Vec<String>),

    #[error("invalid manifest: {0}")]
    InvalidManifest(String),

    #[error("phase expansion failed: {0}")]
    ExpansionFailed(String),

    /// User profile declared a secret in `manifest.env`. Profiles are
    /// non-secret runtime config only; credentials are MCP-owned and
    /// auto-injected during apply. See `docs/profile-and-orchestration.md`
    /// §2.4 and `vdsl/.claude/CLAUDE.md` "Profile Secret Policy".
    #[error("secret in user env[{key}]: {reason}")]
    SecretInUserEnv { key: String, reason: String },
}

// =============================================================================
// Batch plan types (shared with batch_service; K-8 contract)
// =============================================================================

/// A compiled batch plan produced by [`expand_phases`].
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BatchPlan {
    pub mode: PlanMode,
    pub steps: Vec<StepEntry>,
    pub dry_run: bool,
}

/// Batch execution mode.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum PlanMode {
    /// Iterate `steps` in array order, stop on first failure.
    Seq,
    /// Topological sort on `depends_on`, fan out where independent.
    Dag,
}

/// One entry in a [`BatchPlan::steps`] list: either a single leaf
/// step or a parallel group.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum StepEntry {
    Leaf(BatchStep),
    Group(GroupBlock),
}

/// A single dispatchable step.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BatchStep {
    /// Unique identifier within the plan (used for `depends_on`
    /// and `__result:` placeholders).
    pub id: String,

    /// MCP tool name without the `vdsl_` prefix (e.g. `"exec"`).
    pub tool: String,

    /// Raw tool arguments. Shape depends on the target tool.
    pub args: serde_json::Value,

    /// DAG mode dependency list. Empty in `seq` mode.
    #[serde(default)]
    pub depends_on: Vec<String>,

    /// Optional validation + retry config.
    #[serde(default)]
    pub validate: Option<ValidateBlock>,
}

/// A group of steps run concurrently with a `parallel` cap.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GroupBlock {
    /// Optional id for result aggregation.
    #[serde(default)]
    pub id: Option<String>,
    /// Max concurrent steps (default 4 applied by callers).
    pub parallel: usize,
    /// Leaf steps only — groups cannot nest further.
    pub steps: Vec<BatchStep>,
}

/// Validator / retry block. If present, the step is re-executed once
/// after a validator failure.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ValidateBlock {
    #[serde(default)]
    pub file_exists: Vec<String>,
    #[serde(default)]
    pub min_size: Option<u64>,
}

/// Per-step result.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BatchStepResult {
    pub id: String,
    pub status: StepStatus,
    #[serde(default)]
    pub output: Option<String>,
    #[serde(default)]
    pub error: Option<String>,
}

/// Step completion status.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum StepStatus {
    Ok,
    Failed,
    Skipped,
}

/// Overall batch result.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BatchResult {
    pub plan_id: String,
    pub results: Vec<BatchStepResult>,
    pub dry_run: bool,
}

// =============================================================================
// Public API
// =============================================================================

/// Parse a profile manifest JSON string and validate its schema tag.
///
/// Returns [`ProfileError::UnsupportedSchema`] if `schema` is not
/// [`PROFILE_SCHEMA`]. Returns [`ProfileError::InvalidManifest`] if
/// the JSON does not deserialize.
pub fn parse_manifest(input: &str) -> Result<ProfileManifest, ProfileError> {
    let manifest: ProfileManifest = serde_json::from_str(input).map_err(|e| {
        tracing::warn!(error = %e, "profile manifest parse failed");
        ProfileError::InvalidManifest(format!("json parse error: {e}"))
    })?;

    if manifest.schema != PROFILE_SCHEMA {
        tracing::warn!(
            expected = PROFILE_SCHEMA,
            got = %manifest.schema,
            "profile manifest has unsupported schema tag"
        );
        return Err(ProfileError::UnsupportedSchema {
            expected: PROFILE_SCHEMA,
            got: manifest.schema,
        });
    }

    validate_user_env(&manifest)?;

    Ok(manifest)
}

/// Substrings (case-insensitive) that mark an env key as secret-shaped.
/// Must stay in sync with `lua/vdsl/runtime/profile.lua:SECRET_KEY_SUBSTRINGS`.
const SECRET_KEY_SUBSTRINGS: &[&str] = &[
    "KEY", "SECRET", "TOKEN", "PASSWORD", "PWD", "AUTH", "CRED", "APIKEY",
];

/// Reject any user-supplied secret in `manifest.env`. Two shapes are
/// forbidden and both must be caught here so the MCP layer fails loud
/// even if a caller bypasses the Lua DSL:
///
/// 1. `EnvValue::Secret` — i.e. the user wrote `{"__secret": "NAME"}`
///    in `env`. The `__secret` sentinel is an MCP-internal emission
///    format (see `env_value` / `build_model_step`); user manifests
///    must not contain it.
/// 2. A key name containing any of [`SECRET_KEY_SUBSTRINGS`] (case-
///    insensitive). Profile.env is non-secret runtime config only.
///    Credentials are MCP-owned and auto-injected during apply.
///
/// Mirrors `lua/vdsl/runtime/profile.lua:normalize_env` — defence in
/// depth so the mcp-direct-call path cannot sneak secrets through.
fn validate_user_env(manifest: &ProfileManifest) -> Result<(), ProfileError> {
    for (key, value) in &manifest.env {
        if matches!(value, EnvValue::Secret(_)) {
            return Err(ProfileError::SecretInUserEnv {
                key: key.clone(),
                reason: "user profiles must not embed `{\"__secret\": ...}` sentinels; \
                     MCP emits them internally during apply"
                    .to_string(),
            });
        }
        let upper = key.to_ascii_uppercase();
        if let Some(hit) = SECRET_KEY_SUBSTRINGS.iter().find(|s| upper.contains(*s)) {
            return Err(ProfileError::SecretInUserEnv {
                key: key.clone(),
                reason: format!(
                    "key contains secret-shaped substring {hit:?}; \
                     Profile.env is for non-secret runtime config only, \
                     credentials are MCP-owned and auto-injected during apply"
                ),
            });
        }
    }
    Ok(())
}

/// Resolve every `{"__secret": "NAME"}` entry in `manifest.env` via
/// `std::env::var`. Returns a `HashMap<env_key, value>` suitable for
/// injecting into `exec` step args.
///
/// # Collect-all semantics
///
/// If any secret is missing, iteration does NOT stop — all missing
/// names are accumulated before returning
/// [`ProfileError::MissingSecrets`] (see §1-2-2 no silent drops and
/// design §4.4 "fail fast with the list of all missing names").
pub fn resolve_secrets(
    manifest: &ProfileManifest,
) -> Result<HashMap<String, String>, ProfileError> {
    let mut resolved = HashMap::new();
    let mut missing: Vec<String> = Vec::new();

    for (key, value) in &manifest.env {
        match value {
            EnvValue::Plain(s) => {
                resolved.insert(key.clone(), s.clone());
            }
            EnvValue::Secret(s) => match std::env::var(&s.name) {
                Ok(v) => {
                    resolved.insert(key.clone(), v);
                }
                Err(_) => {
                    missing.push(s.name.clone());
                }
            },
        }
    }

    // Auto-inject B2 creds when any apply step needs them. Profiles must
    // NOT declare these in `env` (that would leak creds into every
    // phase's shell). Instead we pull them from the MCP process env on
    // demand and scope them per-step via placeholder env (see
    // `build_model_step`, `build_sync_pull_step`, `build_staging_push_step`).
    // Keeps the pod dumb: creds only exist during the rclone call, not
    // in ~/.bashrc or any persistent location.
    //
    // Trigger conditions (any one is enough):
    //   - any model with b2:// src           (Phase 7 model pull)
    //   - any sync.pull route with b2:// src (Phase 5 sync pull)
    //   - any staging.push route with b2:// dst (Phase 5 staging push)
    // sync.push is marker-only and does NOT hit rclone, so it does not
    // trigger B2 auto-inject.
    let needs_b2 = manifest.models.iter().any(|m| m.src.starts_with("b2://"))
        || manifest
            .sync
            .as_ref()
            .map(|s| s.pull.iter().any(|r| r.src.starts_with("b2://")))
            .unwrap_or(false)
        || manifest
            .staging
            .as_ref()
            .map(|s| s.push.iter().any(|r| r.dst.starts_with("b2://")))
            .unwrap_or(false);
    if needs_b2 {
        for name in B2_PULL_SECRET_NAMES {
            if resolved.contains_key(*name) {
                continue;
            }
            match std::env::var(name) {
                Ok(v) if !v.is_empty() => {
                    resolved.insert((*name).to_string(), v);
                }
                _ => missing.push((*name).to_string()),
            }
        }
    }

    // HF_TOKEN is optional: public repos work without it. We pick it up
    // if present so private repos work, but missing is not a hard error.
    let needs_hf = manifest.models.iter().any(|m| m.src.starts_with("hf://"));
    if needs_hf {
        for name in HF_PULL_SECRET_NAMES {
            if resolved.contains_key(*name) {
                continue;
            }
            if let Ok(v) = std::env::var(name) {
                if !v.is_empty() {
                    resolved.insert((*name).to_string(), v);
                }
            }
        }
    }
    if !missing.is_empty() {
        missing.sort();
        missing.dedup();
        tracing::warn!(missing = ?missing, "profile manifest references unset secret(s)");
        return Err(ProfileError::MissingSecrets(missing));
    }

    Ok(resolved)
}

/// MCP-side env var names that `build_model_step` injects into a
/// b2:// model step's per-step `env` as `__secret:NAME` placeholders.
/// `resolve_secrets` pre-populates these in the secrets map so the
/// apply handler fails fast if any are unset.
const B2_PULL_SECRET_NAMES: &[&str] = &["VDSL_B2_KEY_ID", "VDSL_B2_KEY"];

/// MCP-side env var names injected for hf:// model pulls.
const HF_PULL_SECRET_NAMES: &[&str] = &["HF_TOKEN"];

/// Expand a manifest into a concrete [`BatchPlan`] targeting `pod_id`.
///
/// Sync routes (`sync.pull` / `sync.push`) are self-describing — each
/// route declares `b2://<bucket>/<path>` on the cloud side and an
/// absolute pod-local path on the pod side (docs §2.3). Pull routes
/// run as blocking `rclone` exec steps in Phase 5; push routes are
/// recorded to `/workspace/.vdsl/push_routes.jsonl` on the pod for
/// later consumption by generation flows (apply itself does not
/// trigger uploads). No pre-registered SDK topology is required.
///
/// `dry_run` is forwarded onto the resulting [`BatchPlan`] unchanged.
///
/// # Secrets
///
/// Secret values are **not** embedded in the returned plan. Every
/// `EnvValue::Secret { name }` in `manifest.env` is emitted as the
/// placeholder string `"__secret:NAME"` inside `args.env`. The
/// caller is expected to have invoked [`resolve_secrets`] separately
/// (for fail-fast validation) and to pass the resolved map to
/// `batch_service` for dispatch-time substitution. See module-level
/// doc comment.
pub fn expand_phases(
    manifest: &ProfileManifest,
    pod_id: &str,
    dry_run: bool,
) -> Result<BatchPlan, ProfileError> {
    let mut steps: Vec<StepEntry> = Vec::new();

    // Shared env object injected into every exec step.
    // Plaintext values pass through; secrets become "__secret:NAME"
    // placeholders resolved by batch_service at dispatch time.
    for key in manifest.env.keys() {
        assert_shell_safe(key, "env[].key")?;
    }
    let env_json = env_value(&manifest.env);
    // Port only matters when a `comfyui` block is present (Phase 2 / 9 /
    // 10). Otherwise we skip those phases entirely.
    let comfy_port = manifest
        .comfyui
        .as_ref()
        .and_then(|c| c.port)
        .unwrap_or(8188);

    // ---- Phase 1: system.apt ----
    if let Some(system) = &manifest.system {
        if !system.apt.is_empty() {
            for pkg in &system.apt {
                assert_shell_safe(pkg, "system.apt")?;
            }
            let pkgs = system.apt.join(" ");
            steps.push(StepEntry::Leaf(exec_bg_step(
                "1_system_apt",
                &format!(
                    "apt-get update && DEBIAN_FRONTEND=noninteractive \
                     apt-get install -y {pkgs}"
                ),
                pod_id,
                &env_json,
            )));
        }
    }

    // ---- Phase 2: ComfyUI install (skipped when comfyui block absent) ----
    if let Some(comfyui) = &manifest.comfyui {
        assert_shell_safe(&comfyui.ref_, "comfyui.ref")?;
        let comfy_repo = comfyui.repo.as_deref().unwrap_or("comfyanonymous/ComfyUI");
        assert_shell_safe(comfy_repo, "comfyui.repo")?;
        steps.push(StepEntry::Leaf(exec_bg_step(
            "2_comfyui_install",
            &comfyui_install_script(comfy_repo, &comfyui.ref_),
            pod_id,
            &env_json,
        )));
    }

    // ---- Phase 3: python version check (advisory) + python.deps ----
    if let Some(python) = &manifest.python {
        // Phase 3a: warn if python.version is set and mismatches pod python3.
        //
        // Skip when version equals the vdsl runtime default ("3.12"). The
        // runtime's `normalize_python` injects this default whenever the
        // user omits `python.version`, so emitting the check unconditionally
        // makes every apply (including ones with no explicit version) carry
        // a Phase 3a step that is meaningful only when the user actually
        // chose 3.12 deliberately. The advisory has no effect at the default,
        // so skipping it keeps the BatchPlan minimal and avoids confusing
        // pollers ("why is there a 3a step when I didn't ask for one?").
        const PYTHON_DEFAULT_VERSION: &str = "3.12";
        if let Some(want) = &python.version {
            if want != PYTHON_DEFAULT_VERSION {
                assert_shell_safe(want, "python.version")?;
                let warn_script = format!(
                "set -e\n\
                 actual=$(python3 -c 'import sys; print(f\"{{sys.version_info.major}}.{{sys.version_info.minor}}\")')\n\
                 want={want}\n\
                 if [ \"$actual\" != \"$want\" ]; then\n\
                   echo \"WARN: python.version={want} requested but pod python3 reports $actual; proceeding with $actual\" >&2\n\
                 else\n\
                   echo \"python version match: $actual\"\n\
                 fi"
                );
                steps.push(StepEntry::Leaf(exec_step(
                    "3a_python_version_check",
                    &warn_script,
                    pod_id,
                    &env_json,
                )));
            }
        }
        // Phase 3: install python deps.
        //
        // Pip target depends on whether a `comfyui` block is present:
        //   - present: install into the ComfyUI venv (custom_nodes,
        //     ComfyUI itself, and python.deps share one env)
        //   - absent (vllm-only / pure-LLM workload): install into the
        //     base image's system python — there is no ComfyUI venv to
        //     target and `cd /workspace/ComfyUI` would fail
        //
        // `force_reinstall` is opt-in. Required when a base-image
        // package must be replaced (e.g. torch 2.4 → 2.10 for vllm
        // 0.18.1, see workspace/qwen3.6-vllm-runpod-setup.md §Step 3).
        if !python.deps.is_empty() {
            for dep in &python.deps {
                assert_shell_safe(dep, "python.deps")?;
            }
            let deps = python.deps.join(" ");
            let pip_cmd = if manifest.comfyui.is_some() {
                "cd /workspace/ComfyUI && .venv/bin/pip install".to_string()
            } else {
                "python3 -m pip install".to_string()
            };
            let force_flag = if python.force_reinstall.unwrap_or(false) {
                " --force-reinstall"
            } else {
                ""
            };
            steps.push(StepEntry::Leaf(exec_bg_step(
                "3_python_deps",
                &format!("{pip_cmd}{force_flag} {deps}"),
                pod_id,
                &env_json,
            )));
        }
    }

    // ---- Phase 4: custom_nodes (parallel group) ----
    if !manifest.custom_nodes.is_empty() {
        let mut node_steps: Vec<BatchStep> = Vec::with_capacity(manifest.custom_nodes.len());
        for (idx, node) in manifest.custom_nodes.iter().enumerate() {
            assert_shell_safe(&node.name, "custom_nodes[].name")?;
            assert_shell_safe(&node.repo, "custom_nodes[].repo")?;
            if let Some(r) = &node.ref_ {
                assert_shell_safe(r, "custom_nodes[].ref")?;
            }
            let clone_url = expand_repo_url(&node.repo);
            let script = custom_node_install_script(
                &node.name,
                &clone_url,
                node.ref_.as_deref(),
                node.pip.unwrap_or(false),
            );
            node_steps.push(exec_bg_step(
                &format!("4_custom_node_{idx}"),
                &script,
                pod_id,
                &env_json,
            ));
        }
        steps.push(StepEntry::Group(GroupBlock {
            id: Some("4_custom_nodes".to_string()),
            parallel: 4,
            steps: node_steps,
        }));
    }

    // ---- Phase 5: sync routes + staging (parallel group) ----
    // Pull         : blocking rclone exec (b2://<bucket>/<path>/ -> /<pod-path>/).
    // sync.push    : marker-only exec that appends to
    //                /workspace/.vdsl/push_routes.jsonl for later
    //                generation-flow consumption. Not executed at apply.
    // staging.push : eager blocking rclone exec (/<pod-path>/ -> b2://).
    //                One-shot pre-apply staging — fires during apply, not
    //                during generation.
    let mut route_steps: Vec<BatchStep> = Vec::new();
    if let Some(sync) = &manifest.sync {
        for (idx, route) in sync.pull.iter().enumerate() {
            validate_pull_route(route)?;
            route_steps.push(build_sync_pull_step(idx, route, pod_id, &env_json)?);
        }
        for (idx, route) in sync.push.iter().enumerate() {
            validate_push_route(route)?;
            route_steps.push(build_sync_push_step(idx, route, pod_id, &env_json)?);
        }
    }
    if let Some(staging) = &manifest.staging {
        for (idx, route) in staging.push.iter().enumerate() {
            validate_staging_push_route(route)?;
            route_steps.push(build_staging_push_step(idx, route, pod_id, &env_json)?);
        }
    }
    if !route_steps.is_empty() {
        steps.push(StepEntry::Group(GroupBlock {
            id: Some("5_sync_routes".to_string()),
            parallel: 4,
            steps: route_steps,
        }));
    }

    // Phase 6 is unused after the URL-scheme route redesign:
    // pulls are blocking rclone execs in Phase 5, so there is no
    // task_id to poll. Intentionally emit no steps at this index.

    // ---- Phase 7: models (serial group) ----
    // Serialized intentionally: each model is already large (GBs) and
    // rclone parallelizes within a single transfer (multi-threaded
    // chunks). Stacking concurrent rclones saturates network/disk and
    // also multiplies SSH sessions per pod — both hurt more than they
    // help. Keeping `parallel: 1` gives deterministic ordering and
    // clearer per-step error reporting when a single model fails.
    if !manifest.models.is_empty() {
        let mut model_steps: Vec<BatchStep> = Vec::with_capacity(manifest.models.len());
        for (idx, model) in manifest.models.iter().enumerate() {
            model_steps.push(build_model_step(idx, model, pod_id, &env_json)?);
        }
        steps.push(StepEntry::Group(GroupBlock {
            id: Some("7_models".to_string()),
            parallel: 1,
            steps: model_steps,
        }));
    }

    // ---- Phase 7b: llm_models (raw LLM weight staging, serial) ----
    // Independent of Phase 7 — `models[]` targets ComfyUI's models tree,
    // `llm_models[]` targets arbitrary dst_dir for non-ComfyUI workloads.
    // Serial for the same reason: each repo is large, parallel pulls
    // saturate network/disk.
    if !manifest.llm_models.is_empty() {
        let mut llm_steps: Vec<BatchStep> = Vec::with_capacity(manifest.llm_models.len());
        for (idx, lm) in manifest.llm_models.iter().enumerate() {
            llm_steps.push(build_llm_model_step(idx, lm, pod_id, &env_json)?);
        }
        steps.push(StepEntry::Group(GroupBlock {
            id: Some("7b_llm_models".to_string()),
            parallel: 1,
            steps: llm_steps,
        }));
    }

    // ---- Phase 8: hooks.post_install ----
    if let Some(hooks) = &manifest.hooks {
        if let Some(script) = &hooks.post_install {
            if !script.trim().is_empty() {
                steps.push(StepEntry::Leaf(exec_bg_step(
                    "8_post_install",
                    script,
                    pod_id,
                    &env_json,
                )));
            }
        }
    }

    // ---- Phase 9 / 10 are COMFYUI-only (skipped when block absent) ----
    // A profile with no `comfyui` block is declaring "this apply has no
    // ComfyUI runtime to restart or health-check" (e.g. a pure
    // volume-evacuation profile that only emits `staging.push`). Emitting
    // a restart step in that case would force a wasted git clone / pip
    // install to produce a process we're not going to use.
    if let Some(comfyui) = &manifest.comfyui {
        // ---- Phase 9: ComfyUI restart ----
        // Single `exec_bg` step. The restart script internally bounds
        // itself at ~210s (30s port-free wait + 180s curl-ready wait),
        // and launches the server via `nohup ... &` so the python
        // process survives the SSH session exit.
        //
        // DSL emits args as an array of tokens; validate each token
        // individually (no spaces) then shell-join with a single space
        // for the launch command.
        let args_vec = comfyui.args.as_deref().unwrap_or(&[] as &[String]);
        for tok in args_vec {
            assert_shell_safe(tok, "comfyui.args[]")?;
        }
        let extra_args = args_vec.join(" ");
        let restart_script = comfyui_restart_script(comfy_port, &extra_args);
        steps.push(StepEntry::Leaf(exec_bg_step(
            "9_comfyui_restart",
            &restart_script,
            pod_id,
            &env_json,
        )));

        // ---- Phase 10: health check ----
        // Pass `pod_id` explicitly so comfy_api auto-constructs the
        // RunPod proxy URL (`https://<pod_id>-8188.proxy.runpod.net`).
        // Without it the tool falls through to the default "connect
        // first" guard and fails with -32602.
        steps.push(StepEntry::Leaf(BatchStep {
            id: "10_health".to_string(),
            tool: "comfy_api".to_string(),
            args: serde_json::json!({
                "pod_id": pod_id,
                "method": "GET",
                "path": "/object_info",
            }),
            depends_on: Vec::new(),
            validate: None,
        }));
    }

    // ---- Phase 11: generic services ----
    // Reject duplicate service names — log files would collide on
    // /workspace/.vdsl/service_{name}.log and ready_check messages
    // would mis-attribute failures.
    {
        let mut seen = std::collections::HashSet::new();
        for service in &manifest.services {
            if !seen.insert(service.name.as_str()) {
                return Err(ProfileError::InvalidManifest(format!(
                    "duplicate services[].name: '{}'",
                    service.name
                )));
            }
        }
    }
    for (idx, service) in manifest.services.iter().enumerate() {
        assert_shell_safe(&service.name, "services[].name")?;
        let launch = build_service_launch_cmd(&service.platform)?;

        // Launch step (detached). Writes pid to a file so the readiness
        // poll can detect a service that dies during the readiness wait
        // (e.g. vllm crashes on `sock.bind()` only after a ~10-30s
        // import phase, well past any sleep-N + `kill -0` settle check
        // we could put here). The 1s `kill -0` still catches the
        // fastest-fail cases (binary missing / arg parse error).
        //
        // 2026-04-30 accident: vllm port-bind conflict (8188 already
        // taken by ComfyUI bundled in default RunPod template) crashed
        // the daemon ~10s after launch. The pre-fix start step was
        // marked ok at sleep 1 because vllm was still loading, and
        // ready_check then waited the full 600s timeout instead of
        // failing in seconds.
        steps.push(StepEntry::Leaf(exec_bg_step(
            &format!("11_service_{idx}_start"),
            &format!(
                "set -e\n\
                 mkdir -p /workspace/.vdsl\n\
                 nohup {launch} > /workspace/.vdsl/service_{name}.log 2>&1 &\n\
                 pid=$!\n\
                 echo $pid > /workspace/.vdsl/service_{name}.pid\n\
                 sleep 1\n\
                 kill -0 $pid 2>/dev/null || {{ \
                   echo 'service {name} died immediately' >&2; \
                   tail -100 /workspace/.vdsl/service_{name}.log >&2; \
                   exit 1; \
                 }}\n",
                launch = launch,
                name = service.name
            ),
            pod_id,
            &env_json,
        )));

        // Readiness poll step. Each iteration also checks that the
        // launched pid is still alive — a daemon that crashed during
        // import / bind must fail readiness immediately, not after the
        // full timeout. See the accident note on the start step.
        if let Some(check) = &service.ready_check {
            let timeout = check.timeout_sec.unwrap_or(300);
            steps.push(StepEntry::Leaf(exec_bg_step(
                &format!("11_service_{idx}_ready"),
                &format!(
                    "pid_file=/workspace/.vdsl/service_{name}.pid\n\
                     i=0\n\
                     until curl -sf {url} >/dev/null; do\n\
                       pid=$(cat \"$pid_file\" 2>/dev/null)\n\
                       if [ -z \"$pid\" ] || ! kill -0 \"$pid\" 2>/dev/null; then\n\
                         echo 'service {name} died during readiness wait' >&2\n\
                         tail -100 /workspace/.vdsl/service_{name}.log >&2\n\
                         exit 1\n\
                       fi\n\
                       i=$((i+1))\n\
                       if [ $i -ge {timeout} ]; then\n\
                         echo 'service {name} not ready after {timeout}s' >&2\n\
                         tail -100 /workspace/.vdsl/service_{name}.log >&2\n\
                         exit 1\n\
                       fi\n\
                       sleep 1\n\
                     done\n",
                    url = check.http,
                    timeout = timeout,
                    name = service.name
                ),
                pod_id,
                &env_json,
            )));
        }
    }

    Ok(BatchPlan {
        mode: PlanMode::Seq,
        steps,
        dry_run,
    })
}

/// SHA-256 hex digest of the manifest JSON bytes as supplied.
///
/// This function does NOT canonicalize — it hashes the exact bytes
/// passed in. Matching `profile:hash_source()` on the vdsl side therefore
/// requires the caller to pass the identical byte stream the vdsl runtime
/// produced (typically the JSON string emitted by the Lua DSL, forwarded
/// through without pretty-printing / key reordering / whitespace changes).
///
/// If the two sides ever serialize differently (key order, escaping,
/// trailing newline), hashes will diverge. Treat this as a byte-identity
/// check, not a semantic-equivalence check.
pub fn compute_profile_hash(manifest_json: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(manifest_json.as_bytes());
    format!("{:x}", hasher.finalize())
}

// =============================================================================
// Shell-safety validation
// =============================================================================

/// Characters allowed in manifest fields that are interpolated into shell
/// scripts. Rejects shell metacharacters to prevent command injection.
///
/// Allowed: `[A-Za-z0-9._/@:+-=~]` — covers package names, git refs, URLs
/// (after scheme strip), and file paths. **Spaces are NOT allowed** because
/// unquoted interpolation into shell commands causes word splitting.
/// Use [`is_shell_safe_with_spaces`] for fields that legitimately contain
/// spaces (e.g. `comfyui.args`).
fn is_shell_safe(s: &str) -> bool {
    !s.is_empty()
        && s.chars()
            .all(|c| c.is_ascii_alphanumeric() || "-._/@:+=~".contains(c))
}

fn assert_shell_safe(value: &str, field: &str) -> Result<(), ProfileError> {
    if is_shell_safe(value) {
        Ok(())
    } else {
        Err(ProfileError::InvalidManifest(format!(
            "field '{field}' contains unsafe characters: {value:?}"
        )))
    }
}

// =============================================================================
// Private helpers
// =============================================================================

/// Build the `args.env` JSON object for exec-style steps.
///
/// Plain values pass through verbatim. Secret references become
/// `"__secret:NAME"` placeholder strings — the plan never carries
/// plaintext secret values. `batch_service` resolves these at
/// dispatch time using the `HashMap<String, String>` produced
/// separately by [`resolve_secrets`].
fn env_value(env: &HashMap<String, EnvValue>) -> serde_json::Value {
    let mut map = serde_json::Map::with_capacity(env.len());
    for (key, value) in env {
        let entry = match value {
            EnvValue::Plain(s) => serde_json::Value::String(s.clone()),
            EnvValue::Secret(secret_ref) => {
                serde_json::Value::String(format!("__secret:{}", secret_ref.name))
            }
        };
        map.insert(key.clone(), entry);
    }
    serde_json::Value::Object(map)
}

/// Default per-step exec timeout (seconds). The underlying
/// `vdsl_exec` tool defaults to 30s which is too short for install /
/// pip / apt phases. 30 minutes covers a cold `pip install -r
/// requirements.txt` pulling torch + native deps on a fresh venv
/// while still failing loudly on true hangs.
const DEFAULT_EXEC_TIMEOUT_SECS: u64 = 3600;

/// Build a standard `exec` step shape.
fn exec_step(id: &str, script: &str, pod_id: &str, env: &serde_json::Value) -> BatchStep {
    exec_step_with_timeout(id, script, pod_id, env, DEFAULT_EXEC_TIMEOUT_SECS)
}

fn exec_step_with_timeout(
    id: &str,
    script: &str,
    pod_id: &str,
    env: &serde_json::Value,
    timeout_secs: u64,
) -> BatchStep {
    BatchStep {
        id: id.to_string(),
        tool: "exec".to_string(),
        args: serde_json::json!({
            "pod_id": pod_id,
            "script": script,
            "env": env,
            "timeout": timeout_secs,
        }),
        depends_on: Vec::new(),
        validate: None,
    }
}

/// Build a background-exec step (tool = `"exec_bg"`). Same arg shape
/// as [`exec_step`] — `batch_service`'s `exec_bg` dispatch launches
/// via `task_run` and polls `task_status` until the job reaches
/// `state == "done"`. Use for any phase that may run longer than
/// the SSH idle-drop threshold (~5-10 min for some proxies);
/// holding a single SSH channel for a 20-min pip install reliably
/// hangs the RunPod proxy and blocks the caller for the full
/// per-step timeout. See the 2026-04-22 accident log in the project
/// `CLAUDE.md`.
fn exec_bg_step(id: &str, script: &str, pod_id: &str, env: &serde_json::Value) -> BatchStep {
    exec_bg_step_with_timeout(id, script, pod_id, env, DEFAULT_EXEC_TIMEOUT_SECS)
}

fn exec_bg_step_with_timeout(
    id: &str,
    script: &str,
    pod_id: &str,
    env: &serde_json::Value,
    timeout_secs: u64,
) -> BatchStep {
    BatchStep {
        id: id.to_string(),
        tool: "exec_bg".to_string(),
        args: serde_json::json!({
            "pod_id": pod_id,
            "script": script,
            "env": env,
            "timeout": timeout_secs,
        }),
        depends_on: Vec::new(),
        validate: None,
    }
}

/// Expand a repo reference into a git clone URL.
///
/// Accepts `owner/name` slug (GitHub assumed) or a full `https://` /
/// `git@` URL. Slug form is expanded to `https://github.com/owner/name.git`.
fn expand_repo_url(repo: &str) -> String {
    if repo.starts_with("https://") || repo.starts_with("git@") || repo.starts_with("http://") {
        repo.to_string()
    } else {
        format!("https://github.com/{repo}.git")
    }
}

/// Regex (extended POSIX) used by Phase 2 / Phase 4 to strip torch-family
/// lines from any `requirements.txt` before pip-installing it.
///
/// ComfyUI's own `requirements.txt` and many custom-node `requirements.txt`
/// files pin a torch wheel that overrides the pod base image's
/// driver-matched torch (`--system-site-packages` does not help once a
/// satisfying wheel is also installed inside the venv). The result is a
/// CUDA mismatch (e.g. cu128 wheel against a cu124 driver) that surfaces
/// only as `torch.cuda.is_available() == False` and silent ComfyUI startup
/// failure at Phase 9.
///
/// Keeping a single source of truth for this filter prevents Phase 2 and
/// Phase 4 drifting apart.
const TORCH_FAMILY_FILTER_REGEX: &str = r"^[[:space:]]*(torch|torchvision|torchaudio|xformers|bitsandbytes|triton)([[:space:]=<>~!;]|$)";

/// Resolve a script-override env var to rendered script content.
///
/// When `env_var` is set, its value is treated as a filesystem path. The
/// file is read and `${TOKEN}` placeholders are substituted from
/// `placeholders`. On read failure, returns `None` and emits a `tracing::warn`
/// so the caller falls back to the built-in default. On success, returns
/// `Some(rendered)`.
///
/// Placeholders use shell-style `${TOKEN}` so that override files remain
/// editable as bash with their host editor's syntax highlighting intact.
/// Unknown `${TOKEN}` left in the body is logged as a `tracing::warn` but
/// otherwise left in place — bash itself will resolve genuine env-var
/// references at execution time on the pod.
fn load_script_override(env_var: &str, placeholders: &[(&str, &str)]) -> Option<String> {
    let path = std::env::var(env_var).ok()?;
    let path = path.trim();
    if path.is_empty() {
        return None;
    }
    // Expand a leading `~/` so user-level paths work without manual env eval.
    let expanded: std::path::PathBuf = if let Some(rest) = path.strip_prefix("~/") {
        match std::env::var("HOME") {
            Ok(home) => std::path::PathBuf::from(home).join(rest),
            Err(_) => std::path::PathBuf::from(path),
        }
    } else {
        std::path::PathBuf::from(path)
    };
    match std::fs::read_to_string(&expanded) {
        Ok(body) => {
            let mut rendered = body;
            for (token, value) in placeholders {
                rendered = rendered.replace(&format!("${{{token}}}"), value);
            }
            tracing::info!(
                env = env_var,
                path = %expanded.display(),
                "script override applied"
            );
            Some(rendered)
        }
        Err(e) => {
            tracing::warn!(
                env = env_var,
                path = %expanded.display(),
                error = %e,
                "script override read failed; falling back to built-in default"
            );
            None
        }
    }
}

/// ComfyUI checkout + venv + requirements install script (phase 2).
///
/// `repo` accepts either a `owner/name` slug or a full `https://` URL.
/// A slug is expanded to `https://github.com/owner/name.git`.
///
/// Override via `VDSL_SCRIPT_COMFYUI_INSTALL=<file path>` — the file is
/// read as bash with `${REPO_URL}` and `${GIT_REF}` substituted by the
/// loader. See [`load_script_override`].
fn comfyui_install_script(repo: &str, git_ref: &str) -> String {
    let url = expand_repo_url(repo);
    if let Some(rendered) = load_script_override(
        "VDSL_SCRIPT_COMFYUI_INSTALL",
        &[("REPO_URL", &url), ("GIT_REF", git_ref)],
    ) {
        return rendered;
    }
    // --system-site-packages: inherit the base image's pre-compiled
    // torch (driver-matched on RunPod pytorch images). Without this,
    // `pip install torch` pulls whatever latest wheel exists and
    // usually requires a newer CUDA driver than the pod provides, so
    // `torch.cuda.is_available()` silently returns False.
    //
    // ComfyUI's own requirements.txt also gets the same torch-family
    // filter as Phase 4 custom_nodes — see TORCH_FAMILY_FILTER_REGEX.
    // Without it, an upstream pin (e.g. torch>=2.7) silently shadows
    // the system wheel inside the venv.
    //
    // Final smoke: assert torch.cuda.is_available(). A driver mismatch
    // here used to surface only at Phase 9 restart as a silent exit 1;
    // failing Phase 2 loudly is materially easier to diagnose.
    format!(
        "set -e\n\
         cd /workspace\n\
         test -d ComfyUI || git clone {url}\n\
         cd ComfyUI\n\
         git fetch --all --tags && git checkout {git_ref}\n\
         test -d .venv || python3 -m venv --system-site-packages .venv\n\
         .venv/bin/pip uninstall -y \
           nvidia-cublas-cu13 nvidia-cudnn-cu13 nvidia-cuda-runtime-cu13 \
           nvidia-cuda-cupti-cu13 nvidia-cuda-nvrtc-cu13 nvidia-cufft-cu13 \
           nvidia-curand-cu13 nvidia-cusolver-cu13 nvidia-cusparse-cu13 \
           nvidia-cusparselt-cu12 nvidia-nccl-cu13 nvidia-nvjitlink-cu13 \
           nvidia-nvtx-cu13 2>/dev/null || true\n\
         .venv/bin/pip install --upgrade pip\n\
         grep -viE '{regex}' requirements.txt \
           | .venv/bin/pip install -r /dev/stdin\n\
         .venv/bin/python -c 'import torch, sys; \
           ok = torch.cuda.is_available(); \
           print(f\"torch={{torch.__version__}} cuda={{torch.version.cuda}} avail={{ok}}\"); \
           sys.exit(0 if ok else 1)'\n",
        regex = TORCH_FAMILY_FILTER_REGEX,
    )
}

/// Custom-node clone + optional checkout + optional pip install script
/// (phase 4). Custom-node `requirements.txt` files routinely pin a torch
/// wheel that upgrades past the driver — see [`TORCH_FAMILY_FILTER_REGEX`]
/// — so torch-family lines are stripped before pip install.
///
/// Override via `VDSL_SCRIPT_CUSTOM_NODE_INSTALL=<file path>` — the file
/// is read as bash with `${NODE_NAME}`, `${REPO_URL}`, `${GIT_REF}`,
/// `${PIP_FLAG}` (`"1"`/`"0"`) substituted. See [`load_script_override`].
fn custom_node_install_script(
    name: &str,
    clone_url: &str,
    git_ref: Option<&str>,
    pip: bool,
) -> String {
    let pip_flag = if pip { "1" } else { "0" };
    let git_ref_value = git_ref.unwrap_or("");
    if let Some(rendered) = load_script_override(
        "VDSL_SCRIPT_CUSTOM_NODE_INSTALL",
        &[
            ("NODE_NAME", name),
            ("REPO_URL", clone_url),
            ("GIT_REF", git_ref_value),
            ("PIP_FLAG", pip_flag),
        ],
    ) {
        return rendered;
    }
    let checkout = match git_ref {
        Some(r) => format!(" && cd {name} && git checkout {r}"),
        None => String::new(),
    };
    let pip_install = if pip {
        format!(
            " && if [ -f /workspace/ComfyUI/custom_nodes/{name}/requirements.txt ]; then \
               grep -viE '{regex}' \
                 /workspace/ComfyUI/custom_nodes/{name}/requirements.txt \
                 | /workspace/ComfyUI/.venv/bin/pip install -r /dev/stdin; \
             fi",
            regex = TORCH_FAMILY_FILTER_REGEX,
        )
    } else {
        String::new()
    };
    format!(
        "cd /workspace/ComfyUI/custom_nodes && \
         (test -d {name} || git clone {clone_url} {name}){checkout}{pip_install}"
    )
}

/// ComfyUI restart script (design §4.5). Runs inside a `task_run`
/// wrapper so the launching process can exit once HTTP answers.
///
/// Both wait loops are bounded: port release caps at 30s (old process
/// hung on close), HTTP readiness caps at 180s (cold-start + model
/// discovery). Exceeding either limit exits non-zero so Phase 9 fails
/// loudly instead of hanging forever.
///
/// Override via `VDSL_SCRIPT_COMFYUI_RESTART=<file path>` — the file is
/// read as bash with `${PORT}` and `${EXTRA_ARGS}` substituted by the
/// loader. See [`load_script_override`].
fn comfyui_restart_script(port: u16, extra_args: &str) -> String {
    // DSL-provided args are authoritative. Only inject default
    // `--listen 0.0.0.0 --port $PORT` when the manifest gave nothing.
    let launch_args = if extra_args.trim().is_empty() {
        "--listen 0.0.0.0 --port $PORT".to_string()
    } else {
        extra_args.to_string()
    };
    let port_str = port.to_string();
    if let Some(rendered) = load_script_override(
        "VDSL_SCRIPT_COMFYUI_RESTART",
        &[("PORT", &port_str), ("EXTRA_ARGS", &launch_args)],
    ) {
        return rendered;
    }
    // Kill whatever process is actually listening on $PORT, regardless
    // of whether it was launched via `.venv/bin/python` or the pod
    // image's system python (e.g. runpod-slim's pre-baked ComfyUI auto-
    // started at boot). The previous pgrep-on-argv approach missed the
    // latter entirely and silently left the wrong ComfyUI bound to the
    // port; apply would still report Phase 9 ok, but generation pointed
    // at a models-less instance. See the 2026-04-21 accident in the
    // project CLAUDE.md.
    //
    // ss -ltnpH gives "... users:(("python",pid=185,fd=5),...)"; we
    // extract the pid tokens. -p requires CAP_NET_ADMIN / root, which
    // is granted in the RunPod execution context. $$ (this shell) and
    // $PPID (the ssh-level parent) are excluded so the script cannot
    // accidentally kill its own ancestors.
    format!(
        "set -e\n\
         PORT={port}\n\
         # Ensure `ss` (iproute2) is available — some RunPod base images\n\
         # ship without it, and a missing `ss` silently no-ops the kill\n\
         # loop (bash `ss: command not found` -> stderr only, the `grep\n\
         # -q LISTEN` falls through immediately). See 2026-04-21\n\
         # runpod-slim incident in project CLAUDE.md. Idempotent — apt\n\
         # is a no-op if the package is already installed.\n\
         if ! command -v ss >/dev/null 2>&1; then\n\
           DEBIAN_FRONTEND=noninteractive apt-get update -q >/dev/null 2>&1 || true\n\
           DEBIAN_FRONTEND=noninteractive apt-get install -y -q iproute2 >/dev/null 2>&1 || true\n\
         fi\n\
         listener_pids() {{\n\
           ss -ltnpH \"sport = :$PORT\" 2>/dev/null \\\n\
             | grep -oE 'pid=[0-9]+' | cut -d= -f2 | sort -u\n\
         }}\n\
         pids=$(listener_pids)\n\
         for pid in $pids; do\n\
           [ \"$pid\" = \"$$\" ] && continue\n\
           [ \"$pid\" = \"$PPID\" ] && continue\n\
           kill \"$pid\" 2>/dev/null || true\n\
         done\n\
         i=0\n\
         while ss -ltnH \"sport = :$PORT\" | grep -q LISTEN; do\n\
           i=$((i+1))\n\
           if [ $i -eq 10 ]; then\n\
             # 10 s grace; escalate laggards to SIGKILL (pod supervisor\n\
             # may respawn between SIGTERM and the next loop tick).\n\
             for pid in $(listener_pids); do\n\
               [ \"$pid\" = \"$$\" ] && continue\n\
               [ \"$pid\" = \"$PPID\" ] && continue\n\
               kill -KILL \"$pid\" 2>/dev/null || true\n\
             done\n\
           fi\n\
           if [ $i -ge 30 ]; then\n\
             echo 'port $PORT still held after 30s' >&2\n\
             [ -f /workspace/.vdsl/comfyui.log ] && tail -100 /workspace/.vdsl/comfyui.log >&2\n\
             exit 1\n\
           fi\n\
           sleep 1\n\
         done\n\
         mkdir -p /workspace/.vdsl\n\
         cd /workspace/ComfyUI\n\
         nohup .venv/bin/python main.py {launch_args} \\\n\
           > /workspace/.vdsl/comfyui.log 2>&1 &\n\
         i=0\n\
         until curl -sf http://localhost:$PORT/ >/dev/null; do\n\
           i=$((i+1))\n\
           if [ $i -ge 180 ]; then\n\
             echo 'comfyui not ready after 180s' >&2\n\
             tail -100 /workspace/.vdsl/comfyui.log >&2\n\
             exit 1\n\
           fi\n\
           sleep 1\n\
         done\n"
    )
}

/// Build a phase-7 model step.
///
/// - `b2://<bucket>/<path>` — fetched on the pod via `rclone copyto`.
///   B2 credentials are injected per-step via placeholder env
///   (`__secret:VDSL_B2_KEY_ID` / `__secret:VDSL_B2_KEY`), resolved by
///   `batch_service` at dispatch time from the secrets map that
///   `resolve_secrets` populated from the MCP process env. Creds do
///   NOT land in any persistent file on the pod — they live only in
///   the transient rclone process env for the duration of the copy.
/// - `file://...` — copied on the pod via `exec cp`.
fn build_model_step(
    idx: usize,
    model: &Model,
    pod_id: &str,
    env: &serde_json::Value,
) -> Result<BatchStep, ProfileError> {
    assert_shell_safe(&model.subdir, "models[].subdir")?;
    assert_shell_safe(&model.dst, "models[].dst")?;

    let dst_path = format!("/workspace/ComfyUI/models/{}/{}", model.subdir, model.dst);
    let dst_dir = format!("/workspace/ComfyUI/models/{}", model.subdir);

    if let Some(rest) = model.src.strip_prefix("b2://") {
        assert_shell_safe(rest, "models[].src (b2 path)")?;
        // Split `<bucket>/<path>` — `rclone copyto` needs the fully
        // qualified `remote:bucket/path` form.
        let (bucket, object) = rest.split_once('/').ok_or_else(|| {
            ProfileError::InvalidManifest(format!(
                "b2:// src must be b2://<bucket>/<path>, got: {}",
                model.src
            ))
        })?;
        if object.is_empty() {
            return Err(ProfileError::InvalidManifest(format!(
                "b2:// src missing object path: {}",
                model.src
            )));
        }

        // Env-based rclone config keeps credentials out of argv
        // (visible via `ps`). They still live in the rclone process
        // env for the duration of the copy, readable via
        // /proc/<pid>/environ by same-uid processes — acceptable for
        // an ephemeral pod whose only tenant is this apply run.
        let script = format!(
            "set -e\n\
             command -v rclone >/dev/null 2>&1 || curl -sL https://rclone.org/install.sh | bash\n\
             mkdir -p {dst_dir}\n\
             export RCLONE_CONFIG_B2_TYPE=b2\n\
             export RCLONE_CONFIG_B2_ACCOUNT=\"$VDSL_B2_KEY_ID\"\n\
             export RCLONE_CONFIG_B2_KEY=\"$VDSL_B2_KEY\"\n\
             rclone copyto --progress b2:{bucket}/{object} {dst}\n",
            dst_dir = dst_dir,
            bucket = bucket,
            object = object,
            dst = dst_path,
        );
        let step_env = merge_b2_env(env);
        Ok(exec_bg_step(
            &format!("7_model_{idx}"),
            &script,
            pod_id,
            &step_env,
        ))
    } else if let Some(rest) = model.src.strip_prefix("file://") {
        assert_shell_safe(rest, "models[].src (file path)")?;
        let script = format!(
            "mkdir -p {dst_dir} && cp {src} {dst}",
            dst_dir = dst_dir,
            src = rest,
            dst = dst_path,
        );
        Ok(exec_bg_step(
            &format!("7_model_{idx}"),
            &script,
            pod_id,
            env,
        ))
    } else {
        tracing::warn!(src = %model.src, "unsupported model src scheme");
        Err(ProfileError::InvalidManifest(format!(
            "unsupported model src scheme: '{}' (expected 'b2://' or 'file://'; for hf:// use llm_models[])",
            model.src
        )))
    }
}

/// Build a Phase 7b llm_model step. Currently only `hf://<org>/<repo>`
/// is supported — pulled with `huggingface-cli download` into
/// `dst_dir`. HF_TOKEN is injected when the host env has it (required
/// only for private repos; public pulls work anonymously).
fn build_llm_model_step(
    idx: usize,
    lm: &LlmModel,
    pod_id: &str,
    env: &serde_json::Value,
) -> Result<BatchStep, ProfileError> {
    assert_shell_safe(&lm.dst_dir, "llm_models[].dst_dir")?;
    if let Some(rev) = &lm.revision {
        assert_shell_safe(rev, "llm_models[].revision")?;
    }
    let Some(repo) = lm.src.strip_prefix("hf://") else {
        return Err(ProfileError::InvalidManifest(format!(
            "unsupported llm_models src scheme: '{}' (expected 'hf://<org>/<repo>')",
            lm.src
        )));
    };
    assert_shell_safe(repo, "llm_models[].src (hf repo)")?;
    let revision_arg = lm
        .revision
        .as_deref()
        .map(|r| format!(" --revision \"{r}\""))
        .unwrap_or_default();
    let script = format!(
        "set -e\n\
         mkdir -p \"{dst_dir}\"\n\
         python3 -c 'import huggingface_hub' 2>/dev/null || pip install -q huggingface_hub\n\
         HF_TOKEN=\"${{HF_TOKEN:-}}\" huggingface-cli download \"{repo}\" --local-dir \"{dst_dir}\"{revision_arg}\n",
        dst_dir = lm.dst_dir,
        repo = repo,
        revision_arg = revision_arg,
    );
    let step_env = merge_hf_env(env);
    Ok(exec_bg_step(
        &format!("7b_llm_model_{idx}"),
        &script,
        pod_id,
        &step_env,
    ))
}

/// Build the launch shell command for a service platform. The command
/// is generated from the typed variant — there is no free-form `cmd`
/// field by design. New platforms require extending [`ServicePlatform`].
fn build_service_launch_cmd(platform: &ServicePlatform) -> Result<String, ProfileError> {
    match platform {
        ServicePlatform::Vllm {
            model,
            port,
            dtype,
            tensor_parallel_size,
            extra_args,
        } => {
            assert_shell_safe(model, "services[].vllm.model")?;
            if let Some(d) = dtype {
                assert_shell_safe(d, "services[].vllm.dtype")?;
            }
            for a in extra_args {
                if !is_shell_safe_with_spaces(a) {
                    return Err(ProfileError::InvalidManifest(format!(
                        "services[].vllm.extra_args contains unsafe token: {a:?}"
                    )));
                }
            }
            let mut cmd = format!("vllm serve \"{model}\" --port {port}");
            if let Some(tp) = tensor_parallel_size {
                cmd.push_str(&format!(" --tensor-parallel-size {tp}"));
            }
            if let Some(d) = dtype {
                cmd.push_str(&format!(" --dtype {d}"));
            }
            for a in extra_args {
                cmd.push(' ');
                cmd.push_str(a);
            }
            Ok(cmd)
        }
        ServicePlatform::Ollama { port, models: _ } => {
            // Daemon only — model pulls are out of scope for the launch
            // command (operator runs `ollama pull` via hooks.post_install
            // or a follow-up tool call after readiness).
            Ok(format!("OLLAMA_HOST=0.0.0.0:{port} ollama serve"))
        }
    }
}

/// Like [`is_shell_safe`] but additionally allows single spaces.
/// Intended for fields that legitimately carry multi-token strings
/// (e.g. `comfyui.args`, `services[].vllm.extra_args`). Double spaces
/// are rejected.
fn is_shell_safe_with_spaces(s: &str) -> bool {
    !s.is_empty()
        && s.chars()
            .all(|c| c.is_ascii_alphanumeric() || "-._/@:+=~ ".contains(c))
        && !s.contains("  ")
}

/// Clone `base_env` (the profile-global env object) and overlay
/// B2 credential placeholders for a single b2:// model step. The
/// placeholders are resolved to real values by `batch_service` at
/// dispatch time from the secrets map populated by `resolve_secrets`.
fn merge_b2_env(base_env: &serde_json::Value) -> serde_json::Value {
    let mut map = match base_env {
        serde_json::Value::Object(m) => m.clone(),
        _ => serde_json::Map::new(),
    };
    for name in B2_PULL_SECRET_NAMES {
        map.insert(
            (*name).to_string(),
            serde_json::Value::String(format!("__secret:{name}")),
        );
    }
    serde_json::Value::Object(map)
}

/// Similar to merge_b2_env, but for HF_TOKEN. Unlike B2 (required for
/// b2:// pulls), HF_TOKEN is optional — public repos work anonymously.
/// We emit the `__secret:HF_TOKEN` placeholder only when the host env
/// actually has it set, matching the optional-resolution path in
/// [`resolve_secrets`]. Missing HF_TOKEN with a private repo will fail
/// at huggingface-cli download time, not at dispatch.
fn merge_hf_env(base_env: &serde_json::Value) -> serde_json::Value {
    let mut map = match base_env {
        serde_json::Value::Object(m) => m.clone(),
        _ => serde_json::Map::new(),
    };
    for name in HF_PULL_SECRET_NAMES {
        if std::env::var(name).map(|v| !v.is_empty()).unwrap_or(false) {
            map.insert(
                (*name).to_string(),
                serde_json::Value::String(format!("__secret:{name}")),
            );
        }
    }
    serde_json::Value::Object(map)
}

/// Validate a `sync.pull` route — must be `b2://<bucket>/<path>` on
/// the src (cloud) side and an absolute pod-local path on the dst.
fn validate_pull_route(route: &SyncRoute) -> Result<(), ProfileError> {
    let b2_rest = route.src.strip_prefix("b2://").ok_or_else(|| {
        ProfileError::InvalidManifest(format!(
            "sync.pull src must be 'b2://<bucket>/<path>', got: {}",
            route.src
        ))
    })?;
    let (bucket, object) = b2_rest.split_once('/').ok_or_else(|| {
        ProfileError::InvalidManifest(format!(
            "sync.pull src must be 'b2://<bucket>/<path>', missing bucket or path: {}",
            route.src
        ))
    })?;
    assert_shell_safe(bucket, "sync.pull src (b2 bucket)")?;
    assert_shell_safe(object, "sync.pull src (b2 path)")?;
    if !route.dst.starts_with('/') {
        return Err(ProfileError::InvalidManifest(format!(
            "sync.pull dst must be an absolute pod path starting with '/', got: {}",
            route.dst
        )));
    }
    assert_no_path_traversal(&route.dst, "sync.pull dst")?;
    assert_shell_safe(&route.dst, "sync.pull dst")?;
    Ok(())
}

/// Validate a `sync.push` route — must be an absolute pod-local path
/// on the src side and `b2://<bucket>/<path>` on the dst (cloud) side.
/// The `{pod_id}` placeholder is allowed in the b2 path and substituted
/// at step-build time.
fn validate_push_route(route: &SyncRoute) -> Result<(), ProfileError> {
    if !route.src.starts_with('/') {
        return Err(ProfileError::InvalidManifest(format!(
            "sync.push src must be an absolute pod path starting with '/', got: {}",
            route.src
        )));
    }
    assert_no_path_traversal(&route.src, "sync.push src")?;
    assert_shell_safe(&route.src, "sync.push src")?;
    let b2_rest = route.dst.strip_prefix("b2://").ok_or_else(|| {
        ProfileError::InvalidManifest(format!(
            "sync.push dst must be 'b2://<bucket>/<path>', got: {}",
            route.dst
        ))
    })?;
    let (bucket, object) = b2_rest.split_once('/').ok_or_else(|| {
        ProfileError::InvalidManifest(format!(
            "sync.push dst must be 'b2://<bucket>/<path>', missing bucket or path: {}",
            route.dst
        ))
    })?;
    assert_shell_safe(bucket, "sync.push dst (b2 bucket)")?;
    // Allow `{pod_id}` placeholder — stripped before shell-safety check.
    let object_for_check = object.replace("{pod_id}", "");
    assert_shell_safe(&object_for_check, "sync.push dst (b2 path)")?;
    Ok(())
}

fn assert_no_path_traversal(path: &str, field: &str) -> Result<(), ProfileError> {
    if path.split('/').any(|seg| seg == "..") {
        return Err(ProfileError::InvalidManifest(format!(
            "field '{field}' contains '..' path traversal: {path}"
        )));
    }
    Ok(())
}

/// Build a Phase-5 pull step: blocking `rclone copyto` from the
/// `b2://<bucket>/<path>` source to the absolute pod destination.
fn build_sync_pull_step(
    idx: usize,
    route: &SyncRoute,
    pod_id: &str,
    env: &serde_json::Value,
) -> Result<BatchStep, ProfileError> {
    let b2_rest = route.src.strip_prefix("b2://").expect("validated");
    let (bucket, object) = b2_rest.split_once('/').expect("validated");
    // rclone copyto with a trailing-slash src copies directory
    // contents. Strip trailing slash from both so the shell command
    // reads naturally; mkdir -p on dst already handles the directory.
    let bucket = bucket.trim_end_matches('/');
    let object = object.trim_end_matches('/');
    let dst = route.dst.trim_end_matches('/');
    let script = format!(
        "set -e\n\
         command -v rclone >/dev/null 2>&1 || curl -sL https://rclone.org/install.sh | bash\n\
         mkdir -p {dst}\n\
         export RCLONE_CONFIG_B2_TYPE=b2\n\
         export RCLONE_CONFIG_B2_ACCOUNT=\"$VDSL_B2_KEY_ID\"\n\
         export RCLONE_CONFIG_B2_KEY=\"$VDSL_B2_KEY\"\n\
         rclone copyto --progress b2:{bucket}/{object} {dst}\n"
    );
    let step_env = merge_b2_env(env);
    Ok(exec_bg_step(
        &format!("5_sync_pull_{idx}"),
        &script,
        pod_id,
        &step_env,
    ))
}

/// Build a Phase-5 push step: append one JSON line to
/// `/workspace/.vdsl/push_routes.jsonl` on the pod. Apply itself never
/// triggers the upload; later generation flows consume this file.
/// `{pod_id}` placeholders in the dst are substituted at build time.
fn build_sync_push_step(
    idx: usize,
    route: &SyncRoute,
    pod_id: &str,
    env: &serde_json::Value,
) -> Result<BatchStep, ProfileError> {
    let dst_resolved = route.dst.replace("{pod_id}", pod_id);
    // Re-check shell safety of the resolved dst — pod_id is assumed
    // safe (comes from RunPod), but belt-and-suspenders.
    let b2_rest = dst_resolved
        .strip_prefix("b2://")
        .expect("validated by validate_push_route");
    let (bucket, object) = b2_rest.split_once('/').expect("validated");
    assert_shell_safe(bucket, "sync.push dst (resolved b2 bucket)")?;
    assert_shell_safe(object, "sync.push dst (resolved b2 path)")?;
    let src = &route.src;
    // Serialize the {src, dst} pair as a single JSON line. jq is not
    // assumed present; use printf with minimal escaping.
    let json_line = format!(r#"{{"src":"{src}","dst":"{dst_resolved}"}}"#);
    let script = format!(
        "set -e\n\
         mkdir -p /workspace/.vdsl\n\
         printf '%s\\n' '{json_line}' >> /workspace/.vdsl/push_routes.jsonl\n"
    );
    Ok(exec_step(
        &format!("5_sync_push_{idx}"),
        &script,
        pod_id,
        env,
    ))
}

/// Validate a `staging.push` route — absolute pod-local path on
/// the src side, `b2://<bucket>/<path>` on the dst. Same shape as
/// `sync.push` but fires eagerly during apply, so the label in
/// error messages differs. `{pod_id}` is allowed in the b2 path.
fn validate_staging_push_route(route: &SyncRoute) -> Result<(), ProfileError> {
    if !route.src.starts_with('/') {
        return Err(ProfileError::InvalidManifest(format!(
            "staging.push src must be an absolute pod path starting with '/', got: {}",
            route.src
        )));
    }
    assert_no_path_traversal(&route.src, "staging.push src")?;
    assert_shell_safe(&route.src, "staging.push src")?;
    let b2_rest = route.dst.strip_prefix("b2://").ok_or_else(|| {
        ProfileError::InvalidManifest(format!(
            "staging.push dst must be 'b2://<bucket>/<path>', got: {}",
            route.dst
        ))
    })?;
    let (bucket, object) = b2_rest.split_once('/').ok_or_else(|| {
        ProfileError::InvalidManifest(format!(
            "staging.push dst must be 'b2://<bucket>/<path>', missing bucket or path: {}",
            route.dst
        ))
    })?;
    assert_shell_safe(bucket, "staging.push dst (b2 bucket)")?;
    let object_for_check = object.replace("{pod_id}", "");
    assert_shell_safe(&object_for_check, "staging.push dst (b2 path)")?;
    Ok(())
}

/// Build a Phase-5 staging push step: eager `rclone copyto` from a
/// pod-absolute path to B2. Mirror of [`build_sync_pull_step`] with
/// the direction reversed. Secrets are emitted as `__secret:NAME`
/// placeholders in the step env; `batch_service` resolves them at
/// dispatch.
fn build_staging_push_step(
    idx: usize,
    route: &SyncRoute,
    pod_id: &str,
    env: &serde_json::Value,
) -> Result<BatchStep, ProfileError> {
    let dst_resolved = route.dst.replace("{pod_id}", pod_id);
    let b2_rest = dst_resolved
        .strip_prefix("b2://")
        .expect("validated by validate_staging_push_route");
    let (bucket, object) = b2_rest.split_once('/').expect("validated");
    assert_shell_safe(bucket, "staging.push dst (resolved b2 bucket)")?;
    assert_shell_safe(object, "staging.push dst (resolved b2 path)")?;
    let src = route.src.trim_end_matches('/');
    let bucket = bucket.trim_end_matches('/');
    let object = object.trim_end_matches('/');
    let script = format!(
        "set -e\n\
         command -v rclone >/dev/null 2>&1 || curl -sL https://rclone.org/install.sh | bash\n\
         export RCLONE_CONFIG_B2_TYPE=b2\n\
         export RCLONE_CONFIG_B2_ACCOUNT=\"$VDSL_B2_KEY_ID\"\n\
         export RCLONE_CONFIG_B2_KEY=\"$VDSL_B2_KEY\"\n\
         rclone copyto --progress {src} b2:{bucket}/{object}\n"
    );
    let step_env = merge_b2_env(env);
    Ok(exec_bg_step(
        &format!("5_staging_push_{idx}"),
        &script,
        pod_id,
        &step_env,
    ))
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
#[path = "profile_service_tests.rs"]
mod tests;
