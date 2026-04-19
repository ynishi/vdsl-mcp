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
//! | 5 | sync routes        | group (parallel 4)    | `sync_route` / `sync_route_register` |
//! | 6 | `sync.pull` wait   | group (parallel 4)    | `sync_poll`                   |
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

use crate::domain::profile::{EnvValue, Model, ProfileManifest, SyncRoute, PROFILE_SCHEMA};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use vdsl_sync::LocationId;

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

    Ok(manifest)
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

    // Auto-inject B2 creds when any model uses b2://. Profiles must NOT
    // declare these in `env` (that would leak creds into every phase's
    // shell). Instead we pull them from the MCP process env on demand
    // and scope them per-model-step via placeholder env (see
    // `build_model_step`). Keeps the pod dumb: creds only exist during
    // the rclone call, not in ~/.bashrc or any persistent location.
    if manifest
        .models
        .iter()
        .any(|m| m.src.starts_with("b2://"))
    {
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

/// Expand a manifest into a concrete [`BatchPlan`] targeting `pod_id`.
///
/// `valid_edges` is the current SDK topology — each
/// `sync.pull` / `sync.push` route is checked against it. Unknown
/// edge -> [`ProfileError::InvalidManifest`] (design §2 PrimaryRoute
/// ONLY).
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
    valid_edges: &[(LocationId, LocationId)],
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
    let comfy_port = manifest.comfyui.port.unwrap_or(8188);

    // ---- Phase 1: system.apt ----
    if let Some(system) = &manifest.system {
        if !system.apt.is_empty() {
            for pkg in &system.apt {
                assert_shell_safe(pkg, "system.apt")?;
            }
            let pkgs = system.apt.join(" ");
            steps.push(StepEntry::Leaf(exec_step(
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

    // ---- Phase 2: ComfyUI install ----
    assert_shell_safe(&manifest.comfyui.ref_, "comfyui.ref")?;
    let comfy_repo = manifest
        .comfyui
        .repo
        .as_deref()
        .unwrap_or("comfyanonymous/ComfyUI");
    assert_shell_safe(comfy_repo, "comfyui.repo")?;
    steps.push(StepEntry::Leaf(exec_step(
        "2_comfyui_install",
        &comfyui_install_script(comfy_repo, &manifest.comfyui.ref_),
        pod_id,
        &env_json,
    )));

    // ---- Phase 3: python.deps ----
    if let Some(python) = &manifest.python {
        if !python.deps.is_empty() {
            for dep in &python.deps {
                assert_shell_safe(dep, "python.deps")?;
            }
            let deps = python.deps.join(" ");
            steps.push(StepEntry::Leaf(exec_step(
                "3_python_deps",
                &format!("cd /workspace/ComfyUI && .venv/bin/pip install {deps}"),
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
            let checkout = match &node.ref_ {
                Some(r) => format!(" && cd {name} && git checkout {r}", name = node.name),
                None => String::new(),
            };
            let clone_url = expand_repo_url(&node.repo);
            // pip=true → install requirements.txt into ComfyUI's .venv,
            // but filter out torch-family lines. Custom-node reqs
            // routinely pin a torch version that upgrades the wheel
            // past the pod's CUDA driver (seen with Impact Pack pulling
            // torch 2.11.0 onto a driver that only supports <=2.5).
            // The driver-matched torch was installed by Phase 2 via
            // --system-site-packages, so we honor that pin here.
            let pip_install = if node.pip.unwrap_or(false) {
                format!(
                    " && if [ -f /workspace/ComfyUI/custom_nodes/{name}/requirements.txt ]; then \
                       grep -viE '^[[:space:]]*(torch|torchvision|torchaudio|xformers|bitsandbytes|triton)([[:space:]=<>~!;]|$)' \
                         /workspace/ComfyUI/custom_nodes/{name}/requirements.txt \
                         | /workspace/ComfyUI/.venv/bin/pip install -r /dev/stdin; \
                     fi",
                    name = node.name,
                )
            } else {
                String::new()
            };
            let script = format!(
                "cd /workspace/ComfyUI/custom_nodes && \
                 (test -d {name} || git clone {url} {name}){checkout}{pip_install}",
                name = node.name,
                url = clone_url,
            );
            node_steps.push(exec_step(
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

    // ---- Phase 5: sync routes (parallel group) ----
    // Pull -> sync_route (spawns a transfer task); push -> sync_route_register (verify only).
    // Each (src, dest) pair must appear in `valid_edges` (design §2).
    let mut route_steps: Vec<BatchStep> = Vec::new();
    if let Some(sync) = &manifest.sync {
        for (idx, route) in sync.pull.iter().enumerate() {
            validate_route(route, valid_edges)?;
            route_steps.push(BatchStep {
                id: format!("5_sync_pull_{idx}"),
                tool: "sync_route".to_string(),
                args: serde_json::json!({
                    "src": route.src,
                    "dest": route.dst,
                }),
                depends_on: Vec::new(),
                validate: None,
            });
        }
        for (idx, route) in sync.push.iter().enumerate() {
            validate_route(route, valid_edges)?;
            route_steps.push(BatchStep {
                id: format!("5_sync_push_{idx}"),
                tool: "sync_route_register".to_string(),
                args: serde_json::json!({
                    "src": route.src,
                    "dest": route.dst,
                }),
                depends_on: Vec::new(),
                validate: None,
            });
        }
    }
    if !route_steps.is_empty() {
        steps.push(StepEntry::Group(GroupBlock {
            id: Some("5_sync_routes".to_string()),
            parallel: 4,
            steps: route_steps,
        }));
    }

    // ---- Phase 6: sync.pull wait (parallel group) ----
    // Each pull route spawned in phase 5 exposes a task_id — resolved
    // via placeholder `__result:5_sync_pull_{idx}.task_id`.
    if let Some(sync) = &manifest.sync {
        if !sync.pull.is_empty() {
            let mut poll_steps: Vec<BatchStep> = Vec::with_capacity(sync.pull.len());
            for (idx, _route) in sync.pull.iter().enumerate() {
                poll_steps.push(BatchStep {
                    id: format!("6_sync_poll_{idx}"),
                    tool: "sync_poll".to_string(),
                    args: serde_json::json!({
                        "task_id": format!("__result:5_sync_pull_{idx}.task_id"),
                    }),
                    depends_on: vec![format!("5_sync_pull_{idx}")],
                    validate: None,
                });
            }
            steps.push(StepEntry::Group(GroupBlock {
                id: Some("6_sync_pull_wait".to_string()),
                parallel: 4,
                steps: poll_steps,
            }));
        }
    }

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

    // ---- Phase 8: hooks.post_install ----
    if let Some(hooks) = &manifest.hooks {
        if let Some(script) = &hooks.post_install {
            if !script.trim().is_empty() {
                steps.push(StepEntry::Leaf(exec_step(
                    "8_post_install",
                    script,
                    pod_id,
                    &env_json,
                )));
            }
        }
    }

    // ---- Phase 9: ComfyUI restart ----
    // Single blocking `exec` step. The restart script internally bounds
    // itself at ~210s (30s port-free wait + 180s curl-ready wait), and
    // launches the server via `nohup ... &` so the python process survives
    // the SSH session exit. Using `exec` (not `task_run` + `task_status`)
    // means Phase 10's health check fires only after the server actually
    // answers HTTP — no race, no polling primitive needed.
    //
    // DSL emits args as an array of tokens; validate each token
    // individually (no spaces) then shell-join with a single space for
    // the launch command.
    let args_vec = manifest
        .comfyui
        .args
        .as_deref()
        .unwrap_or(&[] as &[String]);
    for tok in args_vec {
        assert_shell_safe(tok, "comfyui.args[]")?;
    }
    let extra_args = args_vec.join(" ");
    let restart_script = comfyui_restart_script(comfy_port, &extra_args);
    steps.push(StepEntry::Leaf(exec_step(
        "9_comfyui_restart",
        &restart_script,
        pod_id,
        &env_json,
    )));

    // ---- Phase 10: health check ----
    // Pass `pod_id` explicitly so comfy_api auto-constructs the RunPod
    // proxy URL (`https://<pod_id>-8188.proxy.runpod.net`). Without it
    // the tool falls through to the default "connect first" guard and
    // fails with -32602.
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

/// Like [`is_shell_safe`] but additionally allows single spaces.
/// Intended for `comfyui.args` where multiple flags are space-separated
/// (e.g. `--lowvram --preview-method auto`). Double spaces are rejected.
#[cfg(test)]
fn is_shell_safe_with_spaces(s: &str) -> bool {
    !s.is_empty()
        && s.chars()
            .all(|c| c.is_ascii_alphanumeric() || "-._/@:+=~ ".contains(c))
        && !s.contains("  ")
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

/// ComfyUI checkout + venv + requirements install script (phase 2).
///
/// `repo` accepts either a `owner/name` slug or a full `https://` URL.
/// A slug is expanded to `https://github.com/owner/name.git`.
fn comfyui_install_script(repo: &str, git_ref: &str) -> String {
    let url = expand_repo_url(repo);
    // --system-site-packages: inherit the base image's pre-compiled
    // torch (driver-matched on RunPod pytorch images). Without this,
    // `pip install torch` pulls whatever latest wheel exists and
    // usually requires a newer CUDA driver than the pod provides, so
    // `torch.cuda.is_available()` silently returns False.
    format!(
        "set -e\n\
         cd /workspace\n\
         test -d ComfyUI || git clone {url}\n\
         cd ComfyUI\n\
         git fetch --all --tags && git checkout {git_ref}\n\
         test -d .venv || python3 -m venv --system-site-packages .venv\n\
         .venv/bin/pip install --upgrade pip\n\
         .venv/bin/pip install -r requirements.txt\n"
    )
}

/// ComfyUI restart script (design §4.5). Runs inside a `task_run`
/// wrapper so the launching process can exit once HTTP answers.
///
/// Both wait loops are bounded: port release caps at 30s (old process
/// hung on close), HTTP readiness caps at 180s (cold-start + model
/// discovery). Exceeding either limit exits non-zero so Phase 9 fails
/// loudly instead of hanging forever.
fn comfyui_restart_script(port: u16, extra_args: &str) -> String {
    // DSL-provided args are authoritative. Only inject default
    // `--listen 0.0.0.0 --port $PORT` when the manifest gave nothing.
    let launch_args = if extra_args.trim().is_empty() {
        "--listen 0.0.0.0 --port $PORT".to_string()
    } else {
        extra_args.to_string()
    };
    // pgrep pattern matches the restart script's own spawned process:
    //   `.venv/bin/python main.py --listen ... --port ...`
    // argv (cwd-relative). The bracket trick `[.]venv` matches the literal
    // `.venv` path segment while NOT matching this script's own argv
    // (which contains `[.]venv/bin/python` unescaped in the pattern text).
    format!(
        "set -e\n\
         PORT={port}\n\
         pgrep -f '[.]venv/bin/python main\\.py' | xargs -r kill || true\n\
         i=0; while lsof -i:$PORT >/dev/null 2>&1; do\n\
           i=$((i+1)); [ $i -ge 30 ] && echo 'port $PORT still held after 30s' >&2 && exit 1; sleep 1;\n\
         done\n\
         mkdir -p /workspace/.vdsl\n\
         cd /workspace/ComfyUI\n\
         nohup .venv/bin/python main.py {launch_args} \\\n\
           > /workspace/.vdsl/comfyui.log 2>&1 &\n\
         i=0; until curl -sf http://localhost:$PORT/ >/dev/null; do\n\
           i=$((i+1)); [ $i -ge 180 ] && echo 'comfyui not ready after 180s' >&2 && exit 1; sleep 1;\n\
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
             mkdir -p /workspace/ComfyUI/models/{subdir}\n\
             export RCLONE_CONFIG_B2_TYPE=b2\n\
             export RCLONE_CONFIG_B2_ACCOUNT=\"$VDSL_B2_KEY_ID\"\n\
             export RCLONE_CONFIG_B2_KEY=\"$VDSL_B2_KEY\"\n\
             rclone copyto --progress b2:{bucket}/{object} {dst}\n",
            subdir = model.subdir,
            bucket = bucket,
            object = object,
            dst = dst_path,
        );
        let step_env = merge_b2_env(env);
        Ok(exec_step(
            &format!("7_model_{idx}"),
            &script,
            pod_id,
            &step_env,
        ))
    } else if let Some(rest) = model.src.strip_prefix("file://") {
        assert_shell_safe(rest, "models[].src (file path)")?;
        let script = format!(
            "mkdir -p /workspace/ComfyUI/models/{subdir} && cp {src} {dst}",
            subdir = model.subdir,
            src = rest,
            dst = dst_path,
        );
        Ok(exec_step(&format!("7_model_{idx}"), &script, pod_id, env))
    } else {
        tracing::warn!(src = %model.src, "unsupported model src scheme");
        Err(ProfileError::InvalidManifest(format!(
            "unsupported model src scheme: '{}' (expected 'b2://' or 'file://')",
            model.src
        )))
    }
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

/// Validate a single sync route against the SDK's topology.
fn validate_route(
    route: &SyncRoute,
    valid_edges: &[(LocationId, LocationId)],
) -> Result<(), ProfileError> {
    let src = LocationId::new(route.src.clone()).map_err(|e| {
        tracing::warn!(src = %route.src, error = %e, "sync route src invalid");
        ProfileError::InvalidManifest(format!("invalid route src '{}': {e}", route.src))
    })?;
    let dest = LocationId::new(route.dst.clone()).map_err(|e| {
        tracing::warn!(dest = %route.dst, error = %e, "sync route dst invalid");
        ProfileError::InvalidManifest(format!("invalid route dst '{}': {e}", route.dst))
    })?;

    if valid_edges.iter().any(|(s, d)| *s == src && *d == dest) {
        Ok(())
    } else {
        let available = valid_edges
            .iter()
            .map(|(s, d)| format!("{s}->{d}"))
            .collect::<Vec<_>>()
            .join(", ");
        tracing::warn!(
            src = %src,
            dest = %dest,
            available = %available,
            "sync route not in SDK topology"
        );
        Err(ProfileError::InvalidManifest(format!(
            "unknown route {src}->{dest}; valid edges: [{available}]"
        )))
    }
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
#[path = "profile_service_tests.rs"]
mod tests;
