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
//! | 7 | models             | group (parallel 4)    | `sync_route` / `exec` (cp)    |
//! | 8 | `hooks.post_install` | leaf                | `exec`                        |
//! | 9 | ComfyUI restart    | leaf pair             | `task_run` + `task_status`    |
//! |10 | health check       | leaf                  | `comfy_api`                   |
//!
//! # Placeholder syntax (consumed by batch_service)
//!
//! Some steps reference values produced by earlier steps at runtime.
//! Rather than pre-resolving, the plan embeds a placeholder:
//!
//! ```text
//! "__result:{step_id}.{field}"
//! ```
//!
//! batch_service pattern-matches these at dispatch time. Subtask 1
//! produces them; Subtask 2 resolves them.
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

    if !missing.is_empty() {
        missing.sort();
        missing.dedup();
        tracing::warn!(missing = ?missing, "profile manifest references unset secret(s)");
        return Err(ProfileError::MissingSecrets(missing));
    }

    Ok(resolved)
}

/// Expand a manifest into a concrete [`BatchPlan`] targeting `pod_id`.
///
/// `secrets` should be the output of [`resolve_secrets`]. `valid_edges`
/// is the current SDK topology — each `sync.pull` / `sync.push` route
/// is checked against it. Unknown edge ->
/// [`ProfileError::InvalidManifest`] (design §2 PrimaryRoute ONLY).
///
/// `dry_run` is forwarded onto the resulting [`BatchPlan`] unchanged;
/// secret redaction happens in batch_service at dispatch time.
pub fn expand_phases(
    manifest: &ProfileManifest,
    pod_id: &str,
    secrets: &HashMap<String, String>,
    valid_edges: &[(LocationId, LocationId)],
    dry_run: bool,
) -> Result<BatchPlan, ProfileError> {
    let mut steps: Vec<StepEntry> = Vec::new();

    // Shared env object injected into every exec step.
    let env_json = env_value(secrets);
    let comfy_port = manifest.comfyui.port.unwrap_or(8188);

    // ---- Phase 1: system.apt ----
    if let Some(system) = &manifest.system {
        if !system.apt.is_empty() {
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
    steps.push(StepEntry::Leaf(exec_step(
        "2_comfyui_install",
        &comfyui_install_script(&manifest.comfyui.ref_),
        pod_id,
        &env_json,
    )));

    // ---- Phase 3: python.deps ----
    if let Some(python) = &manifest.python {
        if !python.deps.is_empty() {
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
            let checkout = match &node.ref_ {
                Some(r) => format!(" && cd {name} && git checkout {r}", name = node.name),
                None => String::new(),
            };
            let script = format!(
                "cd /workspace/ComfyUI/custom_nodes && \
                 (test -d {name} || git clone {repo} {name}){checkout}",
                name = node.name,
                repo = node.repo,
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
                    "dest": route.dest,
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
                    "dest": route.dest,
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

    // ---- Phase 7: models (parallel group) ----
    if !manifest.models.is_empty() {
        let mut model_steps: Vec<BatchStep> = Vec::with_capacity(manifest.models.len());
        for (idx, model) in manifest.models.iter().enumerate() {
            model_steps.push(build_model_step(idx, model, pod_id, &env_json)?);
        }
        steps.push(StepEntry::Group(GroupBlock {
            id: Some("7_models".to_string()),
            parallel: 4,
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

    // ---- Phase 9: ComfyUI restart (task_run + task_status pair) ----
    let restart_script =
        comfyui_restart_script(comfy_port, manifest.comfyui.args.as_deref().unwrap_or(""));
    steps.push(StepEntry::Leaf(BatchStep {
        id: "9a_task_run".to_string(),
        tool: "task_run".to_string(),
        args: serde_json::json!({
            "script": restart_script,
            "pod_id": pod_id,
            "env": env_json,
        }),
        depends_on: Vec::new(),
        validate: None,
    }));
    steps.push(StepEntry::Leaf(BatchStep {
        id: "9b_task_poll".to_string(),
        tool: "task_status".to_string(),
        args: serde_json::json!({
            "job_id": "__result:9a_task_run.job_id",
        }),
        depends_on: vec!["9a_task_run".to_string()],
        validate: None,
    }));

    // ---- Phase 10: health check ----
    steps.push(StepEntry::Leaf(BatchStep {
        id: "10_health".to_string(),
        tool: "comfy_api".to_string(),
        args: serde_json::json!({
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

/// SHA-256 hex digest of the canonical manifest JSON.
///
/// Callers pass the bytes they parsed. This matches what the vdsl
/// side reports via `profile:hash_source()` as long as both sides
/// hash the same canonical representation.
pub fn compute_profile_hash(manifest_json: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(manifest_json.as_bytes());
    format!("{:x}", hasher.finalize())
}

// =============================================================================
// Private helpers
// =============================================================================

/// Build a serde_json Value from the resolved secrets map.
fn env_value(secrets: &HashMap<String, String>) -> serde_json::Value {
    match serde_json::to_value(secrets) {
        Ok(v) => v,
        Err(e) => {
            tracing::warn!(error = %e, "env_value serialization failed, using empty object");
            serde_json::Value::Object(Default::default())
        }
    }
}

/// Build a standard `exec` step shape.
fn exec_step(id: &str, script: &str, pod_id: &str, env: &serde_json::Value) -> BatchStep {
    BatchStep {
        id: id.to_string(),
        tool: "exec".to_string(),
        args: serde_json::json!({
            "pod_id": pod_id,
            "script": script,
            "env": env,
        }),
        depends_on: Vec::new(),
        validate: None,
    }
}

/// ComfyUI checkout + venv + requirements install script (phase 2).
fn comfyui_install_script(git_ref: &str) -> String {
    format!(
        "set -e\n\
         cd /workspace\n\
         test -d ComfyUI || git clone https://github.com/comfyanonymous/ComfyUI.git\n\
         cd ComfyUI\n\
         git fetch --all --tags && git checkout {git_ref}\n\
         test -d .venv || python3 -m venv .venv\n\
         .venv/bin/pip install --upgrade pip\n\
         .venv/bin/pip install -r requirements.txt\n"
    )
}

/// ComfyUI restart script (design §4.5). Runs inside a `task_run`
/// wrapper so the launching process can exit once HTTP answers.
fn comfyui_restart_script(port: u16, extra_args: &str) -> String {
    format!(
        "set -e\n\
         PORT={port}\n\
         pkill -f ComfyUI/main.py || true\n\
         until ! lsof -i:$PORT >/dev/null 2>&1; do sleep 1; done\n\
         cd /workspace/ComfyUI\n\
         nohup .venv/bin/python main.py --listen 0.0.0.0 --port $PORT {extra_args} \\\n\
           > /workspace/.vdsl/comfyui.log 2>&1 &\n\
         until curl -sf http://localhost:$PORT/ >/dev/null; do sleep 1; done\n"
    )
}

/// Build a phase-7 model step.
///
/// - `b2://...` — staged via `sync_route` (same edge vocabulary as
///   phase 5, but registered separately per model).
/// - `file://...` — copied on the pod via `exec cp`.
fn build_model_step(
    idx: usize,
    model: &Model,
    pod_id: &str,
    env: &serde_json::Value,
) -> Result<BatchStep, ProfileError> {
    let dst_path = format!("/workspace/ComfyUI/models/{}/{}", model.subdir, model.dst);

    if let Some(rest) = model.src.strip_prefix("b2://") {
        Ok(BatchStep {
            id: format!("7_model_{idx}"),
            tool: "sync_route".to_string(),
            args: serde_json::json!({
                "src": "cloud",
                "dest": format!("pod-{pod_id}"),
                "src_path": rest,
                "dest_path": dst_path,
            }),
            depends_on: Vec::new(),
            validate: None,
        })
    } else if let Some(rest) = model.src.strip_prefix("file://") {
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

/// Validate a single sync route against the SDK's topology.
fn validate_route(
    route: &SyncRoute,
    valid_edges: &[(LocationId, LocationId)],
) -> Result<(), ProfileError> {
    let src = LocationId::new(route.src.clone()).map_err(|e| {
        tracing::warn!(src = %route.src, error = %e, "sync route src invalid");
        ProfileError::InvalidManifest(format!("invalid route src '{}': {e}", route.src))
    })?;
    let dest = LocationId::new(route.dest.clone()).map_err(|e| {
        tracing::warn!(dest = %route.dest, error = %e, "sync route dest invalid");
        ProfileError::InvalidManifest(format!("invalid route dest '{}': {e}", route.dest))
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
mod tests {
    use super::*;
    use crate::domain::profile::{ComfyUiConfig, Hooks, PythonConfig, SecretRef, SystemConfig};

    fn minimal_manifest_json() -> String {
        serde_json::json!({
            "schema": "vdsl.profile/1",
            "name": "minimal",
            "comfyui": { "ref": "v0.3.10" }
        })
        .to_string()
    }

    fn full_manifest() -> ProfileManifest {
        ProfileManifest {
            schema: PROFILE_SCHEMA.to_string(),
            name: "full".to_string(),
            comfyui: ComfyUiConfig {
                ref_: "master".to_string(),
                args: Some("--lowvram".to_string()),
                port: Some(8188),
            },
            system: Some(SystemConfig {
                apt: vec!["git".to_string(), "curl".to_string()],
            }),
            python: Some(PythonConfig {
                deps: vec!["numpy".to_string()],
            }),
            custom_nodes: vec![crate::domain::profile::CustomNode {
                name: "ComfyUI-Manager".to_string(),
                repo: "https://github.com/ltdrdata/ComfyUI-Manager".to_string(),
                ref_: None,
            }],
            sync: Some(crate::domain::profile::SyncConfig {
                pull: vec![SyncRoute {
                    src: "cloud".to_string(),
                    dest: "pod-abc".to_string(),
                }],
                push: vec![SyncRoute {
                    src: "pod-abc".to_string(),
                    dest: "cloud".to_string(),
                }],
            }),
            models: vec![
                Model {
                    src: "b2://bucket/sdxl.safetensors".to_string(),
                    dst: "sdxl.safetensors".to_string(),
                    kind: "checkpoint".to_string(),
                    subdir: "checkpoints".to_string(),
                },
                Model {
                    src: "file:///mnt/models/lora.safetensors".to_string(),
                    dst: "lora.safetensors".to_string(),
                    kind: "lora".to_string(),
                    subdir: "loras".to_string(),
                },
            ],
            env: HashMap::new(),
            hooks: Some(Hooks {
                post_install: Some("echo done".to_string()),
            }),
        }
    }

    fn edges_for_pod(pod: &str) -> Vec<(LocationId, LocationId)> {
        let local = LocationId::local();
        let cloud = LocationId::new("cloud").unwrap();
        let pod = LocationId::new(format!("pod-{pod}")).unwrap();
        vec![
            (local.clone(), cloud.clone()),
            (cloud.clone(), local.clone()),
            (cloud.clone(), pod.clone()),
            (pod.clone(), cloud.clone()),
            (local, pod),
        ]
    }

    // ----- parse_manifest -----

    #[test]
    fn parse_manifest_accepts_valid_json() {
        let m = parse_manifest(&minimal_manifest_json()).expect("parse ok");
        assert_eq!(m.schema, PROFILE_SCHEMA);
        assert_eq!(m.name, "minimal");
        assert_eq!(m.comfyui.ref_, "v0.3.10");
    }

    #[test]
    fn parse_manifest_rejects_wrong_schema() {
        let json = serde_json::json!({
            "schema": "vdsl.profile/999",
            "name": "bad",
            "comfyui": { "ref": "x" }
        })
        .to_string();
        let err = parse_manifest(&json).unwrap_err();
        match err {
            ProfileError::UnsupportedSchema { got, .. } => {
                assert_eq!(got, "vdsl.profile/999");
            }
            other => panic!("expected UnsupportedSchema, got {other:?}"),
        }
    }

    #[test]
    fn parse_manifest_rejects_missing_required_fields() {
        let json = r#"{ "schema": "vdsl.profile/1" }"#;
        let err = parse_manifest(json).unwrap_err();
        assert!(matches!(err, ProfileError::InvalidManifest(_)));
    }

    #[test]
    fn parse_manifest_deserializes_env_value_variants() {
        let json = serde_json::json!({
            "schema": "vdsl.profile/1",
            "name": "env-test",
            "comfyui": { "ref": "x" },
            "env": {
                "PLAIN_KEY": "plain_value",
                "SECRET_KEY": { "__secret": "MY_SECRET" }
            }
        })
        .to_string();
        let m = parse_manifest(&json).expect("parse ok");
        match m.env.get("PLAIN_KEY") {
            Some(EnvValue::Plain(s)) => assert_eq!(s, "plain_value"),
            other => panic!("expected Plain, got {other:?}"),
        }
        match m.env.get("SECRET_KEY") {
            Some(EnvValue::Secret(SecretRef { name })) => assert_eq!(name, "MY_SECRET"),
            other => panic!("expected Secret, got {other:?}"),
        }
    }

    // ----- resolve_secrets -----

    #[test]
    fn resolve_secrets_reads_env_var() {
        // Unique var name to avoid flakiness / cross-test interference.
        let var = "VDSL_MCP_TEST_SECRET_OK_X";
        // SAFETY: test-only; single-threaded within this #[test] fn.
        unsafe {
            std::env::set_var(var, "s3cret");
        }

        let mut manifest = ProfileManifest {
            schema: PROFILE_SCHEMA.to_string(),
            name: "s".to_string(),
            comfyui: ComfyUiConfig {
                ref_: "x".to_string(),
                args: None,
                port: None,
            },
            system: None,
            python: None,
            custom_nodes: vec![],
            sync: None,
            models: vec![],
            env: HashMap::new(),
            hooks: None,
        };
        manifest.env.insert(
            "FOO".to_string(),
            EnvValue::Secret(SecretRef {
                name: var.to_string(),
            }),
        );
        manifest
            .env
            .insert("BAR".to_string(), EnvValue::Plain("bar_value".to_string()));

        let resolved = resolve_secrets(&manifest).expect("resolve ok");
        assert_eq!(resolved.get("FOO").map(|s| s.as_str()), Some("s3cret"));
        assert_eq!(resolved.get("BAR").map(|s| s.as_str()), Some("bar_value"));

        unsafe {
            std::env::remove_var(var);
        }
    }

    #[test]
    fn resolve_secrets_collects_all_missing() {
        let mut manifest = ProfileManifest {
            schema: PROFILE_SCHEMA.to_string(),
            name: "s".to_string(),
            comfyui: ComfyUiConfig {
                ref_: "x".to_string(),
                args: None,
                port: None,
            },
            system: None,
            python: None,
            custom_nodes: vec![],
            sync: None,
            models: vec![],
            env: HashMap::new(),
            hooks: None,
        };
        manifest.env.insert(
            "A".to_string(),
            EnvValue::Secret(SecretRef {
                name: "VDSL_MCP_TEST_MISSING_A_ZZ".to_string(),
            }),
        );
        manifest.env.insert(
            "B".to_string(),
            EnvValue::Secret(SecretRef {
                name: "VDSL_MCP_TEST_MISSING_B_ZZ".to_string(),
            }),
        );

        let err = resolve_secrets(&manifest).unwrap_err();
        match err {
            ProfileError::MissingSecrets(mut names) => {
                names.sort();
                assert_eq!(
                    names,
                    vec![
                        "VDSL_MCP_TEST_MISSING_A_ZZ".to_string(),
                        "VDSL_MCP_TEST_MISSING_B_ZZ".to_string(),
                    ]
                );
            }
            other => panic!("expected MissingSecrets, got {other:?}"),
        }
    }

    // ----- expand_phases -----

    fn leaf_ids(plan: &BatchPlan) -> Vec<String> {
        let mut ids = Vec::new();
        for entry in &plan.steps {
            match entry {
                StepEntry::Leaf(s) => ids.push(s.id.clone()),
                StepEntry::Group(g) => {
                    if let Some(id) = &g.id {
                        ids.push(id.clone());
                    }
                    for s in &g.steps {
                        ids.push(s.id.clone());
                    }
                }
            }
        }
        ids
    }

    #[test]
    fn expand_phases_minimal_manifest_emits_2_9_10() {
        let m = parse_manifest(&minimal_manifest_json()).expect("parse ok");
        let plan =
            expand_phases(&m, "abc", &HashMap::new(), &edges_for_pod("abc"), false).expect("ok");
        let ids = leaf_ids(&plan);

        assert!(ids.iter().any(|i| i == "2_comfyui_install"));
        assert!(ids.iter().any(|i| i == "9a_task_run"));
        assert!(ids.iter().any(|i| i == "9b_task_poll"));
        assert!(ids.iter().any(|i| i == "10_health"));

        // Phases that should NOT be present with an empty manifest:
        assert!(!ids.iter().any(|i| i == "1_system_apt"));
        assert!(!ids.iter().any(|i| i == "3_python_deps"));
        assert!(!ids.iter().any(|i| i.starts_with("4_custom_node_")));
        assert!(!ids.iter().any(|i| i.starts_with("5_sync_")));
        assert!(!ids.iter().any(|i| i.starts_with("6_sync_poll_")));
        assert!(!ids.iter().any(|i| i.starts_with("7_model_")));
        assert!(!ids.iter().any(|i| i == "8_post_install"));
    }

    #[test]
    fn expand_phases_full_manifest_emits_all_phases_and_correct_tools() {
        let m = full_manifest();
        let plan =
            expand_phases(&m, "abc", &HashMap::new(), &edges_for_pod("abc"), false).expect("ok");

        // Walk the plan and collect (id, tool) pairs for every leaf.
        let mut pairs: Vec<(String, String)> = Vec::new();
        for entry in &plan.steps {
            match entry {
                StepEntry::Leaf(s) => pairs.push((s.id.clone(), s.tool.clone())),
                StepEntry::Group(g) => {
                    for s in &g.steps {
                        pairs.push((s.id.clone(), s.tool.clone()));
                    }
                }
            }
        }

        let find = |id: &str| pairs.iter().find(|(i, _)| i == id).map(|(_, t)| t.clone());

        assert_eq!(find("1_system_apt"), Some("exec".to_string()));
        assert_eq!(find("2_comfyui_install"), Some("exec".to_string()));
        assert_eq!(find("3_python_deps"), Some("exec".to_string()));
        assert_eq!(find("4_custom_node_0"), Some("exec".to_string()));
        assert_eq!(find("5_sync_pull_0"), Some("sync_route".to_string()));
        assert_eq!(
            find("5_sync_push_0"),
            Some("sync_route_register".to_string())
        );
        assert_eq!(find("6_sync_poll_0"), Some("sync_poll".to_string()));
        assert_eq!(find("7_model_0"), Some("sync_route".to_string())); // b2://
        assert_eq!(find("7_model_1"), Some("exec".to_string())); // file://
        assert_eq!(find("8_post_install"), Some("exec".to_string()));
        assert_eq!(find("9a_task_run"), Some("task_run".to_string()));
        assert_eq!(find("9b_task_poll"), Some("task_status".to_string()));
        assert_eq!(find("10_health"), Some("comfy_api".to_string()));
    }

    #[test]
    fn expand_phases_rejects_unknown_edge() {
        let mut m = full_manifest();
        // Replace valid pull with one referencing a pod that is not in the topology.
        m.sync.as_mut().unwrap().pull[0].dest = "pod-other".to_string();

        let err =
            expand_phases(&m, "abc", &HashMap::new(), &edges_for_pod("abc"), false).unwrap_err();
        assert!(matches!(err, ProfileError::InvalidManifest(_)));
    }

    #[test]
    fn expand_phases_rejects_invalid_location_string() {
        let mut m = full_manifest();
        // Uppercase rejected by LocationId::new.
        m.sync.as_mut().unwrap().pull[0].src = "CLOUD".to_string();

        let err =
            expand_phases(&m, "abc", &HashMap::new(), &edges_for_pod("abc"), false).unwrap_err();
        assert!(matches!(err, ProfileError::InvalidManifest(_)));
    }

    #[test]
    fn expand_phases_rejects_unsupported_model_scheme() {
        let mut m = full_manifest();
        m.models[0].src = "http://example.com/x.safetensors".to_string();

        let err =
            expand_phases(&m, "abc", &HashMap::new(), &edges_for_pod("abc"), false).unwrap_err();
        assert!(matches!(err, ProfileError::InvalidManifest(_)));
    }

    #[test]
    fn expand_phases_forwards_dry_run_flag() {
        let m = parse_manifest(&minimal_manifest_json()).expect("parse ok");
        let plan =
            expand_phases(&m, "abc", &HashMap::new(), &edges_for_pod("abc"), true).expect("ok");
        assert!(plan.dry_run);
        assert_eq!(plan.mode, PlanMode::Seq);
    }

    // ----- compute_profile_hash -----

    #[test]
    fn compute_profile_hash_is_deterministic() {
        let a = compute_profile_hash("hello");
        let b = compute_profile_hash("hello");
        assert_eq!(a, b);
        assert_eq!(a.len(), 64); // SHA-256 hex
    }

    #[test]
    fn compute_profile_hash_differs_for_different_input() {
        let a = compute_profile_hash("hello");
        let b = compute_profile_hash("hellO");
        assert_ne!(a, b);
    }
}
