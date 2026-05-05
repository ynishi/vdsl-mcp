//! Profile manifest domain types.
//!
//! Shape mirrors the JSON emitted by the vdsl Lua Profile DSL
//! (`lua/vdsl/runtime/profile.lua`), schema tag `vdsl.profile/1`.
//!
//! These types are intentionally inert — they describe a declarative
//! ComfyUI-on-pod environment. Execution lives in
//! `application::profile_service` (phase expansion + secret resolution)
//! and `application::batch_service` (orchestration).
//!
//! # Cross-repo sync
//!
//! Any change to field names / serde renames / required-vs-optional
//! here must be mirrored on the vdsl side. See project `CLAUDE.md`
//! for the cross-repo rule and the 2026-04 PNG metadata drift fault.
//!
//! # Invariants
//!
//! - `schema` must equal `"vdsl.profile/1"`. Validated in
//!   `profile_service::parse_manifest` — NOT in serde (serde can't
//!   easily reject specific string values while still keeping the
//!   field typed).
//! - `Model::subdir` is trusted verbatim. MCP does NOT re-derive it
//!   from `kind`. The vdsl DSL owns the `kind -> subdir` mapping.
//! - `EnvValue::Secret` wraps a `{"__secret": "NAME"}` object. The
//!   secret value is resolved via `std::env::var(NAME)` during
//!   `profile_service::resolve_secrets`, not at deserialization time.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Expected schema tag. Anything else is rejected by
/// `profile_service::parse_manifest`.
pub const PROFILE_SCHEMA: &str = "vdsl.profile/1";

/// Root profile manifest. Deserialized from the JSON emitted by the
/// vdsl Lua DSL.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProfileManifest {
    /// Schema tag. Must equal [`PROFILE_SCHEMA`].
    pub schema: String,

    /// Human-readable profile name (used for logging / plan_id hints).
    pub name: String,

    /// ComfyUI installation config (ref, args, port).
    ///
    /// Optional: profiles that only use `staging.push` / `sync` /
    /// `system.apt` without needing a ComfyUI runtime omit this block.
    /// When `None`, Phase 2 (install), Phase 9 (restart), and Phase 10
    /// (health check) are all skipped by `expand_phases`.
    #[serde(default)]
    pub comfyui: Option<ComfyUiConfig>,

    /// System-level apt packages to install.
    #[serde(default)]
    pub system: Option<SystemConfig>,

    /// Python packages installed into the ComfyUI venv.
    #[serde(default)]
    pub python: Option<PythonConfig>,

    /// Custom ComfyUI nodes to clone into `custom_nodes/`.
    #[serde(default)]
    pub custom_nodes: Vec<CustomNode>,

    /// Pull / push routes for the session's sync store.
    #[serde(default)]
    pub sync: Option<SyncConfig>,

    /// One-shot eager pod → B2 uploads executed during Phase 5 of
    /// apply. Distinct from `sync.push` (which is marker-only and
    /// consumed by later generation flows). See
    /// docs/profile-and-orchestration.md §2.3 / §2.5.
    #[serde(default)]
    pub staging: Option<StagingConfig>,

    /// ComfyUI checkpoints / LoRAs / VAEs etc. to stage into
    /// `/workspace/ComfyUI/models/<subdir>/<dst>`. Reserved for ComfyUI;
    /// non-ComfyUI workloads use [`Self::llm_models`] instead.
    #[serde(default)]
    pub models: Vec<Model>,

    /// Raw LLM weight staging for non-ComfyUI workloads (vLLM / Ollama
    /// / TEI etc.). Independent of [`Self::models`] so ComfyUI subdir /
    /// kind semantics don't bleed into LLM staging.
    #[serde(default)]
    pub llm_models: Vec<LlmModel>,

    /// Adjacent AI daemon services (vLLM / Ollama / etc.). Each entry
    /// names a known platform; the launch command is generated from the
    /// platform variant — no free-form shell strings.
    #[serde(default)]
    pub services: Vec<ServiceConfig>,

    /// Environment variables. Plain strings or `{"__secret": "NAME"}`.
    #[serde(default)]
    pub env: HashMap<String, EnvValue>,

    /// Post-install hook script bodies.
    #[serde(default)]
    pub hooks: Option<Hooks>,
}

/// ComfyUI clone / checkout / launch config.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ComfyUiConfig {
    /// Git ref (branch / tag / SHA) to check out.
    #[serde(rename = "ref")]
    pub ref_: String,

    /// Git repo slug (`owner/name`) or full URL. Defaults to
    /// `comfyanonymous/ComfyUI` server-side.
    #[serde(default)]
    pub repo: Option<String>,

    /// Extra argv tacked onto `python main.py ...` at launch. DSL
    /// emits an array; we keep it as `Vec<String>` and shell-join on
    /// expansion.
    #[serde(default)]
    pub args: Option<Vec<String>>,

    /// TCP port the server binds to. Defaults to 8188 server-side.
    #[serde(default)]
    pub port: Option<u16>,
}

/// `system.apt` block.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct SystemConfig {
    #[serde(default)]
    pub apt: Vec<String>,
}

/// `python.deps` block.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct PythonConfig {
    #[serde(default)]
    pub deps: Vec<String>,
    /// Optional Python version pin (e.g. `"3.12"`). Advisory only:
    /// when set, `profile_apply` emits a Phase 3 warn step that compares
    /// the requested version to the actual `python3` on the pod.
    /// Mismatch logs to stderr but does not fail the apply.
    #[serde(default)]
    pub version: Option<String>,
    /// When true, Phase 3 pip install adds `--force-reinstall`. Required
    /// for cases where a base-image package (e.g. torch 2.4 from
    /// runpod/pytorch) must be replaced by a wheel pulled by a dep
    /// (e.g. vllm 0.18.1 needs torch 2.10).
    #[serde(default)]
    pub force_reinstall: Option<bool>,
}

/// One custom-node clone entry.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CustomNode {
    /// Directory name under `custom_nodes/`.
    pub name: String,

    /// Git URL to clone from.
    pub repo: String,

    /// Optional ref (branch / tag / SHA). `None` means default branch.
    #[serde(default, rename = "ref")]
    pub ref_: Option<String>,

    /// Run `pip install -r requirements.txt` after clone if true.
    /// Tolerated for DSL compatibility; expansion not wired yet.
    #[serde(default)]
    pub pip: Option<bool>,
}

/// `sync.pull` / `sync.push` config.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct SyncConfig {
    #[serde(default)]
    pub pull: Vec<SyncRoute>,

    #[serde(default)]
    pub push: Vec<SyncRoute>,
}

/// Staging block. Currently holds eager pod → B2 uploads (`push`).
/// Each route runs as a blocking `rclone copyto` in Phase 5, in the
/// same parallel group as `sync.pull`. Separate from `sync.push` on
/// purpose: `sync.push` is marker-only (consumed by later generation
/// flows), `staging.push` fires during apply.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct StagingConfig {
    #[serde(default)]
    pub push: Vec<SyncRoute>,
}

/// One sync route edge. Schemes depend on the direction:
///
/// - `sync.pull`: `src = "b2://<bucket>/<path>"`, `dst = "/<pod-path>"`
/// - `sync.push`: `src = "/<pod-path>"`, `dst = "b2://<bucket>/<path>"`
///
/// The `{pod_id}` placeholder is allowed inside a b2 path and is
/// substituted with the target pod id at phase expansion time (docs
/// §2.3).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SyncRoute {
    pub src: String,
    /// Destination. Field name is `dst` on the wire (DSL + docs
    /// canonical); `dest` accepted as alias for older JSON.
    #[serde(rename = "dst", alias = "dest")]
    pub dst: String,
}

/// Adjacent daemon service. Launch command is derived from `platform`
/// — there is no free-form `cmd: String` field by design. Adding a new
/// platform requires extending [`ServicePlatform`].
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServiceConfig {
    /// Identifier used for log file naming (`service_<name>.log`) and
    /// step ids. Must be `is_shell_safe`.
    pub name: String,

    /// Platform-specific launch config. The variant determines which
    /// fields are required and how the launch shell command is built.
    #[serde(flatten)]
    pub platform: ServicePlatform,

    #[serde(default)]
    pub ready_check: Option<HttpReadyCheck>,
}

/// Closed set of supported service platforms. Each variant carries its
/// own typed config; the launch command is generated, not user-supplied.
/// New platforms require a code change (intentional — keeps the surface
/// small and auditable).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "lowercase")]
pub enum ServicePlatform {
    /// vLLM OpenAI-compatible server.
    /// `vllm serve <model> --port <port> [--tensor-parallel-size N] [--dtype X] [extra_args...]`
    Vllm {
        /// HF repo id (e.g. `meta-llama/Llama-3-8B-Instruct`) or local path.
        model: String,
        port: u16,
        #[serde(default)]
        dtype: Option<String>,
        #[serde(default)]
        tensor_parallel_size: Option<u32>,
        /// Free-form extra flags (single-token each, validated with
        /// `is_shell_safe_with_spaces`).
        #[serde(default)]
        extra_args: Vec<String>,
    },
    /// Ollama daemon. `ollama serve` listens on `port`; `models` are
    /// pre-pulled with `ollama pull <name>` after the daemon is up.
    Ollama {
        port: u16,
        #[serde(default)]
        models: Vec<String>,
    },
}

/// HTTP readiness probe. `curl -sf <http>` is polled every second up
/// to `timeout_sec` (default 300).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HttpReadyCheck {
    /// Full URL (operator-authored; substituted verbatim into the poll
    /// shell command — same trust model as the rest of the manifest).
    pub http: String,
    #[serde(default)]
    pub timeout_sec: Option<u32>,
}

/// ComfyUI model staging entry.
///
/// Reserved for ComfyUI's `models/<subdir>/<dst>` layout. `subdir` and
/// `kind` are ComfyUI-specific terms (`checkpoints`, `loras`, `vae`,
/// ...). For raw LLM weight staging targeting non-ComfyUI workloads
/// (vLLM / Ollama / TEI), use [`LlmModel`] instead.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Model {
    /// Source URI (`b2://...` or `file://...`).
    pub src: String,

    /// Destination filename (relative to `subdir`).
    pub dst: String,

    /// Model kind (`checkpoint`, `lora`, ...). Trusted verbatim from
    /// the DSL; MCP does not re-derive `subdir` from it.
    pub kind: String,

    /// Sub-directory under `/workspace/ComfyUI/models/`.
    pub subdir: String,
}

/// Raw LLM weight staging entry for non-ComfyUI workloads.
///
/// Unlike [`Model`] which is constrained to ComfyUI's layout, `LlmModel`
/// targets an arbitrary directory and pulls a HuggingFace repo in full.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LlmModel {
    /// Source URI. Currently only `hf://<org>/<repo>` is supported.
    pub src: String,

    /// Absolute directory the repo is materialized into (passed to
    /// `huggingface-cli download --local-dir`).
    pub dst_dir: String,

    /// Optional git revision / branch / tag passed to `--revision`.
    #[serde(default)]
    pub revision: Option<String>,
}

/// Environment value: plain string or a `__secret` reference.
///
/// The `#[serde(untagged)]` attribute lets serde pick the matching
/// shape without a discriminant tag:
///
/// ```json
/// { "FOO": "plain_value",  "BAR": { "__secret": "BAR_NAME" } }
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum EnvValue {
    Plain(String),
    Secret(SecretRef),
}

/// `{"__secret": "NAME"}` wrapper.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SecretRef {
    #[serde(rename = "__secret")]
    pub name: String,
}

/// `hooks.*` block. Only `post_install` is used in v1; other keys
/// (`pre_start`, `post_start`) are reserved — see design §8.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct Hooks {
    #[serde(default)]
    pub post_install: Option<String>,
}

// =============================================================================
// Profile scaffold
// =============================================================================

use std::path::{Path, PathBuf};

/// Errors that can occur when scaffolding a new profile file.
#[derive(Debug, thiserror::Error)]
pub enum ProfileScaffoldError {
    /// The profile name is invalid (empty or contains characters outside `[a-zA-Z0-9_-]`).
    #[error("invalid profile name: {0}")]
    InvalidName(String),

    /// A profile file already exists at the target path and `overwrite` was not requested.
    #[error("profile '{0}' already exists (pass overwrite=true to force)")]
    AlreadyExists(String),

    /// An I/O error occurred while creating the directory or writing the file.
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    /// The home directory could not be resolved (needed for the default root fallback).
    #[error("home directory not found")]
    NoHomeDir,
}

/// Result returned by a successful [`scaffold_profile`] call.
#[derive(Debug, serde::Serialize)]
pub struct ProfileScaffoldResult {
    /// Absolute path of the created (or overwritten) profile file.
    pub profile_path: PathBuf,
    /// `true` if the file was newly created; `false` if it was overwritten
    /// (only possible when `overwrite=true`).
    pub file_created: bool,
}

/// Lua DSL profile template. Placeholders `${NAME}` and `${OUT}` are substituted
/// at scaffold time. Content is a verbatim port of the heredoc in
/// `vdsl/scripts/new_profile.sh:36-121`.
const PROFILE_TEMPLATE: &str = r#"--- ${OUT}
-- <one-line purpose of this profile>
--
-- Standing prohibitions (identical to every profile in this dir):
--   - SECRETS: never declare. MCP auto-injects at apply time.
--   - NO DSL-BYPASS: never hand-roll `mv`/`cp`/`rclone`/`wget`/`curl`
--     via `vdsl_exec` / `vdsl_task_run` to paper over DSL gaps.
--     Extend `lua/vdsl/runtime/profile.lua` + `vdsl-mcp
--     profile_service.rs` instead. See docs/profile-and-orchestration.md
--     §2.4 (secrets) and §2.5 (bypass). Run
--     `scripts/check_profile_ops.sh` before committing pod-op changes.
--
-- Target image:
--   runpod/pytorch:2.4.0-py3.11-cuda12.4.1-devel-ubuntu22.04
--
-- Staging:
--   List required B2 objects here. User profiles reference
--   `b2://<bucket>/...` sources only — stage upstream assets into B2
--   first (never hand-roll wget/curl inside this profile; see §2.5).
--
-- Apply:
--   vdsl_profile_apply(
--     manifest = "${OUT}",
--     pod_id   = "<ephemeral pod id>",
--   )
--
-- Compile check:
--   lua -e "package.path='lua/?.lua;lua/?/init.lua;'..package.path" \
--       ${OUT}

local vdsl = require("vdsl")

local B2_ROOT = "b2://run-pod-ZQyB"

local profile = vdsl.profile {
  name = "${NAME}",

  comfyui = {
    repo = "comfyanonymous/ComfyUI",
    ref  = "master",
    args = {},
  },

  python = {
    version = "3.12",
    deps    = {},
  },

  system = {
    apt = { "git-lfs" },
  },

  custom_nodes = {
    { repo = "ltdrdata/ComfyUI-Manager" },
    -- add more custom nodes here
  },

  models = {
    -- { kind = "checkpoint",
    --   dst  = "<name>.safetensors",
    --   src  = B2_ROOT .. "/models/checkpoints/<name>.safetensors" },
  },

  -- No `env` block: user profiles never carry credentials. Add only
  -- non-secret runtime config (e.g. DEBUG = "1"). Keys matching
  -- KEY/SECRET/TOKEN/PASSWORD/PWD/AUTH/CRED/APIKEY are rejected at
  -- normalize time.

  sync = {
    push = {
      -- "/workspace/ComfyUI/output/ → b2://run-pod-ZQyB/output/{pod_id}/",
    },
  },

  hooks = {
    post_install = [[
python -c "import torch; print('cuda=' + str(torch.cuda.is_available()))"
]],
  },
}

print(profile:manifest_json(true))

return profile
"#;

/// Validate a profile name.
///
/// # Arguments
///
/// * `name` — candidate profile name string
///
/// # Returns
///
/// `Ok(())` if the name is valid; `Err(ProfileScaffoldError::InvalidName)` otherwise.
///
/// # Errors
///
/// Returns [`ProfileScaffoldError::InvalidName`] if the name is empty or contains
/// characters outside `[a-zA-Z0-9_-]`.
fn validate_profile_name(name: &str) -> Result<(), ProfileScaffoldError> {
    if name.is_empty() {
        return Err(ProfileScaffoldError::InvalidName(
            "name must not be empty".to_string(),
        ));
    }
    if !name
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-')
    {
        return Err(ProfileScaffoldError::InvalidName(format!(
            "'{name}' contains invalid characters (only [a-zA-Z0-9_-] allowed)"
        )));
    }
    Ok(())
}

/// Scaffold a new Profile Lua DSL file at `<root>/profiles/<name>.lua`.
///
/// The generated file content is a verbatim port of the heredoc template in
/// `vdsl/scripts/new_profile.sh`, with the standing-prohibitions header
/// (SECRETS / NO DSL-BYPASS) pre-baked. `${NAME}` and `${OUT}` placeholders
/// are substituted with the provided `name` and the resolved output path.
///
/// # Arguments
///
/// * `name` — profile name slug; must match `^[a-zA-Z0-9_-]+$`
/// * `root` — projects root directory (pass the result of `resolve_projects_root`)
/// * `overwrite` — when `false` (default), return an error if the target file already
///   exists; when `true`, overwrite the existing file
///
/// # Returns
///
/// A [`ProfileScaffoldResult`] containing the absolute path of the created file and
/// whether it was newly created (`file_created = true`) or overwritten (`false`).
///
/// # Errors
///
/// - [`ProfileScaffoldError::InvalidName`] — if `name` fails validation
/// - [`ProfileScaffoldError::AlreadyExists`] — if the target file exists and `overwrite` is `false`
/// - [`ProfileScaffoldError::Io`] — if directory creation or file write fails
pub fn scaffold_profile(
    name: &str,
    root: &Path,
    overwrite: bool,
) -> Result<ProfileScaffoldResult, ProfileScaffoldError> {
    validate_profile_name(name)?;

    let profiles_dir = root.join("profiles");
    let target_path = profiles_dir.join(format!("{name}.lua"));

    let was_existing = target_path.exists();
    if was_existing && !overwrite {
        return Err(ProfileScaffoldError::AlreadyExists(
            target_path.display().to_string(),
        ));
    }

    std::fs::create_dir_all(&profiles_dir)?;

    let out_path_str = target_path.display().to_string();
    let content = PROFILE_TEMPLATE
        .replace("${NAME}", name)
        .replace("${OUT}", &out_path_str);

    std::fs::write(&target_path, &content)?;

    Ok(ProfileScaffoldResult {
        profile_path: target_path,
        file_created: !was_existing,
    })
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn setup() -> TempDir {
        // Safety: tempdir() is unlikely to fail in test environments; panic here
        // surfaces the error immediately rather than obscuring it.
        tempfile::tempdir().expect("tempdir")
    }

    /// T1: Happy path — valid name creates the expected file with correct content.
    #[test]
    fn test_scaffold_profile_creates_file() {
        let td = setup();
        let root = td.path().to_path_buf();
        let result = scaffold_profile("my_profile", &root, false).expect("scaffold");

        let expected_path = root.join("profiles").join("my_profile.lua");
        assert_eq!(result.profile_path, expected_path);
        assert!(result.file_created);
        assert!(expected_path.exists());

        let content = std::fs::read_to_string(&expected_path).expect("read");
        assert!(
            content.contains("Standing prohibitions"),
            "missing Standing prohibitions header"
        );
        assert!(
            content.contains(r#"name = "my_profile""#),
            "missing name substitution"
        );
        assert!(
            content.contains("print(profile:manifest_json(true))"),
            "missing manifest_json call"
        );
        assert!(content.contains("return profile"), "missing return profile");
    }

    /// T2: Boundary — overwrite=false (default) must fail with AlreadyExists on second call.
    #[test]
    fn test_scaffold_profile_overwrite_false_fails() {
        let td = setup();
        let root = td.path().to_path_buf();
        scaffold_profile("dup_profile", &root, false).expect("first scaffold");

        let err = scaffold_profile("dup_profile", &root, false)
            .expect_err("expected AlreadyExists error");
        assert!(
            matches!(err, ProfileScaffoldError::AlreadyExists(_)),
            "expected AlreadyExists, got {err:?}"
        );
    }

    /// T1: overwrite=true must succeed and overwrite the existing file.
    #[test]
    fn test_scaffold_profile_overwrite_true_succeeds() {
        let td = setup();
        let root = td.path().to_path_buf();
        scaffold_profile("over_profile", &root, false).expect("first scaffold");

        let result = scaffold_profile("over_profile", &root, true).expect("second scaffold");
        assert!(
            !result.file_created,
            "file_created should be false on overwrite"
        );
        assert!(result.profile_path.exists());
    }

    /// T2+T3: Edge case — empty name must return InvalidName.
    #[test]
    fn test_scaffold_profile_invalid_name_empty() {
        let td = setup();
        let root = td.path().to_path_buf();
        let err = scaffold_profile("", &root, false).expect_err("expected error");
        assert!(
            matches!(err, ProfileScaffoldError::InvalidName(_)),
            "expected InvalidName, got {err:?}"
        );
    }

    /// T2+T3: Edge case — slash in name must return InvalidName (path traversal prevention).
    #[test]
    fn test_scaffold_profile_invalid_name_slash() {
        let td = setup();
        let root = td.path().to_path_buf();
        let err = scaffold_profile("foo/bar", &root, false).expect_err("expected error");
        assert!(
            matches!(err, ProfileScaffoldError::InvalidName(_)),
            "expected InvalidName, got {err:?}"
        );
    }

    /// Optional: template must contain B2_ROOT literal.
    #[test]
    fn test_scaffold_profile_template_contains_b2_root() {
        let td = setup();
        let root = td.path().to_path_buf();
        let result = scaffold_profile("b2test", &root, false).expect("scaffold");
        let content = std::fs::read_to_string(&result.profile_path).expect("read");
        assert!(
            content.contains(r#"local B2_ROOT = "b2://run-pod-ZQyB""#),
            "missing B2_ROOT"
        );
    }

    /// Optional: template must contain vdsl_profile_apply reference.
    #[test]
    fn test_scaffold_profile_template_contains_apply_block() {
        let td = setup();
        let root = td.path().to_path_buf();
        let result = scaffold_profile("applytest", &root, false).expect("scaffold");
        let content = std::fs::read_to_string(&result.profile_path).expect("read");
        assert!(
            content.contains("vdsl_profile_apply("),
            "missing vdsl_profile_apply block"
        );
    }
}
