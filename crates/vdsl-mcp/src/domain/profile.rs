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
