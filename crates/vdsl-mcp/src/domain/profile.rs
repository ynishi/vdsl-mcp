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
    pub comfyui: ComfyUiConfig,

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

    /// Checkpoints / LoRAs / VAEs etc. to stage into
    /// `/workspace/ComfyUI/models/<subdir>/<dst>`.
    #[serde(default)]
    pub models: Vec<Model>,

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

    /// Extra argv tacked onto `python main.py ...` at launch.
    #[serde(default)]
    pub args: Option<String>,

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
}

/// `sync.pull` / `sync.push` config.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct SyncConfig {
    #[serde(default)]
    pub pull: Vec<SyncRoute>,

    #[serde(default)]
    pub push: Vec<SyncRoute>,
}

/// One `(src, dest)` edge reference.
///
/// `src` / `dest` are `LocationId` string forms: `"local"`, `"cloud"`,
/// or `"pod-{id}"`. Validated against `sdk.all_edges()` at phase
/// expansion time (design §2 PrimaryRoute ONLY).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SyncRoute {
    pub src: String,
    pub dest: String,
}

/// One model staging entry.
///
/// `subdir` is authoritative — MCP does not re-derive it from `kind`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Model {
    /// Source URI (`b2://...` or `file://...`).
    pub src: String,

    /// Destination filename (relative to `subdir`).
    pub dst: String,

    /// Model kind (`checkpoint`, `lora`, ..., or `"custom"` sentinel).
    pub kind: String,

    /// Sub-directory under `/workspace/ComfyUI/models/`.
    pub subdir: String,
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
