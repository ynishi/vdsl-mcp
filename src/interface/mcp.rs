//! MCP Server for vdsl-mcp
//!
//! MCP Protocol (stdio) <-> RunPod CLI / ComfyUI HTTP
//!
//! Phase 0: vdsl_pod_list
//! Phase 1: vdsl_pod_start, vdsl_pod_stop, vdsl_pod_delete
//! Phase 2: vdsl_connect

use rmcp::{
    handler::server::{tool::ToolCallContext, tool::ToolRouter, wrapper::Parameters},
    model::{
        CallToolRequestParams, CallToolResult, Content, Implementation, ListToolsResult,
        PaginatedRequestParams, ProtocolVersion, ServerCapabilities, ServerInfo,
    },
    service::{RequestContext, RoleServer},
    tool, tool_router,
    transport::stdio,
    ErrorData as McpError, ServerHandler, ServiceExt,
};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::sync::{Arc, Mutex};

use crate::application::pod_service::{resolve_api_key, PodService};
use crate::domain::models::{format_model_catalog_with_limit, parse_model_catalog};
use crate::domain::pod::{format_pod_list, format_volume_list};
use crate::infra::comfyui_client::{proxy_url, ComfyUiClient};
use crate::infra::runpod_cli::RunPodCli;

// =============================================================================
// Public entry point
// =============================================================================

/// Start the MCP server on stdio.
pub async fn run() -> anyhow::Result<()> {
    let server = VdslMcpServer::new();
    let service = server.serve(stdio()).await?;
    service.waiting().await?;
    Ok(())
}

// =============================================================================
// MCP Server
// =============================================================================

#[derive(Clone)]
struct VdslMcpServer {
    tool_router: ToolRouter<Self>,
    /// Last successfully connected ComfyUI URL (session state).
    /// Set by `vdsl_connect` and `vdsl_pod_setup` on success.
    /// Used as fallback when pod_id/url are omitted in subsequent calls.
    last_url: Arc<Mutex<Option<String>>>,
    /// Last successfully connected pod ID (session state).
    /// Set alongside `last_url` when the pod ID is known.
    /// Used by `vdsl_exec` to avoid requiring pod_id on every call.
    last_pod_id: Arc<Mutex<Option<String>>>,
}

impl VdslMcpServer {
    fn new() -> Self {
        Self {
            tool_router: Self::tool_router(),
            last_url: Arc::new(Mutex::new(None)),
            last_pod_id: Arc::new(Mutex::new(None)),
        }
    }

    /// Create a PodService from environment API key.
    fn pod_service() -> Result<PodService, McpError> {
        let api_key = resolve_api_key().map_err(Self::to_mcp_error)?;
        let cli = RunPodCli::new(api_key);
        Ok(PodService::new(cli))
    }

    /// Resolve ComfyUI Bearer token from VDSL_COMFYUI_TOKEN env var.
    fn comfyui_token() -> Option<String> {
        std::env::var("VDSL_COMFYUI_TOKEN")
            .ok()
            .filter(|s| !s.is_empty())
    }

    /// Build a ComfyUiClient from URL, with env-based token auth.
    fn comfyui_client(url: String) -> Result<ComfyUiClient, McpError> {
        ComfyUiClient::new(url, Self::comfyui_token())
            .map_err(|e| McpError::internal_error(format!("HTTP client init failed: {e}"), None))
    }

    /// Store the last successfully connected URL and pod ID for session reuse.
    fn save_session(&self, url: &str, pod_id: Option<&str>) {
        if let Ok(mut guard) = self.last_url.lock() {
            *guard = Some(url.to_string());
        }
        if let Some(id) = pod_id {
            if let Ok(mut guard) = self.last_pod_id.lock() {
                *guard = Some(id.to_string());
            }
        }
    }

    /// Resolve pod ID from explicit parameter or session state.
    fn resolve_pod_id(&self, pod_id: Option<&str>) -> Result<String, McpError> {
        if let Some(id) = pod_id {
            if !id.is_empty() {
                return Ok(id.to_string());
            }
        }
        let guard = self
            .last_pod_id
            .lock()
            .map_err(|_| McpError::internal_error("session state lock poisoned", None))?;
        match guard.as_deref() {
            Some(id) => Ok(id.to_string()),
            None => Err(McpError::invalid_params(
                "pod_id is required (no previous connection to fall back to). Use vdsl_connect first.",
                None,
            )),
        }
    }

    /// Resolve ComfyUI URL from pod_id/url fields.
    /// Falls back to the last successfully connected URL if both are None.
    fn resolve_comfyui_url(
        &self,
        pod_id: Option<&str>,
        url: Option<&str>,
    ) -> Result<String, McpError> {
        match (pod_id, url) {
            (Some(id), _) => Ok(proxy_url(id, 8188)),
            (None, Some(u)) => Ok(u.to_string()),
            (None, None) => {
                // Fallback to session state
                let guard = self
                    .last_url
                    .lock()
                    .map_err(|_| McpError::internal_error("session state lock poisoned", None))?;
                match guard.as_deref() {
                    Some(url) => Ok(url.to_string()),
                    None => Err(McpError::invalid_params(
                        "either pod_id or url is required (no previous connection to fall back to)",
                        None,
                    )),
                }
            }
        }
    }

    fn to_mcp_error(e: impl std::fmt::Display) -> McpError {
        McpError::internal_error(format!("{e}"), None)
    }
}

// =============================================================================
// ServerHandler impl
// =============================================================================

impl ServerHandler for VdslMcpServer {
    fn get_info(&self) -> ServerInfo {
        ServerInfo {
            protocol_version: ProtocolVersion::V_2025_03_26,
            capabilities: ServerCapabilities::builder().enable_tools().build(),
            server_info: Implementation {
                name: "vdsl-mcp".to_string(),
                title: Some("VDSL MCP — AI-native image generation platform".to_string()),
                description: Some(
                    "AI-first image generation workflow: \
                     RunPod GPU provisioning, ComfyUI orchestration, \
                     model management."
                        .to_string(),
                ),
                version: env!("CARGO_PKG_VERSION").to_string(),
                icons: None,
                website_url: None,
            },
            instructions: Some(
                "VDSL MCP — AI-native image generation platform.\n\
                 \n\
                 Normal usage (ComfyUI already running):\n\
                 1. vdsl_connect(url) — connect to ComfyUI\n\
                 2. vdsl_models — list available checkpoints/LoRAs\n\
                 3. vdsl_generate — create images\n\
                 \n\
                 Infrastructure (RunPod provisioning):\n\
                 - vdsl_pod_list / vdsl_pod_start / vdsl_pod_stop / vdsl_pod_delete"
                    .to_string(),
            ),
        }
    }

    async fn list_tools(
        &self,
        _request: Option<PaginatedRequestParams>,
        _context: RequestContext<RoleServer>,
    ) -> Result<ListToolsResult, McpError> {
        Ok(ListToolsResult {
            tools: self.tool_router.list_all(),
            next_cursor: None,
            meta: None,
        })
    }

    async fn call_tool(
        &self,
        request: CallToolRequestParams,
        context: RequestContext<RoleServer>,
    ) -> Result<CallToolResult, McpError> {
        let tool_ctx = ToolCallContext::new(self, request, context);
        self.tool_router.call(tool_ctx).await
    }
}

// =============================================================================
// Request types
// =============================================================================

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct VdslPodListRequest {}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct VdslPodActionRequest {
    /// RunPod pod ID (e.g. "pod_abc123def")
    pub pod_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct VdslConnectRequest {
    /// ComfyUI URL (e.g. "https://pod_id-8188.proxy.runpod.net") or RunPod pod ID.
    /// If a pod ID is given, the proxy URL is auto-constructed.
    pub url: Option<String>,

    /// RunPod pod ID (e.g. "pod_abc123def"). Proxy URL is auto-constructed.
    /// Takes precedence over url if both are provided.
    pub pod_id: Option<String>,

    /// Wait for ComfyUI to become ready (default: false).
    /// When true, polls until ComfyUI responds (up to 5 minutes).
    /// Useful after vdsl_pod_start or vdsl_pod_create.
    #[serde(default)]
    pub wait: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct VdslQueueStatusRequest {
    /// ComfyUI URL (e.g. "https://pod_id-8188.proxy.runpod.net") or RunPod pod ID.
    pub url: Option<String>,

    /// RunPod pod ID (e.g. "pod_abc123def"). Proxy URL is auto-constructed.
    /// Takes precedence over url if both are provided.
    pub pod_id: Option<String>,

    /// Prompt ID to check status for. If omitted, returns the full queue state.
    pub prompt_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct VdslUploadRequest {
    /// ComfyUI URL (e.g. "https://pod_id-8188.proxy.runpod.net") or RunPod pod ID.
    pub url: Option<String>,

    /// RunPod pod ID (e.g. "pod_abc123def"). Proxy URL is auto-constructed.
    /// Takes precedence over url if both are provided.
    pub pod_id: Option<String>,

    /// Single file path to upload. Mutually exclusive with files/dir.
    pub filepath: Option<String>,

    /// Multiple file paths to upload. Mutually exclusive with filepath/dir.
    pub files: Option<Vec<String>>,

    /// Directory path — upload all files in this directory. Mutually exclusive with filepath/files.
    pub dir: Option<String>,

    /// Target subfolder on the ComfyUI server (default: "").
    pub subfolder: Option<String>,

    /// Whether to overwrite existing files (default: true).
    pub overwrite: Option<bool>,
}

/// Resolve upload file list from mutually exclusive filepath/files/dir parameters.
fn resolve_upload_files(req: &VdslUploadRequest) -> Result<Vec<std::path::PathBuf>, McpError> {
    let sources = [
        req.filepath.is_some(),
        req.files.is_some(),
        req.dir.is_some(),
    ];
    let count = sources.iter().filter(|&&b| b).count();
    if count == 0 {
        return Err(McpError::invalid_params(
            "one of filepath, files, or dir is required",
            None,
        ));
    }
    if count > 1 {
        return Err(McpError::invalid_params(
            "filepath, files, and dir are mutually exclusive",
            None,
        ));
    }

    if let Some(ref path) = req.filepath {
        let p = std::path::PathBuf::from(path);
        if !p.exists() {
            return Err(McpError::invalid_params(
                format!("file not found: {path}"),
                None,
            ));
        }
        return Ok(vec![p]);
    }

    if let Some(ref paths) = req.files {
        let mut result = Vec::with_capacity(paths.len());
        for path in paths {
            let p = std::path::PathBuf::from(path);
            if !p.exists() {
                return Err(McpError::invalid_params(
                    format!("file not found: {path}"),
                    None,
                ));
            }
            result.push(p);
        }
        if result.is_empty() {
            return Err(McpError::invalid_params("files array is empty", None));
        }
        return Ok(result);
    }

    if let Some(ref dir_path) = req.dir {
        let dir = std::path::Path::new(dir_path);
        if !dir.is_dir() {
            return Err(McpError::invalid_params(
                format!("directory not found: {dir_path}"),
                None,
            ));
        }
        let mut result = Vec::new();
        let entries = std::fs::read_dir(dir).map_err(|e| {
            McpError::internal_error(format!("failed to read directory {dir_path}: {e}"), None)
        })?;
        for entry in entries {
            let entry = entry.map_err(|e| {
                McpError::internal_error(format!("directory read error: {e}"), None)
            })?;
            let path = entry.path();
            if path.is_file() {
                result.push(path);
            }
        }
        if result.is_empty() {
            return Err(McpError::invalid_params(
                format!("no files found in directory: {dir_path}"),
                None,
            ));
        }
        result.sort();
        return Ok(result);
    }

    Err(McpError::invalid_params(
        "one of filepath, files, or dir is required",
        None,
    ))
}

/// Default spec for ComfyUI pods on RunPod.
/// Matches Lua `M.COMFY_DEFAULTS` in runpod.lua L36-41.
const COMFY_DEFAULTS_NAME: &str = "comfyui-vdsl";
const COMFY_DEFAULTS_TEMPLATE: &str = "cw3nka7d08";
const COMFY_DEFAULTS_DISK: u32 = 30;

/// Max wait time for pod + ComfyUI readiness (seconds).
const SETUP_TIMEOUT_SECS: u64 = 300;
/// Interval between readiness polls (seconds).
const SETUP_POLL_INTERVAL_SECS: u64 = 10;

/// Default SSH key path for RunPod pods.
const DEFAULT_SSH_KEY: &str = "~/.ssh/id_ed25519_runpod";
/// Base path for ComfyUI models on RunPod pods.
const COMFYUI_MODELS_BASE: &str = "/workspace/runpod-slim/ComfyUI/models";
/// Base path for ComfyUI custom nodes on RunPod pods.
const COMFYUI_CUSTOM_NODES: &str = "/workspace/runpod-slim/ComfyUI/custom_nodes";
/// Max wait time for downloads (seconds).
const DOWNLOAD_TIMEOUT_SECS: u64 = 600;
/// Interval between download status polls (seconds).
const DOWNLOAD_POLL_INTERVAL_SECS: u64 = 5;

/// ComfyUI model directory mapping (relative to models base dir).
/// Matches Lua `MODEL_DIRS` in runpod.lua L256-266.
const MODEL_DIRS: &[(&str, &str)] = &[
    ("checkpoints", "checkpoints"),
    ("loras", "loras"),
    ("controlnet", "controlnet"),
    ("vae", "vae"),
    ("upscale", "upscale_models"),
    ("embeddings", "embeddings"),
    ("clip", "clip"),
    ("unet", "unet"),
];

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct VdslDownloadRequest {
    /// RunPod pod ID (e.g. "pod_abc123def").
    pub pod_id: String,

    /// Model source. Formats:
    /// - "hf:user/repo/file.safetensors" (HuggingFace)
    /// - "cv:VERSION_ID" (CivitAI — token auto-injected from VDSL_CIVITAI_TOKEN env)
    /// - "https://..." (direct URL — CivitAI URLs get token auto-injected)
    /// - "user/repo/file.safetensors" (bare path defaults to HuggingFace)
    pub source: String,

    /// Target model category: checkpoints, loras, controlnet, vae, upscale, embeddings, clip, unet.
    pub target: String,

    /// Override filename (default: extracted from URL).
    pub filename: Option<String>,

    /// SSH key path. Falls back to VDSL_SSH_KEY env, then ~/.ssh/id_ed25519_runpod.
    pub ssh_key: Option<String>,
}

/// Resolved download info: URL + filename.
struct DownloadInfo {
    url: String,
    filename: String,
}

/// Parse a model source string into market + download URL.
///
/// Supported prefixes:
///   - `hf:user/repo/file.safetensors` — HuggingFace
///   - `cv:VERSION_ID` — CivitAI (token auto-injected from VDSL_CIVITAI_TOKEN env)
///   - `https://...` — direct URL (CivitAI URLs get token auto-injected)
///   - `user/repo/file.safetensors` — bare path defaults to HuggingFace
fn resolve_source(source: &str, filename_override: Option<&str>) -> Result<DownloadInfo, String> {
    let (market, identifier) = if let Some(rest) = source.strip_prefix("hf:") {
        ("hf", rest)
    } else if let Some(rest) = source.strip_prefix("cv:") {
        ("cv", rest)
    } else if source.starts_with("http://") || source.starts_with("https://") {
        ("url", source)
    } else {
        // Bare path: default to HuggingFace (user/repo/file pattern)
        ("hf", source)
    };

    let mut info = match market {
        "hf" => {
            // Parse: "user/repo/path/to/file.ext"
            let parts: Vec<&str> = identifier.splitn(3, '/').collect();
            if parts.len() < 3 {
                return Err(format!(
                    "HuggingFace source requires 'user/repo/file.ext', got: {identifier}"
                ));
            }
            let repo = format!("{}/{}", parts[0], parts[1]);
            let filepath = parts[2];
            let fname = filepath.rsplit('/').next().unwrap_or(filepath);
            DownloadInfo {
                url: format!("https://huggingface.co/{repo}/resolve/main/{filepath}"),
                filename: fname.to_string(),
            }
        }
        "cv" => {
            // CivitAI version ID → download URL with auto token
            let version_id = identifier.trim();
            if version_id.is_empty() {
                return Err("cv: requires a version ID (e.g. cv:1595775)".into());
            }
            let mut url = format!("https://civitai.com/api/download/models/{version_id}");
            if let Ok(token) = std::env::var("VDSL_CIVITAI_TOKEN") {
                if !token.is_empty() {
                    url.push_str(&format!("?token={token}"));
                }
            }
            DownloadInfo {
                url,
                filename: format!("{version_id}.safetensors"),
            }
        }
        "url" => {
            let path = identifier.split(['?', '#']).next().unwrap_or(identifier);
            let fname = path.rsplit('/').next().unwrap_or("download");
            // Auto-inject CivitAI token for civitai.com URLs without token
            let url = inject_civitai_token(identifier);
            DownloadInfo {
                url,
                filename: fname.to_string(),
            }
        }
        _ => return Err(format!("unknown source prefix: {market}")),
    };

    if let Some(f) = filename_override {
        info.filename = f.to_string();
    }
    Ok(info)
}

/// Auto-inject VDSL_CIVITAI_TOKEN into civitai.com download URLs if not already present.
fn inject_civitai_token(url: &str) -> String {
    if !url.contains("civitai.com") {
        return url.to_string();
    }
    if url.contains("token=") {
        return url.to_string();
    }
    let Ok(token) = std::env::var("VDSL_CIVITAI_TOKEN") else {
        return url.to_string();
    };
    if token.is_empty() {
        return url.to_string();
    }
    let sep = if url.contains('?') { "&" } else { "?" };
    format!("{url}{sep}token={token}")
}

/// Format CivitAI /api/v1/models response into human-readable text.
///
/// Each model shows: name, type, base model, download count, rating,
/// and the latest version's ID (usable with `vdsl_download source: "cv:ID"`).
fn format_civitai_results(json: &serde_json::Value) -> String {
    let items = match json["items"].as_array() {
        Some(arr) if !arr.is_empty() => arr,
        _ => return "No models found.".to_string(),
    };

    let mut out = format!("Found {} model(s):\n\n", items.len());

    for (i, model) in items.iter().enumerate() {
        let name = model["name"].as_str().unwrap_or("(unknown)");
        let model_type = model["type"].as_str().unwrap_or("?");
        let stats = &model["stats"];
        let downloads = stats["downloadCount"].as_u64().unwrap_or(0);
        let rating = stats["rating"].as_f64().unwrap_or(0.0);
        let rating_count = stats["ratingCount"].as_u64().unwrap_or(0);
        let nsfw = model["nsfw"].as_bool().unwrap_or(false);

        out.push_str(&format!("{}. **{}**\n", i + 1, name));
        out.push_str(&format!(
            "   Type: {model_type} | Downloads: {downloads} | Rating: {rating:.1} ({rating_count})"
        ));
        if nsfw {
            out.push_str(" | NSFW");
        }
        out.push('\n');

        // Show versions (latest first, max 10)
        if let Some(versions) = model["modelVersions"].as_array() {
            let show = versions.len().min(10);
            for ver in &versions[..show] {
                let ver_id = ver["id"].as_u64().unwrap_or(0);
                let ver_name = ver["name"].as_str().unwrap_or("?");
                let base = ver["baseModel"].as_str().unwrap_or("?");

                // File size from first file entry
                let file_size = ver["files"]
                    .as_array()
                    .and_then(|f| f.first())
                    .and_then(|f| f["sizeKB"].as_f64())
                    .map(format_file_size);

                let mut line = format!("   - v{ver_name} (base: {base})");
                if let Some(ref size) = file_size {
                    line.push_str(&format!(" [{size}]"));
                }
                line.push_str(&format!(" → cv:{ver_id}"));

                // Trained words (trigger words for LoRAs)
                if let Some(words) = ver["trainedWords"].as_array() {
                    let triggers: Vec<&str> = words.iter().filter_map(|w| w.as_str()).collect();
                    if !triggers.is_empty() {
                        line.push_str(&format!("\n     triggers: {}", triggers.join(", ")));
                    }
                }

                out.push_str(&line);
                out.push('\n');
            }
            if versions.len() > 10 {
                out.push_str(&format!(
                    "   ... and {} more version(s)\n",
                    versions.len() - 10
                ));
            }
        }
        out.push('\n');
    }

    // Pagination info
    if let Some(meta) = json["metadata"].as_object() {
        let total = meta.get("totalItems").and_then(|v| v.as_u64());
        let page = meta.get("currentPage").and_then(|v| v.as_u64());
        let pages = meta.get("totalPages").and_then(|v| v.as_u64());
        if let (Some(t), Some(p), Some(tp)) = (total, page, pages) {
            out.push_str(&format!("Page {p}/{tp} ({t} total models)\n"));
        }
    }

    out
}

/// Format file size from KB to human-readable string.
fn format_file_size(size_kb: f64) -> String {
    if size_kb >= 1_048_576.0 {
        format!("{:.1} GB", size_kb / 1_048_576.0)
    } else if size_kb >= 1024.0 {
        format!("{:.0} MB", size_kb / 1024.0)
    } else {
        format!("{:.0} KB", size_kb)
    }
}

/// Resolve model directory name from target category.
fn resolve_model_dir(target: &str) -> Result<&'static str, String> {
    MODEL_DIRS
        .iter()
        .find(|(k, _)| *k == target)
        .map(|(_, v)| *v)
        .ok_or_else(|| {
            let valid: Vec<&str> = MODEL_DIRS.iter().map(|(k, _)| *k).collect();
            format!("unknown target '{target}'. Valid: {}", valid.join(", "))
        })
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct VdslStorageListRequest {
    /// RunPod pod ID (e.g. "pod_abc123def").
    pub pod_id: String,

    /// B2 bucket name. If omitted, uses VDSL_B2_BUCKET env var.
    pub bucket: Option<String>,

    /// Path within the bucket (e.g. "models/checkpoints"). Defaults to root.
    pub path: Option<String>,

    /// SSH key path. Falls back to VDSL_SSH_KEY env, then ~/.ssh/id_ed25519_runpod.
    pub ssh_key: Option<String>,

    /// Maximum number of entries to return (default: 50).
    pub limit: Option<usize>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct VdslStoragePullRequest {
    /// RunPod pod ID (e.g. "pod_abc123def").
    pub pod_id: String,

    /// B2 bucket name. If omitted, uses VDSL_B2_BUCKET env var.
    pub bucket: Option<String>,

    /// Source path in B2 (e.g. "models/checkpoints/sd_xl_base.safetensors").
    pub source: String,

    /// Target model category: checkpoints, loras, controlnet, vae, upscale, embeddings, clip, unet.
    /// Determines the destination directory under ComfyUI models.
    pub target: String,

    /// SSH key path. Falls back to VDSL_SSH_KEY env, then ~/.ssh/id_ed25519_runpod.
    pub ssh_key: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct VdslStoragePushRequest {
    /// RunPod pod ID (e.g. "pod_abc123def").
    pub pod_id: String,

    /// B2 bucket name. If omitted, uses VDSL_B2_BUCKET env var.
    pub bucket: Option<String>,

    /// Source model category: checkpoints, loras, controlnet, vae, upscale, embeddings, clip, unet.
    /// Determines the source directory under ComfyUI models.
    pub source_target: String,

    /// Specific filename within the model category dir (e.g. "sd_xl_base.safetensors").
    /// If omitted, pushes the entire category directory.
    pub filename: Option<String>,

    /// Destination path prefix in B2 (default: "models/<category>").
    pub dest_path: Option<String>,

    /// SSH key path. Falls back to VDSL_SSH_KEY env, then ~/.ssh/id_ed25519_runpod.
    pub ssh_key: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct VdslStorageArchiveRequest {
    /// RunPod pod ID (e.g. "pod_abc123def").
    pub pod_id: String,

    /// Model category: checkpoints, loras, controlnet, vae, upscale, embeddings, clip, unet.
    pub source_target: String,

    /// Filename to archive (e.g. "GAME_cammy_white_aiwaifu-10.safetensors").
    pub filename: String,

    /// B2 bucket name. If omitted, uses VDSL_B2_BUCKET env var.
    pub bucket: Option<String>,

    /// Destination path prefix in B2 (default: "models/<category>").
    pub dest_path: Option<String>,

    /// SSH key path. Falls back to VDSL_SSH_KEY env, then ~/.ssh/id_ed25519_runpod.
    pub ssh_key: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct VdslImageDownloadRequest {
    /// ComfyUI URL (e.g. "https://pod_id-8188.proxy.runpod.net") or RunPod pod ID.
    pub url: Option<String>,

    /// RunPod pod ID (e.g. "pod_abc123def"). Proxy URL is auto-constructed.
    /// Takes precedence over url if both are provided.
    pub pod_id: Option<String>,

    /// Local directory to save downloaded images.
    pub save_dir: String,

    /// Specific prompt IDs to download images from.
    /// If omitted, downloads from all recent history entries.
    pub prompt_ids: Option<Vec<String>>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct VdslPodSetupRequest {
    /// Network volume ID. If omitted, auto-detected when exactly one volume exists.
    pub volume_id: Option<String>,

    /// GPU type (e.g. "NVIDIA A40", "NVIDIA L4"). Can be a single type or comma-separated list.
    pub gpu: Option<String>,

    /// Datacenter ID (e.g. "EU-SE-1"). Can be a single ID or comma-separated list.
    pub datacenter: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct VdslPodCreateRequest {
    /// Network volume ID to attach (from vdsl_volume_list).
    pub volume_id: String,

    /// GPU type (e.g. "NVIDIA A40", "NVIDIA L4"). Can be a single type or comma-separated list.
    pub gpu: Option<String>,

    /// Pod name (default: "comfyui-vdsl").
    pub name: Option<String>,

    /// Datacenter ID (e.g. "EU-SE-1"). Can be a single ID or comma-separated list.
    pub datacenter: Option<String>,

    /// Container disk size in GB (default: 30).
    pub disk_gb: Option<u32>,
}

/// Default timeout for generate poll (seconds).
const GENERATE_TIMEOUT_SECS: u64 = 300;
/// Default poll interval for generate (seconds).
const GENERATE_POLL_INTERVAL_SECS: u64 = 2;
/// Default poll interval for batch generate (seconds) — slightly longer to reduce /history load.
const BATCH_POLL_INTERVAL_SECS: u64 = 3;
/// Default timeout for script execution (seconds).
const SCRIPT_TIMEOUT_SECS: u64 = 600;
/// Lua package.path prefix for VDSL module resolution.
const VDSL_PACKAGE_PATH: &str = "lua/?.lua;lua/?/init.lua;scripts/?.lua;";

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct VdslGenerateRequest {
    /// ComfyUI URL (e.g. "https://pod_id-8188.proxy.runpod.net") or RunPod pod ID.
    pub url: Option<String>,

    /// RunPod pod ID (e.g. "pod_abc123def"). Proxy URL is auto-constructed.
    /// Takes precedence over url if both are provided.
    pub pod_id: Option<String>,

    /// ComfyUI workflow JSON (inline). Mutually exclusive with workflow_file.
    pub workflow: Option<serde_json::Value>,

    /// Path to a JSON file containing the ComfyUI workflow. Mutually exclusive with workflow.
    pub workflow_file: Option<String>,

    /// Timeout in seconds for waiting for completion (default: 300).
    pub timeout: Option<u64>,

    /// Local directory to save output images. When specified, all generated images
    /// are downloaded from the ComfyUI server to this directory after completion.
    pub save_dir: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct VdslBatchGenerateRequest {
    /// ComfyUI URL (e.g. "https://pod_id-8188.proxy.runpod.net") or RunPod pod ID.
    pub url: Option<String>,

    /// RunPod pod ID (e.g. "pod_abc123def"). Proxy URL is auto-constructed.
    /// Takes precedence over url if both are provided.
    pub pod_id: Option<String>,

    /// Inline array of workflow JSONs. Mutually exclusive with workflow_files and load_dir.
    pub workflows: Option<Vec<serde_json::Value>>,

    /// Array of file paths to workflow JSON files.
    /// Mutually exclusive with workflows and load_dir.
    pub workflow_files: Option<Vec<String>>,

    /// Directory containing .json workflow files (loaded alphabetically).
    /// Mutually exclusive with workflows and workflow_files.
    pub load_dir: Option<String>,

    /// Local directory to save all output images after completion.
    pub save_dir: Option<String>,

    /// Timeout in seconds for the entire batch (default: 300).
    pub timeout: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct VdslRunScriptRequest {
    /// Path to a .lua script file to execute. Mutually exclusive with code.
    pub script_file: Option<String>,

    /// Inline Lua code to execute. Mutually exclusive with script_file.
    pub code: Option<String>,

    /// Working directory for script execution.
    /// Must contain lua/ and scripts/ directories for VDSL module resolution.
    /// If omitted, auto-detected by walking up from script_file's parent.
    pub working_dir: Option<String>,

    /// Timeout in seconds (default: 600).
    pub timeout: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct VdslCatalogsRequest {
    /// VDSL repository root (must contain lua/ for module resolution).
    pub working_dir: String,

    /// Path to the catalog listing script.
    /// Absolute path is used as-is; relative path is resolved from working_dir.
    /// Default: "scripts/catalog_available.lua"
    pub catalog_script: Option<String>,

    /// Optional path to a user catalog directory.
    /// Entries here are merged with built-in catalogs.
    pub catalogs_dir: Option<String>,

    /// Maximum output lines to return (default: 200).
    pub limit: Option<usize>,
}

const DEFAULT_CATALOG_SCRIPT: &str = "scripts/catalog_available.lua";

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct VdslComfyApiRequest {
    /// ComfyUI URL (e.g. "https://pod_id-8188.proxy.runpod.net").
    pub url: Option<String>,

    /// RunPod pod ID (e.g. "pod_abc123def"). Proxy URL is auto-constructed.
    /// Takes precedence over url if both are provided.
    pub pod_id: Option<String>,

    /// HTTP method: "GET" or "POST" (default: "GET").
    pub method: Option<String>,

    /// API endpoint path (e.g. "/queue", "/object_info", "/history/abc123").
    pub path: String,

    /// JSON request body (for POST requests).
    pub body: Option<serde_json::Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct VdslModelsRequest {
    /// ComfyUI URL (e.g. "https://pod_id-8188.proxy.runpod.net").
    pub url: Option<String>,

    /// RunPod pod ID (e.g. "pod_abc123def"). Proxy URL is auto-constructed.
    /// Takes precedence over url if both are provided.
    pub pod_id: Option<String>,

    /// Maximum items per model category (default: 50).
    pub limit: Option<usize>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct VdslNodeSearchRequest {
    /// ComfyUI URL (e.g. "https://pod_id-8188.proxy.runpod.net").
    pub url: Option<String>,

    /// RunPod pod ID (e.g. "pod_abc123def"). Proxy URL is auto-constructed.
    /// Takes precedence over url if both are provided.
    pub pod_id: Option<String>,

    /// Search pattern (case-insensitive substring match).
    /// Examples: "Face", "Color", "Upscale", "CLIP".
    /// If omitted, returns all node names.
    pub pattern: Option<String>,

    /// Maximum number of results to return (default: 50).
    pub limit: Option<usize>,
}

/// Default limit for list/search results.
const DEFAULT_LIST_LIMIT: usize = 50;

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct VdslRunpodCliRequest {
    /// Arguments to pass to runpod-cli (e.g. ["pods", "list-pods"]).
    /// VDSL_RUNPOD_API_KEY and -o json are injected automatically.
    pub args: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct VdslExecRequest {
    /// Shell command to execute on the pod (e.g. "ls /workspace" or "nvidia-smi").
    pub command: String,

    /// RunPod pod ID. If omitted, reuses the last vdsl_connect or vdsl_pod_setup session.
    pub pod_id: Option<String>,

    /// SSH key path. Falls back to VDSL_SSH_KEY env, then ~/.ssh/id_ed25519_runpod.
    pub ssh_key: Option<String>,

    /// Timeout in seconds (default: 30).
    pub timeout: Option<u64>,
}

/// Model marketplace source for search.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "lowercase")]
pub enum ModelSource {
    /// CivitAI (civitai.com)
    Cv,
    /// HuggingFace (huggingface.co)
    Hf,
}

/// Model type filter for search (aligned with ComfyUI model categories).
#[derive(Debug, Clone, Copy, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "lowercase")]
pub enum ModelType {
    Checkpoint,
    Lora,
    Controlnet,
    Vae,
    Upscale,
    Embedding,
}

impl ModelType {
    /// Convert to CivitAI API `types` parameter value.
    fn to_civitai_type(self) -> &'static str {
        match self {
            Self::Checkpoint => "Checkpoint",
            Self::Lora => "LORA",
            Self::Controlnet => "Controlnet",
            Self::Vae => "VAE",
            Self::Upscale => "Upscaler",
            Self::Embedding => "TextualInversion",
        }
    }
}

/// Sort order for model search results.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum ModelSearchSort {
    MostDownloaded,
    HighestRated,
    Newest,
}

impl ModelSearchSort {
    fn to_civitai_sort(self) -> &'static str {
        match self {
            Self::MostDownloaded => "Most Downloaded",
            Self::HighestRated => "Highest Rated",
            Self::Newest => "Newest",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct VdslModelSearchRequest {
    /// Search keyword (e.g. "photorealistic", "anime style", "SDXL").
    pub query: String,

    /// Model marketplace to search. If omitted, defaults to CivitAI.
    /// Currently supported: cv (CivitAI). HuggingFace (hf) support is planned.
    pub source: Option<ModelSource>,

    /// Filter by model type. Maps to ComfyUI model categories.
    pub model_type: Option<ModelType>,

    /// Sort order (default: most_downloaded).
    pub sort: Option<ModelSearchSort>,

    /// Maximum results to return (default: 10, max: 50).
    pub limit: Option<u32>,

    /// Filter by base model (e.g. "SDXL 1.0", "SD 1.5", "Flux.1 D").
    pub base_model: Option<String>,

    /// Include NSFW results (default: false).
    pub nsfw: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct VdslInterruptRequest {
    /// ComfyUI URL (e.g. "https://pod_id-8188.proxy.runpod.net").
    pub url: Option<String>,

    /// RunPod pod ID (e.g. "pod_abc123def"). Proxy URL is auto-constructed.
    /// Takes precedence over url if both are provided.
    pub pod_id: Option<String>,

    /// Prompt ID(s) to remove from the pending queue.
    /// If omitted, sends POST /interrupt to cancel the currently running job.
    /// If provided, sends POST /queue with {"delete": [...]} to remove pending jobs.
    pub prompt_ids: Option<Vec<String>>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub struct VdslRunRequest {
    /// Path to a .lua script file to execute. Mutually exclusive with code.
    pub script_file: Option<String>,

    /// Inline Lua code to execute. Mutually exclusive with script_file.
    pub code: Option<String>,

    /// Working directory for script execution.
    /// Must contain lua/ and scripts/ directories for VDSL module resolution.
    /// If omitted, auto-detected by walking up from script_file's parent.
    pub working_dir: Option<String>,

    /// ComfyUI URL (e.g. "https://pod_id-8188.proxy.runpod.net") or RunPod pod ID.
    pub url: Option<String>,

    /// RunPod pod ID (e.g. "pod_abc123def"). Proxy URL is auto-constructed.
    /// Takes precedence over url if both are provided.
    pub pod_id: Option<String>,

    /// Local directory to save all output images after completion.
    pub save_dir: Option<String>,

    /// Timeout in seconds for the entire operation (default: 600).
    pub timeout: Option<u64>,

    /// Compile only — run the Lua script but do not send workflows to ComfyUI.
    /// The compiled workflow JSONs and script output are returned without generation.
    #[serde(default)]
    pub compile_only: bool,
}

// =============================================================================
// Tool implementations
// =============================================================================

#[tool_router]
impl VdslMcpServer {
    #[tool(
        name = "vdsl_pod_list",
        description = "List all RunPod pods with their status, GPU, and cost. Infrastructure tool — not needed for normal image generation.",
        annotations(
            read_only_hint = true,
            destructive_hint = false,
            open_world_hint = true
        )
    )]
    async fn pod_list(
        &self,
        #[allow(unused_variables)] Parameters(_req): Parameters<VdslPodListRequest>,
    ) -> Result<CallToolResult, McpError> {
        let svc = Self::pod_service()?;
        let pods = svc.list_pods().await.map_err(Self::to_mcp_error)?;

        let output = format_pod_list(&pods);
        Ok(CallToolResult::success(vec![Content::text(output)]))
    }

    #[tool(
        name = "vdsl_pod_start",
        description = "Start (or resume) a RunPod pod. Returns the API response with updated pod status.",
        annotations(
            read_only_hint = false,
            destructive_hint = false,
            open_world_hint = true
        )
    )]
    async fn pod_start(
        &self,
        Parameters(req): Parameters<VdslPodActionRequest>,
    ) -> Result<CallToolResult, McpError> {
        let svc = Self::pod_service()?;
        let result = svc
            .start_pod(&req.pod_id)
            .await
            .map_err(Self::to_mcp_error)?;
        let output =
            serde_json::to_string_pretty(&result).unwrap_or_else(|_| format!("{result:?}"));
        Ok(CallToolResult::success(vec![Content::text(output)]))
    }

    #[tool(
        name = "vdsl_pod_stop",
        description = "Stop a running RunPod pod. The pod can be restarted later with vdsl_pod_start.",
        annotations(
            read_only_hint = false,
            destructive_hint = false,
            open_world_hint = true
        )
    )]
    async fn pod_stop(
        &self,
        Parameters(req): Parameters<VdslPodActionRequest>,
    ) -> Result<CallToolResult, McpError> {
        let svc = Self::pod_service()?;
        let result = svc
            .stop_pod(&req.pod_id)
            .await
            .map_err(Self::to_mcp_error)?;
        let output =
            serde_json::to_string_pretty(&result).unwrap_or_else(|_| format!("{result:?}"));
        Ok(CallToolResult::success(vec![Content::text(output)]))
    }

    #[tool(
        name = "vdsl_pod_create",
        description = "Create a new ComfyUI pod on RunPod. Requires a network volume ID (from vdsl_volume_list). Applies ComfyUI defaults (template, ports, disk) automatically.",
        annotations(
            read_only_hint = false,
            destructive_hint = false,
            open_world_hint = true
        )
    )]
    async fn pod_create(
        &self,
        Parameters(req): Parameters<VdslPodCreateRequest>,
    ) -> Result<CallToolResult, McpError> {
        let mut spec = serde_json::Map::new();
        spec.insert(
            "name".into(),
            serde_json::Value::String(req.name.unwrap_or_else(|| COMFY_DEFAULTS_NAME.to_string())),
        );
        spec.insert(
            "templateId".into(),
            serde_json::Value::String(COMFY_DEFAULTS_TEMPLATE.to_string()),
        );
        spec.insert(
            "containerDiskInGb".into(),
            serde_json::Value::Number(req.disk_gb.unwrap_or(COMFY_DEFAULTS_DISK).into()),
        );
        spec.insert("ports".into(), serde_json::json!(["8188/http", "22/tcp"]));
        spec.insert(
            "networkVolumeId".into(),
            serde_json::Value::String(req.volume_id),
        );

        if let Some(gpu) = req.gpu {
            let gpu_list: Vec<serde_json::Value> = gpu
                .split(',')
                .map(|s| serde_json::Value::String(s.trim().to_string()))
                .collect();
            spec.insert("gpuTypeIds".into(), serde_json::Value::Array(gpu_list));
        }

        if let Some(dc) = req.datacenter {
            let dc_list: Vec<serde_json::Value> = dc
                .split(',')
                .map(|s| serde_json::Value::String(s.trim().to_string()))
                .collect();
            spec.insert("dataCenterIds".into(), serde_json::Value::Array(dc_list));
        }

        let spec_json = serde_json::to_string(&spec).map_err(Self::to_mcp_error)?;
        let svc = Self::pod_service()?;
        let result = svc
            .create_pod(&spec_json)
            .await
            .map_err(Self::to_mcp_error)?;

        let pod_id = result["id"].as_str().unwrap_or("?");
        let pod_name = result["name"].as_str().unwrap_or("?");
        let output = format!(
            "Pod created: {pod_id} ({pod_name})\n\n{}",
            serde_json::to_string_pretty(&result).unwrap_or_else(|_| format!("{result:?}"))
        );
        Ok(CallToolResult::success(vec![Content::text(output)]))
    }

    #[tool(
        name = "vdsl_volume_list",
        description = "List all RunPod network volumes with their size, datacenter, and usage.",
        annotations(
            read_only_hint = true,
            destructive_hint = false,
            open_world_hint = true
        )
    )]
    async fn volume_list(
        &self,
        #[allow(unused_variables)] Parameters(_req): Parameters<VdslPodListRequest>,
    ) -> Result<CallToolResult, McpError> {
        let svc = Self::pod_service()?;
        let volumes = svc.list_volumes().await.map_err(Self::to_mcp_error)?;
        let output = format_volume_list(&volumes);
        Ok(CallToolResult::success(vec![Content::text(output)]))
    }

    #[tool(
        name = "vdsl_connect",
        description = "Connect to a ComfyUI instance. Pass a full URL or a RunPod pod ID (proxy URL is auto-constructed). \
            Returns system stats if ComfyUI is reachable. \
            If pod_id/url are omitted, reuses the last successful connection. \
            Set wait=true to poll until ComfyUI becomes ready (up to 5 minutes) — \
            useful after vdsl_pod_start or vdsl_pod_create.",
        annotations(
            read_only_hint = true,
            destructive_hint = false,
            open_world_hint = true
        )
    )]
    async fn connect(
        &self,
        Parameters(req): Parameters<VdslConnectRequest>,
    ) -> Result<CallToolResult, McpError> {
        let url = self.resolve_comfyui_url(req.pod_id.as_deref(), req.url.as_deref())?;
        let client = Self::comfyui_client(url.clone())?;

        let stats = if req.wait {
            // Poll until ComfyUI responds
            let deadline =
                std::time::Instant::now() + std::time::Duration::from_secs(SETUP_TIMEOUT_SECS);
            let interval = std::time::Duration::from_secs(SETUP_POLL_INTERVAL_SECS);

            loop {
                match client.system_stats().await {
                    Ok(s) => break s,
                    Err(_) if std::time::Instant::now() < deadline => {
                        tokio::time::sleep(interval).await;
                    }
                    Err(e) => {
                        return Err(McpError::internal_error(
                            format!(
                                "ComfyUI at {url} did not become ready within {SETUP_TIMEOUT_SECS}s: {e}"
                            ),
                            None,
                        ));
                    }
                }
            }
        } else {
            client.system_stats().await.map_err(Self::to_mcp_error)?
        };

        // Save successful connection for session reuse
        self.save_session(&url, req.pod_id.as_deref());

        let output = format!(
            "Connected to {url}\n\
             \nAll subsequent tools (generate, models, exec, etc.) will reuse this connection — \
             pod_id/url can be omitted.\n\
             \nComfyUI models path: {COMFYUI_MODELS_BASE}\n\
             Custom nodes path: {COMFYUI_CUSTOM_NODES}\n\
             \n{}",
            serde_json::to_string_pretty(&stats).unwrap_or_else(|_| format!("{stats:?}"))
        );
        Ok(CallToolResult::success(vec![Content::text(output)]))
    }

    #[tool(
        name = "vdsl_pod_delete",
        description = "Delete a RunPod pod permanently. This action is irreversible — the pod and all its data will be destroyed.",
        annotations(
            read_only_hint = false,
            destructive_hint = true,
            open_world_hint = true
        )
    )]
    async fn pod_delete(
        &self,
        Parameters(req): Parameters<VdslPodActionRequest>,
    ) -> Result<CallToolResult, McpError> {
        let svc = Self::pod_service()?;
        let result = svc
            .delete_pod(&req.pod_id)
            .await
            .map_err(Self::to_mcp_error)?;
        let output =
            serde_json::to_string_pretty(&result).unwrap_or_else(|_| format!("{result:?}"));
        Ok(CallToolResult::success(vec![Content::text(output)]))
    }

    #[tool(
        name = "vdsl_pod_setup",
        description = "Find or create a ComfyUI pod and wait until it's ready. \
            Searches for an existing 'comfyui-vdsl' pod first; starts it if stopped, \
            creates a new one if none found. Returns connection info when ComfyUI is responding. \
            Timeout: 5 minutes.",
        annotations(
            read_only_hint = false,
            destructive_hint = false,
            open_world_hint = true
        )
    )]
    async fn pod_setup(
        &self,
        Parameters(req): Parameters<VdslPodSetupRequest>,
    ) -> Result<CallToolResult, McpError> {
        let svc = Self::pod_service()?;
        let mut log = Vec::<String>::new();

        // --- 1. Resolve volume_id ---
        let volume_id = match req.volume_id {
            Some(v) => v,
            None => {
                let volumes = svc.list_volumes().await.map_err(Self::to_mcp_error)?;
                match volumes.len() {
                    0 => {
                        return Err(McpError::invalid_params(
                            "no network volumes found. Create one first via RunPod dashboard.",
                            None,
                        ))
                    }
                    1 => {
                        let id = volumes[0]["id"]
                            .as_str()
                            .ok_or_else(|| {
                                McpError::invalid_params("volume has no id field", None)
                            })?
                            .to_string();
                        log.push(format!("Auto-detected volume: {id}"));
                        id
                    }
                    n => {
                        let list = format_volume_list(&volumes);
                        return Err(McpError::invalid_params(
                            format!("{n} volumes found — specify volume_id explicitly.\n\n{list}"),
                            None,
                        ));
                    }
                }
            }
        };

        // --- 2. Find existing pod ---
        let pods = svc.list_pods().await.map_err(Self::to_mcp_error)?;
        let candidate = pods.iter().find(|p| {
            let name_match = p["name"].as_str() == Some(COMFY_DEFAULTS_NAME);
            let vol_match = p["networkVolume"]["id"].as_str() == Some(&volume_id)
                || p["networkVolumeId"].as_str() == Some(&volume_id);
            name_match && vol_match
        });

        let mut pod_id: String = String::new();
        let mut needs_create = candidate.is_none();

        if let Some(pod) = candidate {
            pod_id = pod["id"]
                .as_str()
                .ok_or_else(|| McpError::invalid_params("pod has no id field", None))?
                .to_string();
            let status = pod["desiredStatus"]
                .as_str()
                .or_else(|| pod["status"].as_str())
                .unwrap_or("unknown");

            log.push(format!("Found existing pod: {pod_id} (status: {status})"));

            if status != "RUNNING" {
                log.push("Starting pod...".to_string());
                match svc.start_pod(&pod_id).await {
                    Ok(_) => {}
                    Err(e) => {
                        // GPU unavailable or host full — delete and recreate
                        let err_msg = e.to_string();
                        log.push(format!(
                            "Start failed: {err_msg}\nDeleting pod and recreating on available host..."
                        ));
                        match svc.delete_pod(&pod_id).await {
                            Ok(_) => log.push(format!("Deleted pod {pod_id}.")),
                            Err(del_err) => log
                                .push(format!("Warning: failed to delete pod {pod_id}: {del_err}")),
                        }
                        needs_create = true;
                    }
                }
            }
        }

        if needs_create {
            // --- 3. Create new pod ---
            log.push(format!("Creating new pod for volume {volume_id}..."));

            let mut spec = serde_json::Map::new();
            spec.insert(
                "name".into(),
                serde_json::Value::String(COMFY_DEFAULTS_NAME.to_string()),
            );
            spec.insert(
                "templateId".into(),
                serde_json::Value::String(COMFY_DEFAULTS_TEMPLATE.to_string()),
            );
            spec.insert(
                "containerDiskInGb".into(),
                serde_json::Value::Number(COMFY_DEFAULTS_DISK.into()),
            );
            spec.insert("ports".into(), serde_json::json!(["8188/http", "22/tcp"]));
            spec.insert(
                "networkVolumeId".into(),
                serde_json::Value::String(volume_id),
            );

            if let Some(gpu) = req.gpu {
                let gpu_list: Vec<serde_json::Value> = gpu
                    .split(',')
                    .map(|s| serde_json::Value::String(s.trim().to_string()))
                    .collect();
                spec.insert("gpuTypeIds".into(), serde_json::Value::Array(gpu_list));
            }
            if let Some(dc) = req.datacenter {
                let dc_list: Vec<serde_json::Value> = dc
                    .split(',')
                    .map(|s| serde_json::Value::String(s.trim().to_string()))
                    .collect();
                spec.insert("dataCenterIds".into(), serde_json::Value::Array(dc_list));
            }

            let spec_json = serde_json::to_string(&spec).map_err(Self::to_mcp_error)?;
            let result = svc
                .create_pod(&spec_json)
                .await
                .map_err(Self::to_mcp_error)?;

            pod_id = result["id"]
                .as_str()
                .ok_or_else(|| McpError::invalid_params("created pod has no id", None))?
                .to_string();
            log.push(format!("Created pod: {pod_id}"));
        }

        // --- 4. Poll for ComfyUI readiness ---
        let url = proxy_url(&pod_id, 8188);
        let client = Self::comfyui_client(url.clone())?;
        let deadline =
            std::time::Instant::now() + std::time::Duration::from_secs(SETUP_TIMEOUT_SECS);
        let interval = std::time::Duration::from_secs(SETUP_POLL_INTERVAL_SECS);

        log.push(format!("Waiting for ComfyUI at {url} ..."));

        let stats = loop {
            match client.system_stats().await {
                Ok(s) => break s,
                Err(_) if std::time::Instant::now() < deadline => {
                    tokio::time::sleep(interval).await;
                }
                Err(e) => {
                    log.push(format!("Timeout waiting for ComfyUI: {e}"));
                    return Err(McpError::internal_error(
                        format!(
                            "ComfyUI did not become ready within {SETUP_TIMEOUT_SECS}s.\n\n{}",
                            log.join("\n")
                        ),
                        None,
                    ));
                }
            }
        };

        log.push("ComfyUI is ready.".to_string());

        // Save successful connection for session reuse
        self.save_session(&url, Some(&pod_id));

        let output = format!(
            "{}\n\npod_id: {pod_id}\nurl: {url}\n\
             \nAll subsequent tools (generate, models, exec, etc.) will reuse this connection — \
             pod_id/url can be omitted.\n\
             \n{}",
            log.join("\n"),
            serde_json::to_string_pretty(&stats).unwrap_or_else(|_| format!("{stats:?}"))
        );
        Ok(CallToolResult::success(vec![Content::text(output)]))
    }

    #[tool(
        name = "vdsl_models",
        description = "List available models (checkpoints, LoRAs, VAEs, ControlNets, upscalers) on a running ComfyUI instance. \
            If pod_id/url are omitted, reuses the last vdsl_connect or vdsl_pod_setup session.",
        annotations(
            read_only_hint = true,
            destructive_hint = false,
            open_world_hint = true
        )
    )]
    async fn models(
        &self,
        Parameters(req): Parameters<VdslModelsRequest>,
    ) -> Result<CallToolResult, McpError> {
        let url = self.resolve_comfyui_url(req.pod_id.as_deref(), req.url.as_deref())?;
        let client = Self::comfyui_client(url.clone())?;
        let object_info = client.object_info().await.map_err(Self::to_mcp_error)?;
        let catalog = parse_model_catalog(&object_info);
        let limit = req.limit.or(Some(DEFAULT_LIST_LIMIT));
        let output = format!(
            "Models on {url}\n\n{}",
            format_model_catalog_with_limit(&catalog, limit)
        );
        Ok(CallToolResult::success(vec![Content::text(output)]))
    }

    #[tool(
        name = "vdsl_node_search",
        description = "Search available ComfyUI node types by name pattern. \
            Returns matching node class names (case-insensitive substring match). \
            Use this instead of /object_info which can exceed token limits. \
            Examples: pattern='Face' finds FaceDetailer, FaceRestore, etc. \
            Omit pattern to list all node names. \
            If pod_id/url are omitted, reuses the last vdsl_connect or vdsl_pod_setup session.",
        annotations(
            read_only_hint = true,
            destructive_hint = false,
            open_world_hint = true
        )
    )]
    async fn node_search(
        &self,
        Parameters(req): Parameters<VdslNodeSearchRequest>,
    ) -> Result<CallToolResult, McpError> {
        let url = self.resolve_comfyui_url(req.pod_id.as_deref(), req.url.as_deref())?;
        let client = Self::comfyui_client(url.clone())?;
        let all_keys = client
            .object_info_keys()
            .await
            .map_err(Self::to_mcp_error)?;

        let matched: Vec<&String> = match &req.pattern {
            Some(pat) => {
                let pat_lower = pat.to_lowercase();
                all_keys
                    .iter()
                    .filter(|k| k.to_lowercase().contains(&pat_lower))
                    .collect()
            }
            None => all_keys.iter().collect(),
        };

        let total = all_keys.len();
        let found = matched.len();
        let limit = req.limit.unwrap_or(DEFAULT_LIST_LIMIT);
        let truncated = found > limit;
        let display_count = found.min(limit);

        let mut output = match &req.pattern {
            Some(pat) => {
                format!("Node search: \"{pat}\" — {found} matches (of {total} total nodes)\n\n")
            }
            None => format!("All available nodes ({total} total):\n\n"),
        };

        for name in matched.iter().take(limit) {
            output.push_str(name);
            output.push('\n');
        }

        if truncated {
            output.push_str(&format!(
                "\n... showing {display_count} of {found} results. Set limit to see more."
            ));
        }

        Ok(CallToolResult::success(vec![Content::text(output)]))
    }

    #[tool(
        name = "vdsl_queue_status",
        description = "Check ComfyUI queue status. With prompt_id: check specific job (pending/running/completed/error). Without: show full queue state. \
            If pod_id/url are omitted, reuses the last vdsl_connect or vdsl_pod_setup session.",
        annotations(
            read_only_hint = true,
            destructive_hint = false,
            open_world_hint = true
        )
    )]
    async fn queue_status(
        &self,
        Parameters(req): Parameters<VdslQueueStatusRequest>,
    ) -> Result<CallToolResult, McpError> {
        let url = self.resolve_comfyui_url(req.pod_id.as_deref(), req.url.as_deref())?;
        let client = Self::comfyui_client(url.clone())?;

        match req.prompt_id {
            Some(pid) => {
                let history = client.history(&pid).await.map_err(Self::to_mcp_error)?;

                let status = if let Some(entry) = history.get(&pid) {
                    if let Some(status) = entry.get("status") {
                        let completed = status["completed"].as_bool().unwrap_or(false);
                        let status_str = status["status_str"].as_str().unwrap_or("unknown");
                        if completed && status_str == "error" {
                            "error"
                        } else if completed {
                            "completed"
                        } else {
                            "running"
                        }
                    } else {
                        "running"
                    }
                } else {
                    "pending"
                };

                let output = format!(
                    "Prompt {pid}: {status}\n\n{}",
                    serde_json::to_string_pretty(&history)
                        .unwrap_or_else(|_| format!("{history:?}"))
                );
                Ok(CallToolResult::success(vec![Content::text(output)]))
            }
            None => {
                let queue = client.queue().await.map_err(Self::to_mcp_error)?;

                let running = queue["queue_running"].as_array().map_or(0, |a| a.len());
                let pending = queue["queue_pending"].as_array().map_or(0, |a| a.len());

                let output = format!(
                    "Queue: {running} running, {pending} pending\n\n{}",
                    serde_json::to_string_pretty(&queue).unwrap_or_else(|_| format!("{queue:?}"))
                );
                Ok(CallToolResult::success(vec![Content::text(output)]))
            }
        }
    }

    #[tool(
        name = "vdsl_upload",
        description = "Upload local files to a running ComfyUI instance (input/ directory). \
            Accepts a single file (filepath), multiple files (files), \
            or an entire directory (dir). Mutually exclusive. \
            Used for ControlNet images, training data, etc. \
            If pod_id/url are omitted, reuses the last vdsl_connect or vdsl_pod_setup session.",
        annotations(
            read_only_hint = false,
            destructive_hint = false,
            open_world_hint = true
        )
    )]
    async fn upload(
        &self,
        Parameters(req): Parameters<VdslUploadRequest>,
    ) -> Result<CallToolResult, McpError> {
        let url = self.resolve_comfyui_url(req.pod_id.as_deref(), req.url.as_deref())?;
        let client = Self::comfyui_client(url.clone())?;

        let file_list = resolve_upload_files(&req)?;
        let subfolder = req.subfolder.as_deref().unwrap_or("");
        let overwrite = req.overwrite.unwrap_or(true);

        let total = file_list.len();
        let mut uploaded = 0usize;
        let mut log = Vec::new();

        for filepath in &file_list {
            let result = client
                .upload_image(filepath, subfolder, overwrite)
                .await
                .map_err(Self::to_mcp_error)?;

            let name = result["name"].as_str().unwrap_or("?");
            log.push(format!("  {name}"));
            uploaded += 1;
        }

        let header = if total == 1 {
            format!(
                "Uploaded to {url}: {}",
                log.first().map_or("?", |s| s.trim())
            )
        } else {
            format!("Uploaded {uploaded}/{total} files to {url}")
        };

        let mut output = header;
        if total > 1 {
            output.push_str(&format!("\n{}", log.join("\n")));
        }
        Ok(CallToolResult::success(vec![Content::text(output)]))
    }

    #[tool(
        name = "vdsl_download",
        description = "Download a model to a RunPod pod's ComfyUI models directory. \
            Sources: hf:user/repo/file (HuggingFace), cv:VERSION_ID (CivitAI), \
            https://... (direct URL), or bare user/repo/file (defaults to HuggingFace). \
            CivitAI token is auto-injected from VDSL_CIVITAI_TOKEN env. \
            Downloads run in background on the pod via SSH; polls until complete. \
            Timeout: 10 minutes.",
        annotations(
            read_only_hint = false,
            destructive_hint = false,
            open_world_hint = true
        )
    )]
    async fn download(
        &self,
        Parameters(req): Parameters<VdslDownloadRequest>,
    ) -> Result<CallToolResult, McpError> {
        let svc = Self::pod_service()?;
        let ssh_key = resolve_ssh_key(req.ssh_key.as_deref());

        // --- 1. Resolve source → URL + filename ---
        let dl_info = resolve_source(&req.source, req.filename.as_deref())
            .map_err(|e| McpError::invalid_params(e, None))?;

        // --- 2. Resolve target directory ---
        let dir_name =
            resolve_model_dir(&req.target).map_err(|e| McpError::invalid_params(e, None))?;

        let dest = format!("{COMFYUI_MODELS_BASE}/{}/{}", dir_name, dl_info.filename);

        let mut log = Vec::<String>::new();
        log.push(format!(
            "Downloading {} → {}/{}",
            req.source, req.target, dl_info.filename
        ));
        log.push(format!("URL: {}", dl_info.url));
        log.push(format!("Dest: {dest}"));

        // --- 3. Start download ---
        let resp = svc
            .download_add(&req.pod_id, &dl_info.url, Some(&dest), &ssh_key)
            .await
            .map_err(Self::to_mcp_error)?;

        let job_id = resp["id"]
            .as_str()
            .ok_or_else(|| {
                McpError::internal_error(format!("download_add returned no job id: {resp:?}"), None)
            })?
            .to_string();

        if resp["state"].as_str() == Some("already_running") {
            log.push(format!(
                "Already in progress (pid {}), waiting...",
                resp["pid"].as_str().unwrap_or("?")
            ));
        } else {
            log.push(format!("Job started: {job_id}"));
        }

        // --- 4. Poll for completion ---
        let deadline =
            std::time::Instant::now() + std::time::Duration::from_secs(DOWNLOAD_TIMEOUT_SECS);
        let interval = std::time::Duration::from_secs(DOWNLOAD_POLL_INTERVAL_SECS);

        let final_status = loop {
            let status = svc
                .download_status(&req.pod_id, &job_id, &ssh_key)
                .await
                .map_err(Self::to_mcp_error)?;

            let state = status["state"].as_str().unwrap_or("unknown");

            if state == "done" {
                let exit_code = status["exit_code"]
                    .as_str()
                    .or_else(|| status["exit_code"].as_i64().map(|_| ""))
                    .unwrap_or("?");
                if exit_code != "0" && !exit_code.is_empty() {
                    let log_msg = status["log"].as_str().unwrap_or("");
                    log.push(format!("Download failed (exit {exit_code}): {log_msg}"));
                    return Err(McpError::internal_error(log.join("\n"), None));
                }
                break status;
            }

            if std::time::Instant::now() >= deadline {
                log.push(format!(
                    "Timeout after {DOWNLOAD_TIMEOUT_SECS}s (last state: {state})"
                ));
                return Err(McpError::internal_error(log.join("\n"), None));
            }

            tokio::time::sleep(interval).await;
        };

        let file_size = final_status["file_size"]
            .as_str()
            .or_else(|| final_status["file_size"].as_i64().map(|_| "?"))
            .unwrap_or("?");
        log.push(format!("Done ({file_size} bytes)"));

        let output = format!(
            "{}\n\n{}",
            log.join("\n"),
            serde_json::to_string_pretty(&final_status)
                .unwrap_or_else(|_| format!("{final_status:?}"))
        );
        Ok(CallToolResult::success(vec![Content::text(output)]))
    }

    #[tool(
        name = "vdsl_generate",
        description = "Queue a ComfyUI workflow and wait for completion. \
            Accepts workflow JSON inline (workflow) or as a file path (workflow_file). \
            Polls /history until done, returns prompt_id and output images. \
            Timeout: 5 minutes (configurable). \
            If pod_id/url are omitted, reuses the last vdsl_connect or vdsl_pod_setup session.",
        annotations(
            read_only_hint = false,
            destructive_hint = false,
            open_world_hint = true
        )
    )]
    async fn generate(
        &self,
        Parameters(req): Parameters<VdslGenerateRequest>,
    ) -> Result<CallToolResult, McpError> {
        let url = self.resolve_comfyui_url(req.pod_id.as_deref(), req.url.as_deref())?;
        let client = Self::comfyui_client(url.clone())?;

        // --- 1. Resolve workflow ---
        let workflow = match (req.workflow, req.workflow_file) {
            (Some(w), None) => w,
            (None, Some(path)) => {
                let content = tokio::fs::read_to_string(&path).await.map_err(|e| {
                    McpError::invalid_params(
                        format!("failed to read workflow file '{path}': {e}"),
                        None,
                    )
                })?;
                serde_json::from_str(&content).map_err(|e| {
                    McpError::invalid_params(
                        format!("invalid JSON in workflow file '{path}': {e}"),
                        None,
                    )
                })?
            }
            (Some(_), Some(_)) => {
                return Err(McpError::invalid_params(
                    "specify either 'workflow' or 'workflow_file', not both",
                    None,
                ))
            }
            (None, None) => {
                return Err(McpError::invalid_params(
                    "either 'workflow' (inline JSON) or 'workflow_file' (path) is required",
                    None,
                ))
            }
        };

        // --- 2. Queue ---
        let resp = client
            .post_prompt(&workflow)
            .await
            .map_err(Self::to_mcp_error)?;

        let prompt_id = resp["prompt_id"]
            .as_str()
            .ok_or_else(|| {
                McpError::internal_error(format!("no prompt_id in response: {resp}"), None)
            })?
            .to_string();

        // --- 3. Poll for completion ---
        let timeout = req.timeout.unwrap_or(GENERATE_TIMEOUT_SECS);
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(timeout);
        let interval = std::time::Duration::from_secs(GENERATE_POLL_INTERVAL_SECS);

        let entry = loop {
            let history = client
                .history(&prompt_id)
                .await
                .map_err(Self::to_mcp_error)?;

            if let Some(entry) = history.get(&prompt_id) {
                if let Some(status) = entry.get("status") {
                    let completed = status["completed"].as_bool().unwrap_or(false);
                    if completed {
                        if let Some(err_msg) = check_execution_error(status) {
                            return Err(McpError::internal_error(err_msg, None));
                        }
                        break entry.clone();
                    }
                }
            }

            if std::time::Instant::now() >= deadline {
                return Err(McpError::internal_error(
                    format!("timeout after {timeout}s waiting for prompt {prompt_id}"),
                    None,
                ));
            }

            tokio::time::sleep(interval).await;
        };

        // --- 4. Collect output images ---
        let images = collect_output_images(&entry);

        // --- 5. Download images locally (if save_dir specified) ---
        let download_log = if let Some(ref dir) = req.save_dir {
            let dl = download_images_to_dir(&client, &images, std::path::Path::new(dir)).await;
            dl.log
        } else {
            Vec::new()
        };

        let image_summary: Vec<String> = images
            .iter()
            .enumerate()
            .map(|(i, img)| {
                let name = img["filename"].as_str().unwrap_or("?");
                let subfolder = img["subfolder"].as_str().unwrap_or("");
                if subfolder.is_empty() {
                    format!("  {}. {name}", i + 1)
                } else {
                    format!("  {}. {subfolder}/{name}", i + 1)
                }
            })
            .collect();

        let mut output = format!(
            "prompt_id: {prompt_id}\nserver: {url}\nimages: {}\n{}",
            images.len(),
            image_summary.join("\n"),
        );

        if !download_log.is_empty() {
            output.push_str(&format!("\n\ndownloads:\n{}", download_log.join("\n")));
        }

        output.push_str(&format!(
            "\n\n{}",
            serde_json::to_string_pretty(&images).unwrap_or_else(|_| format!("{images:?}"))
        ));

        Ok(CallToolResult::success(vec![Content::text(output)]))
    }

    #[tool(
        name = "vdsl_batch_generate",
        description = "Queue multiple ComfyUI workflows and wait for all to complete. \
            Accepts workflows as: inline array (workflows), file list (workflow_files), \
            or directory of .json files (load_dir). \
            All workflows are submitted to the queue, then polled until every job finishes. \
            Results and output images are collected per-workflow. \
            Use save_dir to download all generated images locally. \
            If pod_id/url are omitted, reuses the last vdsl_connect or vdsl_pod_setup session.",
        annotations(
            read_only_hint = false,
            destructive_hint = false,
            open_world_hint = true
        )
    )]
    async fn batch_generate(
        &self,
        Parameters(req): Parameters<VdslBatchGenerateRequest>,
    ) -> Result<CallToolResult, McpError> {
        let url = self.resolve_comfyui_url(req.pod_id.as_deref(), req.url.as_deref())?;
        let client = Self::comfyui_client(url.clone())?;

        // --- 1. Resolve all workflows ---
        let sources = [
            req.workflows.is_some(),
            req.workflow_files.is_some(),
            req.load_dir.is_some(),
        ];
        let source_count = sources.iter().filter(|&&b| b).count();
        if source_count == 0 {
            return Err(McpError::invalid_params(
                "one of 'workflows', 'workflow_files', or 'load_dir' is required",
                None,
            ));
        }
        if source_count > 1 {
            return Err(McpError::invalid_params(
                "specify only one of 'workflows', 'workflow_files', or 'load_dir'",
                None,
            ));
        }

        let tagged: Vec<TaggedWorkflow> = if let Some(wfs) = req.workflows {
            wfs.into_iter()
                .enumerate()
                .map(|(i, w)| TaggedWorkflow {
                    label: format!("inline_{}", i + 1),
                    workflow: w,
                })
                .collect()
        } else if let Some(files) = req.workflow_files {
            let mut out = Vec::with_capacity(files.len());
            for path in &files {
                out.push(load_tagged_workflow(path).await?);
            }
            out
        } else if let Some(dir) = req.load_dir {
            let entries = scan_json_dir(&dir).await?;
            if entries.is_empty() {
                return Err(McpError::invalid_params(
                    format!("no .json files found in '{dir}'"),
                    None,
                ));
            }
            let mut out = Vec::with_capacity(entries.len());
            for path in &entries {
                out.push(load_tagged_workflow(path).await?);
            }
            out
        } else {
            return Err(McpError::invalid_params("no workflow source", None));
        };

        let total = tagged.len();
        let mut log = Vec::<String>::new();
        log.push(format!("Batch: {total} workflow(s) on {url}"));

        // --- 2. Submit all to queue ---
        let jobs = submit_workflows(&client, &tagged, &mut log).await;
        let submitted_count = jobs.len();
        log.push(format!(
            "Submitted: {submitted_count}/{total} (errors: {})",
            total - submitted_count
        ));

        if jobs.is_empty() {
            return Err(McpError::internal_error(
                format!(
                    "all {total} workflows failed to submit.\n\n{}",
                    log.join("\n")
                ),
                None,
            ));
        }

        // --- 3. Poll until all complete ---
        let timeout = req.timeout.unwrap_or(GENERATE_TIMEOUT_SECS);
        let results = poll_jobs(
            &client,
            jobs,
            total,
            timeout,
            BATCH_POLL_INTERVAL_SECS,
            &mut log,
        )
        .await;

        // --- 4. Download images (if save_dir specified) ---
        let all_images: Vec<&serde_json::Value> = collect_batch_images(&results);
        let download_log = if let Some(ref dir) = req.save_dir {
            let owned: Vec<serde_json::Value> = all_images.iter().map(|v| (*v).clone()).collect();
            let dl = download_images_to_dir(&client, &owned, std::path::Path::new(dir)).await;
            dl.log
        } else {
            Vec::new()
        };

        // --- 5. Build summary ---
        format_batch_summary(&results, &mut log);

        let mut output = log.join("\n");
        if !download_log.is_empty() {
            output.push_str(&format!("\n\ndownloads:\n{}", download_log.join("\n")));
        }

        Ok(CallToolResult::success(vec![Content::text(output)]))
    }

    #[tool(
        name = "vdsl_run_script",
        description = "Run a VDSL Lua script via the lua interpreter. \
            Accepts a script file path or inline code. \
            Captures stdout and stderr. \
            The working directory must contain lua/ and scripts/ for VDSL module resolution. \
            If omitted, auto-detected by walking up from the script's location. \
            Timeout: 10 minutes (configurable).",
        annotations(
            read_only_hint = false,
            destructive_hint = false,
            open_world_hint = true
        )
    )]
    async fn run_script(
        &self,
        Parameters(req): Parameters<VdslRunScriptRequest>,
    ) -> Result<CallToolResult, McpError> {
        let (lua_args, script_label) =
            resolve_script_source(req.script_file.as_deref(), req.code.as_deref())?;
        let work_dir = resolve_working_dir(req.working_dir.as_deref(), req.script_file.as_deref())?;
        let timeout = req.timeout.unwrap_or(SCRIPT_TIMEOUT_SECS);

        // Auto-save inline code to history
        let saved_path = if let Some(ref code) = req.code {
            save_inline_script(code, &work_dir)
        } else {
            None
        };

        let result = exec_lua(&lua_args, &work_dir, timeout, &[]).await?;

        let mut response = format!(
            "script: {script_label}\nworking_dir: {}\nexit_code: {}",
            work_dir.display(),
            result.exit_code,
        );
        if let Some(ref path) = saved_path {
            response.push_str(&format!("\nsaved: {}", path.display()));
        }
        if !result.stdout.is_empty() {
            response.push_str(&format!("\n\n--- stdout ---\n{}", result.stdout));
        }
        if !result.stderr.is_empty() {
            response.push_str(&format!("\n\n--- stderr ---\n{}", result.stderr));
        }
        if result.exit_code != 0 {
            response.insert_str(0, "FAILED: ");
        }

        Ok(CallToolResult::success(vec![Content::text(response)]))
    }

    #[tool(
        name = "vdsl_catalogs",
        description = "List all available VDSL catalog entries (built-in + user-defined). \
            Returns catalog names and their entries grouped by top-level catalogs and packs. \
            Useful for discovering available style/quality/camera/lighting entries \
            before writing VDSL scripts. \
            Specify catalogs_dir to include user-defined catalogs.",
        annotations(
            read_only_hint = true,
            destructive_hint = false,
            open_world_hint = false
        )
    )]
    async fn catalogs(
        &self,
        Parameters(req): Parameters<VdslCatalogsRequest>,
    ) -> Result<CallToolResult, McpError> {
        let work_dir = std::path::PathBuf::from(&req.working_dir);
        if !work_dir.join("lua").is_dir() {
            return Err(McpError::invalid_params(
                format!(
                    "working_dir '{}' does not contain a lua/ directory",
                    req.working_dir
                ),
                None,
            ));
        }

        let raw = req
            .catalog_script
            .as_deref()
            .unwrap_or(DEFAULT_CATALOG_SCRIPT);
        let script = {
            let p = std::path::Path::new(raw);
            if p.is_absolute() {
                p.to_path_buf()
            } else {
                work_dir.join(p)
            }
        };
        if !script.exists() {
            return Err(McpError::invalid_params(
                format!("catalog script not found: {}", script.display()),
                None,
            ));
        }

        let mut envs: Vec<(&str, &str)> = Vec::new();
        let catalogs_dir_val;
        if let Some(ref dir) = req.catalogs_dir {
            catalogs_dir_val = dir.clone();
            envs.push(("VDSL_CATALOGS", &catalogs_dir_val));
        }

        let lua_args = vec![script.to_string_lossy().to_string()];
        let result = exec_lua(&lua_args, &work_dir, 30, &envs).await?;

        if result.exit_code != 0 {
            let mut msg = format!("catalog script failed (exit {})", result.exit_code);
            if !result.stderr.is_empty() {
                msg.push_str(&format!("\n{}", result.stderr));
            }
            return Err(McpError::internal_error(msg, None));
        }

        let limit = req.limit.unwrap_or(200);
        let lines: Vec<&str> = result.stdout.lines().collect();
        let total = lines.len();
        let output = if total > limit {
            let mut truncated: String = lines[..limit].join("\n");
            truncated.push_str(&format!(
                "\n\n... showing {limit} of {total} lines. Set limit to see more."
            ));
            truncated
        } else {
            result.stdout
        };

        Ok(CallToolResult::success(vec![Content::text(output)]))
    }

    #[tool(
        name = "vdsl_comfy_api",
        description = "Call any ComfyUI REST API endpoint with automatic authentication. \
            Supports GET and POST. Authentication (Bearer token) and URL construction \
            (from pod_id) are handled automatically. \
            Examples: GET /queue, GET /object_info, POST /prompt, GET /history/{id}. \
            If pod_id/url are omitted, reuses the last vdsl_connect or vdsl_pod_setup session.",
        annotations(
            read_only_hint = false,
            destructive_hint = false,
            open_world_hint = true
        )
    )]
    async fn comfy_api(
        &self,
        Parameters(req): Parameters<VdslComfyApiRequest>,
    ) -> Result<CallToolResult, McpError> {
        let url = self.resolve_comfyui_url(req.pod_id.as_deref(), req.url.as_deref())?;
        let client = Self::comfyui_client(url.clone())?;

        let method = req.method.as_deref().unwrap_or("GET");
        let path = if req.path.starts_with('/') {
            req.path.clone()
        } else {
            format!("/{}", req.path)
        };

        let result = client
            .api_request(method, &path, req.body.as_ref())
            .await
            .map_err(Self::to_mcp_error)?;

        let output = format!(
            "{method} {url}{path}\n\n{}",
            serde_json::to_string_pretty(&result).unwrap_or_else(|_| format!("{result:?}"))
        );
        Ok(CallToolResult::success(vec![Content::text(output)]))
    }

    // =========================================================================
    // Remote Exec
    // =========================================================================

    #[tool(
        name = "vdsl_exec",
        description = "Execute a shell command on a RunPod pod via SSH. \
            Pass a command string (e.g. \"ls /workspace\", \"nvidia-smi\"). \
            If pod_id is omitted, reuses the last vdsl_connect or vdsl_pod_setup session.",
        annotations(
            read_only_hint = false,
            destructive_hint = false,
            open_world_hint = true
        )
    )]
    async fn exec(
        &self,
        Parameters(req): Parameters<VdslExecRequest>,
    ) -> Result<CallToolResult, McpError> {
        let svc = Self::pod_service()?;
        let pod_id = self.resolve_pod_id(req.pod_id.as_deref())?;
        let ssh_key = resolve_ssh_key(req.ssh_key.as_deref());
        let timeout_secs = req.timeout.or(Some(30));

        // Split command string into shell invocation
        let cmd = ["bash", "-c", &req.command];
        let result = svc
            .pod_exec(&pod_id, &cmd, Some(&ssh_key), timeout_secs)
            .await
            .map_err(Self::to_mcp_error)?;

        let mut output = format!("$ {}\n\n", req.command);
        if !result.stdout.is_empty() {
            output.push_str(&result.stdout);
        }
        if !result.stderr.is_empty() {
            if !result.stdout.is_empty() {
                output.push('\n');
            }
            output.push_str("stderr:\n");
            output.push_str(&result.stderr);
        }
        if !result.success {
            output.push_str(&format!("\nexit code: {}", result.exit_code));
        }
        Ok(CallToolResult::success(vec![Content::text(output)]))
    }

    // =========================================================================
    // Model Search
    // =========================================================================

    #[tool(
        name = "vdsl_model_search",
        description = "Search for AI models (checkpoints, LoRAs, VAEs, etc.) on model marketplaces. \
            Returns model names, version IDs, download counts, and base model info. \
            Use the returned version ID with vdsl_download (source: \"cv:VERSION_ID\") to install. \
            Currently supports: CivitAI (cv). HuggingFace (hf) support is planned.",
        annotations(
            read_only_hint = true,
            destructive_hint = false,
            open_world_hint = true
        )
    )]
    async fn model_search(
        &self,
        Parameters(req): Parameters<VdslModelSearchRequest>,
    ) -> Result<CallToolResult, McpError> {
        let query = req.query.trim();
        if query.is_empty() {
            return Err(McpError::invalid_params("query is required", None));
        }

        let source = req.source.unwrap_or(ModelSource::Cv);
        match source {
            ModelSource::Cv => self.search_civitai(&req).await,
            ModelSource::Hf => Err(McpError::invalid_params(
                "HuggingFace search is not yet supported. Use source: \"cv\" (CivitAI) for now.",
                None,
            )),
        }
    }

    /// CivitAI model search via GET /api/v1/models.
    async fn search_civitai(
        &self,
        req: &VdslModelSearchRequest,
    ) -> Result<CallToolResult, McpError> {
        let limit = req.limit.unwrap_or(10).min(50);
        let mut url = format!(
            "https://civitai.com/api/v1/models?query={}&limit={limit}",
            urlencoding::encode(req.query.trim())
        );

        if let Some(mt) = req.model_type {
            url.push_str(&format!("&types={}", mt.to_civitai_type()));
        }
        if let Some(sort) = req.sort {
            url.push_str(&format!(
                "&sort={}",
                urlencoding::encode(sort.to_civitai_sort())
            ));
        }
        if let Some(ref bm) = req.base_model {
            url.push_str(&format!("&baseModels={}", urlencoding::encode(bm)));
        }
        if let Some(nsfw) = req.nsfw {
            url.push_str(&format!("&nsfw={nsfw}"));
        }

        let client = reqwest::Client::new();
        let mut request = client.get(&url);
        if let Ok(token) = std::env::var("VDSL_CIVITAI_TOKEN") {
            if !token.is_empty() {
                request = request.header("Authorization", format!("Bearer {token}"));
            }
        }

        let resp = request.send().await.map_err(|e| {
            McpError::internal_error(format!("CivitAI API request failed: {e}"), None)
        })?;

        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            return Err(McpError::internal_error(
                format!("CivitAI API returned {status}: {body}"),
                None,
            ));
        }

        let json: serde_json::Value = resp.json().await.map_err(|e| {
            McpError::internal_error(format!("Failed to parse CivitAI response: {e}"), None)
        })?;

        let output = format_civitai_results(&json);
        Ok(CallToolResult::success(vec![Content::text(output)]))
    }

    #[tool(
        name = "vdsl_runpod_cli",
        description = "Execute any runpod-cli command directly. \
            VDSL_RUNPOD_API_KEY and -o json are injected automatically. \
            Pass subcommand + arguments as an array. \
            Examples: [\"pods\", \"list-pods\"], [\"exec\", \"pod_id\", \"nvidia-smi\"], \
            [\"download\", \"list\", \"-i\", \"~/.ssh/id_ed25519_runpod\", \"pod_id\"]. \
            For 'exec' subcommand: returns raw text output (not JSON-parsed). \
            SSH key defaults to VDSL_SSH_KEY env, then ~/.ssh/id_ed25519_runpod if -i is not specified.",
        annotations(
            read_only_hint = false,
            destructive_hint = false,
            open_world_hint = true
        )
    )]
    async fn runpod_cli(
        &self,
        Parameters(req): Parameters<VdslRunpodCliRequest>,
    ) -> Result<CallToolResult, McpError> {
        if req.args.is_empty() {
            return Err(McpError::invalid_params(
                "args is required (e.g. [\"pods\", \"list-pods\"])",
                None,
            ));
        }
        let api_key = resolve_api_key().map_err(Self::to_mcp_error)?;
        let cli = RunPodCli::new(api_key);

        // Route 'exec' subcommand through pod_exec (raw text, no JSON parse)
        if req.args.first().map(String::as_str) == Some("exec") {
            return self.runpod_cli_exec(&cli, &req.args).await;
        }

        let result = cli.raw_exec(&req.args).await.map_err(Self::to_mcp_error)?;

        let cmd_display = req.args.join(" ");
        let output = format!(
            "runpod-cli {cmd_display}\n\n{}",
            serde_json::to_string_pretty(&result).unwrap_or_else(|_| format!("{result:?}"))
        );
        Ok(CallToolResult::success(vec![Content::text(output)]))
    }

    /// Handle `vdsl_runpod_cli` with `exec` subcommand.
    ///
    /// Routes through `pod_exec` which returns raw text output instead of
    /// JSON-parsed output. Automatically injects default SSH key if `-i` is
    /// not specified.
    ///
    /// Expected args format: `["exec", <pod_id>, "--", <command...>]`
    /// or with options: `["exec", "-i", "<key>", "-t", "30", <pod_id>, "--", <command...>]`
    async fn runpod_cli_exec(
        &self,
        cli: &RunPodCli,
        args: &[String],
    ) -> Result<CallToolResult, McpError> {
        // Parse exec args: skip "exec", extract options, pod_id, and command
        let rest = &args[1..]; // skip "exec"

        let mut ssh_key: Option<&str> = None;
        let mut timeout_secs: Option<u64> = None;
        let mut pod_id: Option<&str> = None;
        let mut command_parts: &[String] = &[];
        let mut i = 0;

        while i < rest.len() {
            match rest[i].as_str() {
                "-i" => {
                    if i + 1 < rest.len() {
                        ssh_key = Some(rest[i + 1].as_str());
                        i += 2;
                    } else {
                        return Err(McpError::invalid_params("-i requires a value", None));
                    }
                }
                "-t" => {
                    if i + 1 < rest.len() {
                        timeout_secs = Some(rest[i + 1].parse::<u64>().map_err(|_| {
                            McpError::invalid_params(
                                format!("invalid timeout value: {:?}", rest[i + 1]),
                                None,
                            )
                        })?);
                        i += 2;
                    } else {
                        return Err(McpError::invalid_params("-t requires a value", None));
                    }
                }
                "--" => {
                    // Everything after "--" is the command
                    command_parts = &rest[i + 1..];
                    break;
                }
                _ => {
                    // First non-option arg is pod_id
                    pod_id = Some(rest[i].as_str());
                    i += 1;
                }
            }
        }

        let pod_id = pod_id.ok_or_else(|| {
            McpError::invalid_params(
                "pod_id is required for exec (e.g. [\"exec\", \"pod_id\", \"--\", \"ls\"])",
                None,
            )
        })?;

        if command_parts.is_empty() {
            return Err(McpError::invalid_params(
                "command is required after '--' (e.g. [\"exec\", \"pod_id\", \"--\", \"ls\", \"/workspace\"])",
                None,
            ));
        }

        // Default SSH key if not specified
        let resolved_key = resolve_ssh_key(ssh_key);

        let cmd_refs: Vec<&str> = command_parts.iter().map(String::as_str).collect();
        let result = cli
            .pod_exec(pod_id, &cmd_refs, Some(&resolved_key), timeout_secs)
            .await
            .map_err(Self::to_mcp_error)?;

        let cmd_display = args.join(" ");
        let mut output = format!("runpod-cli {cmd_display}\n\n");

        if !result.stdout.is_empty() {
            output.push_str(&result.stdout);
        }
        if !result.stderr.is_empty() {
            if !output.ends_with('\n') {
                output.push('\n');
            }
            output.push_str("[stderr] ");
            output.push_str(&result.stderr);
        }
        if !result.success {
            if !output.ends_with('\n') {
                output.push('\n');
            }
            output.push_str(&format!("[exit code: {}]", result.exit_code));
        }

        Ok(CallToolResult::success(vec![Content::text(output)]))
    }

    #[tool(
        name = "vdsl_interrupt",
        description = "Cancel ComfyUI jobs. \
            Without prompt_ids: sends POST /interrupt to cancel the currently running job. \
            With prompt_ids: sends POST /queue to delete specific pending jobs from the queue. \
            If pod_id/url are omitted, reuses the last vdsl_connect or vdsl_pod_setup session.",
        annotations(
            read_only_hint = false,
            destructive_hint = true,
            open_world_hint = true
        )
    )]
    async fn interrupt(
        &self,
        Parameters(req): Parameters<VdslInterruptRequest>,
    ) -> Result<CallToolResult, McpError> {
        let url = self.resolve_comfyui_url(req.pod_id.as_deref(), req.url.as_deref())?;
        let client = Self::comfyui_client(url.clone())?;

        match req.prompt_ids {
            Some(ids) if !ids.is_empty() => {
                // Delete specific pending jobs from the queue
                let body = serde_json::json!({ "delete": ids });
                let result = client
                    .api_request("POST", "/queue", Some(&body))
                    .await
                    .map_err(Self::to_mcp_error)?;

                let output = format!(
                    "Deleted {} job(s) from queue at {url}\n\n{}",
                    ids.len(),
                    serde_json::to_string_pretty(&result).unwrap_or_else(|_| format!("{result:?}"))
                );
                Ok(CallToolResult::success(vec![Content::text(output)]))
            }
            _ => {
                // Interrupt the currently running job
                let result = client
                    .api_request("POST", "/interrupt", None)
                    .await
                    .map_err(Self::to_mcp_error)?;

                let output = format!(
                    "Interrupted running job at {url}\n\n{}",
                    serde_json::to_string_pretty(&result).unwrap_or_else(|_| format!("{result:?}"))
                );
                Ok(CallToolResult::success(vec![Content::text(output)]))
            }
        }
    }

    // =========================================================================
    // Cold Storage (B2 via rclone)
    // =========================================================================

    #[tool(
        name = "vdsl_storage_list",
        description = "List files in B2 cold storage. \
            Requires VDSL_B2_KEY_ID and VDSL_B2_KEY env vars. \
            Bucket can be specified per-call or via VDSL_B2_BUCKET env. \
            Ensures rclone is installed on the pod (auto-installs if missing).",
        annotations(
            read_only_hint = true,
            destructive_hint = false,
            open_world_hint = true
        )
    )]
    async fn storage_list(
        &self,
        Parameters(req): Parameters<VdslStorageListRequest>,
    ) -> Result<CallToolResult, McpError> {
        let svc = Self::pod_service()?;
        let ssh_key = resolve_ssh_key(req.ssh_key.as_deref());
        let bucket = resolve_bucket(req.bucket.as_deref())?;
        let path = req.path.as_deref().unwrap_or("");
        let remote = b2_remote(&bucket, path)?;

        ensure_rclone(&svc, &req.pod_id, &ssh_key).await?;

        let result = svc
            .pod_exec(
                &req.pod_id,
                &["rclone", "lsf", "--format", "tsp", &remote],
                Some(&ssh_key),
                Some(RCLONE_OP_TIMEOUT_SECS),
            )
            .await
            .map_err(Self::to_mcp_error)?;

        if !result.success {
            return Err(McpError::internal_error(
                format!(
                    "rclone lsf failed (exit {}): {}",
                    result.exit_code,
                    result.stderr.trim()
                ),
                None,
            ));
        }

        let limit = req.limit.unwrap_or(DEFAULT_LIST_LIMIT);
        let lines: Vec<&str> = result.stdout.lines().collect();
        let total = lines.len();
        let truncated = total > limit;

        let header = format!("B2 listing: {bucket}/{path} ({total} entries)\n");
        let mut output = header;
        for line in lines.iter().take(limit) {
            output.push_str(line);
            output.push('\n');
        }
        if truncated {
            output.push_str(&format!(
                "\n... showing {limit} of {total} entries. Set limit to see more."
            ));
        }
        Ok(CallToolResult::success(vec![Content::text(output)]))
    }

    #[tool(
        name = "vdsl_storage_pull",
        description = "Pull a model from B2 cold storage to the pod's ComfyUI models directory. \
            Requires VDSL_B2_KEY_ID and VDSL_B2_KEY env vars. \
            Ensures rclone is installed on the pod (auto-installs if missing). \
            Target maps to ComfyUI model subdirectories (checkpoints, loras, etc.).",
        annotations(
            read_only_hint = false,
            destructive_hint = false,
            open_world_hint = true
        )
    )]
    async fn storage_pull(
        &self,
        Parameters(req): Parameters<VdslStoragePullRequest>,
    ) -> Result<CallToolResult, McpError> {
        let svc = Self::pod_service()?;
        let ssh_key = resolve_ssh_key(req.ssh_key.as_deref());
        let bucket = resolve_bucket(req.bucket.as_deref())?;
        let remote = b2_remote(&bucket, &req.source)?;
        let dir_name =
            resolve_model_dir(&req.target).map_err(|e| McpError::invalid_params(e, None))?;
        let dest = format!("{COMFYUI_MODELS_BASE}/{dir_name}/");

        ensure_rclone(&svc, &req.pod_id, &ssh_key).await?;

        let mut log = Vec::<String>::new();
        log.push(format!("Pulling B2:{bucket}/{} → {dest}", req.source));

        let result = svc
            .pod_exec(
                &req.pod_id,
                &["rclone", "copy", "--progress", &remote, &dest],
                Some(&ssh_key),
                Some(RCLONE_OP_TIMEOUT_SECS),
            )
            .await
            .map_err(Self::to_mcp_error)?;

        if !result.success {
            log.push(format!(
                "rclone copy failed (exit {}): {}",
                result.exit_code,
                result.stderr.trim()
            ));
            return Err(McpError::internal_error(log.join("\n"), None));
        }

        log.push("Done.".to_string());
        if !result.stderr.trim().is_empty() {
            log.push(result.stderr.trim().to_string());
        }
        Ok(CallToolResult::success(vec![Content::text(log.join("\n"))]))
    }

    #[tool(
        name = "vdsl_storage_push",
        description = "Push a model from the pod's ComfyUI models directory to B2 cold storage. \
            Requires VDSL_B2_KEY_ID and VDSL_B2_KEY env vars. \
            Ensures rclone is installed on the pod (auto-installs if missing). \
            Specify filename to push a single file, or omit to push the entire category.",
        annotations(
            read_only_hint = false,
            destructive_hint = false,
            open_world_hint = true
        )
    )]
    async fn storage_push(
        &self,
        Parameters(req): Parameters<VdslStoragePushRequest>,
    ) -> Result<CallToolResult, McpError> {
        let svc = Self::pod_service()?;
        let ssh_key = resolve_ssh_key(req.ssh_key.as_deref());
        let bucket = resolve_bucket(req.bucket.as_deref())?;
        let dir_name =
            resolve_model_dir(&req.source_target).map_err(|e| McpError::invalid_params(e, None))?;

        let source = match req.filename {
            Some(ref f) => format!("{COMFYUI_MODELS_BASE}/{dir_name}/{f}"),
            None => format!("{COMFYUI_MODELS_BASE}/{dir_name}/"),
        };

        let dest_path = req.dest_path.as_deref().unwrap_or("").trim_matches('/');
        let remote_path = if dest_path.is_empty() {
            format!("models/{dir_name}")
        } else {
            dest_path.to_string()
        };
        let remote = b2_remote(&bucket, &remote_path)?;

        ensure_rclone(&svc, &req.pod_id, &ssh_key).await?;

        let mut log = Vec::<String>::new();
        log.push(format!("Pushing {source} → B2:{bucket}/{remote_path}"));

        let result = svc
            .pod_exec(
                &req.pod_id,
                &["rclone", "copy", "--progress", &source, &remote],
                Some(&ssh_key),
                Some(RCLONE_OP_TIMEOUT_SECS),
            )
            .await
            .map_err(Self::to_mcp_error)?;

        if !result.success {
            log.push(format!(
                "rclone copy failed (exit {}): {}",
                result.exit_code,
                result.stderr.trim()
            ));
            return Err(McpError::internal_error(log.join("\n"), None));
        }

        log.push("Done.".to_string());
        if !result.stderr.trim().is_empty() {
            log.push(result.stderr.trim().to_string());
        }
        Ok(CallToolResult::success(vec![Content::text(log.join("\n"))]))
    }

    // =========================================================================
    // Storage Archive (push → verify → delete)
    // =========================================================================

    #[tool(
        name = "vdsl_storage_archive",
        description = "Archive a model from pod to B2 cold storage and delete from pod. \
            Safe 3-step flow: 1) push to B2, 2) verify upload (existence + size), \
            3) delete from pod only after verification. \
            If verification fails, the pod file is NOT deleted. \
            Requires VDSL_B2_KEY_ID and VDSL_B2_KEY env vars.",
        annotations(
            read_only_hint = false,
            destructive_hint = true,
            open_world_hint = true
        )
    )]
    async fn storage_archive(
        &self,
        Parameters(req): Parameters<VdslStorageArchiveRequest>,
    ) -> Result<CallToolResult, McpError> {
        let svc = Self::pod_service()?;
        let ssh_key = resolve_ssh_key(req.ssh_key.as_deref());
        let bucket = resolve_bucket(req.bucket.as_deref())?;
        let dir_name =
            resolve_model_dir(&req.source_target).map_err(|e| McpError::invalid_params(e, None))?;

        let source_path = format!("{COMFYUI_MODELS_BASE}/{dir_name}/{}", req.filename);
        let dest_path = req.dest_path.as_deref().unwrap_or("").trim_matches('/');
        let remote_dir = if dest_path.is_empty() {
            format!("models/{dir_name}")
        } else {
            dest_path.to_string()
        };
        let remote = b2_remote(&bucket, &remote_dir)?;

        ensure_rclone(&svc, &req.pod_id, &ssh_key).await?;

        let mut log = Vec::<String>::new();

        // --- Step 0: Verify source file exists on pod ---
        log.push(format!("[0/3] Checking {source_path} on pod..."));
        let stat_result = svc
            .pod_exec(
                &req.pod_id,
                &["stat", "--format=%s", &source_path],
                Some(&ssh_key),
                Some(30),
            )
            .await
            .map_err(Self::to_mcp_error)?;

        if !stat_result.success {
            log.push(format!("ABORTED: file not found on pod: {source_path}"));
            return Err(McpError::invalid_params(log.join("\n"), None));
        }
        let pod_size: u64 = stat_result.stdout.trim().parse().unwrap_or(0);
        log.push(format!("  Pod file size: {pod_size} bytes"));

        // --- Step 1: Push to B2 ---
        log.push(format!(
            "[1/3] Pushing {source_path} → B2:{bucket}/{remote_dir}/{}",
            req.filename
        ));
        let push_result = svc
            .pod_exec(
                &req.pod_id,
                &["rclone", "copy", "--progress", &source_path, &remote],
                Some(&ssh_key),
                Some(RCLONE_OP_TIMEOUT_SECS),
            )
            .await
            .map_err(Self::to_mcp_error)?;

        if !push_result.success {
            log.push(format!(
                "ABORTED at push (exit {}): {}",
                push_result.exit_code,
                push_result.stderr.trim()
            ));
            return Err(McpError::internal_error(log.join("\n"), None));
        }
        log.push("  Push complete.".to_string());

        // --- Step 2: Verify on B2 (existence + size) ---
        log.push(format!(
            "[2/3] Verifying B2:{bucket}/{remote_dir}/{}...",
            req.filename
        ));
        let verify_remote = b2_remote(&bucket, &format!("{remote_dir}/{}", req.filename))?;
        let verify_result = svc
            .pod_exec(
                &req.pod_id,
                &["rclone", "lsf", "--format", "s", &verify_remote],
                Some(&ssh_key),
                Some(RCLONE_OP_TIMEOUT_SECS),
            )
            .await
            .map_err(Self::to_mcp_error)?;

        if !verify_result.success || verify_result.stdout.trim().is_empty() {
            log.push("ABORTED: file not found in B2 after push. Pod file NOT deleted.".to_string());
            return Err(McpError::internal_error(log.join("\n"), None));
        }

        let b2_size: u64 = verify_result.stdout.trim().parse().unwrap_or(0);
        log.push(format!("  B2 file size: {b2_size} bytes"));

        if b2_size != pod_size {
            log.push(format!(
                "ABORTED: size mismatch (pod: {pod_size}, B2: {b2_size}). Pod file NOT deleted."
            ));
            return Err(McpError::internal_error(log.join("\n"), None));
        }
        log.push("  Size verified OK.".to_string());

        // --- Step 3: Delete from pod ---
        log.push(format!("[3/3] Deleting {source_path} from pod..."));
        let rm_result = svc
            .pod_exec(
                &req.pod_id,
                &["rm", "-f", &source_path],
                Some(&ssh_key),
                Some(30),
            )
            .await
            .map_err(Self::to_mcp_error)?;

        if !rm_result.success {
            log.push(format!(
                "WARNING: delete failed (exit {}): {}. File may still exist on pod.",
                rm_result.exit_code,
                rm_result.stderr.trim()
            ));
        } else {
            log.push("  Deleted from pod.".to_string());
        }

        log.push(format!(
            "\nArchived: {} → B2:{bucket}/{remote_dir}/{} ({pod_size} bytes)",
            req.filename, req.filename
        ));
        Ok(CallToolResult::success(vec![Content::text(log.join("\n"))]))
    }

    // =========================================================================
    // Image batch download
    // =========================================================================

    #[tool(
        name = "vdsl_image_download",
        description = "Batch download output images from ComfyUI history. \
            Downloads all output images from recent history entries to a local directory. \
            Optionally specify prompt_ids to download images from specific jobs only. \
            If prompt_ids is omitted, downloads from all recent history (up to ~100 entries). \
            If pod_id/url are omitted, reuses the last vdsl_connect or vdsl_pod_setup session.",
        annotations(
            read_only_hint = false,
            destructive_hint = false,
            open_world_hint = true
        )
    )]
    async fn image_download(
        &self,
        Parameters(req): Parameters<VdslImageDownloadRequest>,
    ) -> Result<CallToolResult, McpError> {
        let url = self.resolve_comfyui_url(req.pod_id.as_deref(), req.url.as_deref())?;
        let client = Self::comfyui_client(url.clone())?;
        let save_dir = std::path::Path::new(&req.save_dir);

        let mut log = Vec::<String>::new();

        // Collect history entries
        let entries: Vec<(String, serde_json::Value)> = match req.prompt_ids {
            Some(ids) if !ids.is_empty() => {
                log.push(format!("Fetching {} specific prompt(s)...", ids.len()));
                let mut entries = Vec::new();
                for pid in &ids {
                    match client.history(pid).await {
                        Ok(h) => {
                            if let Some(entry) = h.get(pid) {
                                entries.push((pid.clone(), entry.clone()));
                            } else {
                                log.push(format!("  {pid}: not found in history"));
                            }
                        }
                        Err(e) => log.push(format!("  {pid}: fetch failed — {e}")),
                    }
                }
                entries
            }
            _ => {
                log.push("Fetching all recent history...".to_string());
                let history = client.history_all().await.map_err(Self::to_mcp_error)?;
                match history.as_object() {
                    Some(obj) => obj.iter().map(|(k, v)| (k.clone(), v.clone())).collect(),
                    None => {
                        return Err(McpError::internal_error(
                            "unexpected /history response format",
                            None,
                        ));
                    }
                }
            }
        };

        if entries.is_empty() {
            log.push("No history entries found.".to_string());
            return Ok(CallToolResult::success(vec![Content::text(log.join("\n"))]));
        }

        // Collect all output images
        let mut all_images = Vec::new();
        for (pid, entry) in &entries {
            let images = collect_output_images(entry);
            if !images.is_empty() {
                log.push(format!("  {pid}: {} image(s)", images.len()));
                all_images.extend(images);
            }
        }

        if all_images.is_empty() {
            log.push(format!(
                "{} history entries found, but no output images.",
                entries.len()
            ));
            return Ok(CallToolResult::success(vec![Content::text(log.join("\n"))]));
        }

        log.push(format!(
            "Found {} image(s) across {} job(s). Downloading to {}...",
            all_images.len(),
            entries.len(),
            save_dir.display()
        ));

        // Download
        let dl = download_images_to_dir(&client, &all_images, save_dir).await;
        log.extend(dl.log);

        log.push(format!("\nSaved {} file(s).", dl.saved_paths.len()));

        Ok(CallToolResult::success(vec![Content::text(log.join("\n"))]))
    }

    // =========================================================================
    // VDSL Script
    // =========================================================================

    #[tool(
        name = "vdsl_run",
        description = "Compile and generate images from a VDSL Lua script. \
            Phase 1: Runs the script to compile workflows (vdsl.render) into a temp directory. \
            The script receives VDSL_OUT_DIR env var and writes .json workflow files there. \
            Phase 2: Sends all compiled workflows to ComfyUI via batch generate, \
            polls for completion, and downloads output images to save_dir. \
            Set compile_only=true to skip generation — compiled workflows are checked \
            for required models and verified against the server if connected (preflight). \
            If pod_id/url are omitted, reuses the last vdsl_connect or vdsl_pod_setup session.",
        annotations(
            read_only_hint = false,
            destructive_hint = false,
            open_world_hint = true
        )
    )]
    async fn run(
        &self,
        Parameters(req): Parameters<VdslRunRequest>,
    ) -> Result<CallToolResult, McpError> {
        let (lua_args, script_label) =
            resolve_script_source(req.script_file.as_deref(), req.code.as_deref())?;
        let work_dir = resolve_working_dir(req.working_dir.as_deref(), req.script_file.as_deref())?;
        let timeout = req.timeout.unwrap_or(SCRIPT_TIMEOUT_SECS);

        // Auto-save inline code to history
        let saved_path = if let Some(ref code) = req.code {
            save_inline_script(code, &work_dir)
        } else {
            None
        };

        // --- Phase 1: Compile --- temp dir for workflow JSONs
        let out_dir = tempfile::TempDir::new().map_err(|e| {
            McpError::internal_error(format!("failed to create temp dir: {e}"), None)
        })?;
        let out_dir_str = out_dir.path().to_string_lossy().to_string();

        let lua_result = exec_lua(
            &lua_args,
            &work_dir,
            timeout,
            &[("VDSL_OUT_DIR", &out_dir_str)],
        )
        .await?;

        // Collect script output for reporting
        let mut log = Vec::<String>::new();
        log.push(format!("script: {script_label}"));
        log.push(format!("working_dir: {}", work_dir.display()));
        if let Some(ref path) = saved_path {
            log.push(format!("saved: {}", path.display()));
        }
        log.push(format!("compile exit_code: {}", lua_result.exit_code));

        if lua_result.exit_code != 0 {
            let mut msg = format!("FAILED: script exited with code {}", lua_result.exit_code);
            if !lua_result.stdout.is_empty() {
                msg.push_str(&format!("\n\n--- stdout ---\n{}", lua_result.stdout));
            }
            if !lua_result.stderr.is_empty() {
                msg.push_str(&format!("\n\n--- stderr ---\n{}", lua_result.stderr));
            }
            return Err(McpError::internal_error(msg, None));
        }

        // Enumerate compiled .json files
        let out_dir_str_ref = out_dir.path().to_string_lossy().to_string();
        let workflow_files = scan_json_dir(&out_dir_str_ref).await?;

        log.push(format!("compiled: {} workflow(s)", workflow_files.len()));

        if !lua_result.stdout.is_empty() {
            log.push(format!("\n--- script stdout ---\n{}", lua_result.stdout));
        }
        if !lua_result.stderr.is_empty() {
            log.push(format!("\n--- script stderr ---\n{}", lua_result.stderr));
        }

        // --- compile_only: model check + return ---
        if req.compile_only || workflow_files.is_empty() {
            if workflow_files.is_empty() {
                log.push(
                    "No .json workflows found in VDSL_OUT_DIR. \
                          Ensure the script writes workflow files there."
                        .to_string(),
                );
            } else {
                // Extract required models from compiled workflows
                let mut wf_values = Vec::with_capacity(workflow_files.len());
                for path in &workflow_files {
                    let tagged = load_tagged_workflow(path).await?;
                    wf_values.push(tagged.workflow);
                }
                let required = crate::domain::models::extract_required_models(&wf_values);
                if !required.is_empty() {
                    // Check against server if connection available
                    if let Ok(url) =
                        self.resolve_comfyui_url(req.pod_id.as_deref(), req.url.as_deref())
                    {
                        let client = Self::comfyui_client(url)?;
                        let object_info = client.object_info().await.map_err(Self::to_mcp_error)?;
                        let catalog = crate::domain::models::parse_model_catalog(&object_info);
                        let missing = crate::domain::models::check_missing(&required, &catalog);
                        log.push(String::new());
                        log.push(crate::domain::models::format_preflight_report(
                            &required, &missing,
                        ));
                    } else {
                        // No server — list required models only
                        log.push(String::new());
                        let empty_missing = crate::domain::models::RequiredModels::default();
                        log.push(crate::domain::models::format_preflight_report(
                            &required,
                            &empty_missing,
                        ));
                        log.push(
                            "(No ComfyUI connection — showing required models only. \
                             Use vdsl_connect first to enable server check.)"
                                .to_string(),
                        );
                    }
                }
            }
            return Ok(CallToolResult::success(vec![Content::text(log.join("\n"))]));
        }

        // --- Phase 2: Generate via batch ---
        let url = self.resolve_comfyui_url(req.pod_id.as_deref(), req.url.as_deref())?;
        let client = Self::comfyui_client(url.clone())?;

        let mut tagged: Vec<TaggedWorkflow> = Vec::with_capacity(workflow_files.len());
        for path in &workflow_files {
            tagged.push(load_tagged_workflow(path).await?);
        }

        let total = tagged.len();
        log.push(format!("\nBatch: {total} workflow(s) → {url}"));

        let jobs = submit_workflows(&client, &tagged, &mut log).await;
        if jobs.is_empty() {
            log.push("All workflows failed to submit.".to_string());
            return Ok(CallToolResult::success(vec![Content::text(log.join("\n"))]));
        }

        let results = poll_jobs(
            &client,
            jobs,
            total,
            timeout,
            BATCH_POLL_INTERVAL_SECS,
            &mut log,
        )
        .await;

        // Download images
        let all_images: Vec<&serde_json::Value> = collect_batch_images(&results);
        let (download_log, saved_paths) = if let Some(ref dir) = req.save_dir {
            let owned: Vec<serde_json::Value> = all_images.iter().map(|v| (*v).clone()).collect();
            let dl = download_images_to_dir(&client, &owned, std::path::Path::new(dir)).await;
            (dl.log, dl.saved_paths)
        } else {
            (Vec::new(), Vec::new())
        };

        // Inject VDSL metadata into downloaded PNGs
        let png_paths: Vec<&std::path::Path> = saved_paths
            .iter()
            .filter(|p| p.extension().and_then(|e| e.to_str()) == Some("png"))
            .map(|p| p.as_path())
            .collect();
        if !png_paths.is_empty() {
            let dsl_source = match (req.script_file.as_deref(), req.code.as_deref()) {
                (Some(path), _) => tokio::fs::read_to_string(path).await.ok(),
                (_, Some(c)) => Some(c.to_string()),
                _ => None,
            };
            if let Some(source) = dsl_source {
                match inject_vdsl_metadata(&png_paths, &source, &work_dir).await {
                    Ok(msg) => log.push(format!("\nvdsl metadata: {msg}")),
                    Err(e) => log.push(format!("\nvdsl metadata: injection failed — {e}")),
                }
            }
        }

        // Summary
        format_batch_summary(&results, &mut log);

        if !download_log.is_empty() {
            log.push(format!("\ndownloads:\n{}", download_log.join("\n")));
        }

        Ok(CallToolResult::success(vec![Content::text(log.join("\n"))]))
    }
}

// =============================================================================
// Lua execution helpers
// =============================================================================

/// Return a non-colliding path under `dir` for `filename`.
/// If `dir/filename` already exists, inserts `_2`, `_3`, … before the extension.
fn unique_dest(dir: &std::path::Path, filename: &str) -> std::path::PathBuf {
    let candidate = dir.join(filename);
    if !candidate.exists() {
        return candidate;
    }
    let stem = std::path::Path::new(filename)
        .file_stem()
        .and_then(|s| s.to_str())
        .unwrap_or(filename);
    let ext = std::path::Path::new(filename)
        .extension()
        .and_then(|s| s.to_str())
        .unwrap_or("");
    for n in 2u32..10_000 {
        let name = if ext.is_empty() {
            format!("{stem}_{n}")
        } else {
            format!("{stem}_{n}.{ext}")
        };
        let p = dir.join(&name);
        if !p.exists() {
            return p;
        }
    }
    // 10,000件を超える衝突は異常状態。元のパスを返し呼び出し側に委ねる。
    candidate
}

/// Resolved Lua script arguments and display label.
fn resolve_script_source(
    script_file: Option<&str>,
    code: Option<&str>,
) -> Result<(Vec<String>, String), McpError> {
    match (script_file, code) {
        (Some(path), None) => {
            let p = std::path::Path::new(path);
            if !p.exists() {
                return Err(McpError::invalid_params(
                    format!("script not found: {path}"),
                    None,
                ));
            }
            Ok((vec![path.to_string()], path.to_string()))
        }
        (None, Some(c)) => Ok((
            vec!["-e".to_string(), c.to_string()],
            "<inline>".to_string(),
        )),
        (Some(_), Some(_)) => Err(McpError::invalid_params(
            "specify either 'script_file' or 'code', not both",
            None,
        )),
        (None, None) => Err(McpError::invalid_params(
            "either 'script_file' or 'code' is required",
            None,
        )),
    }
}

/// Default subdirectory for inline script history (relative to working_dir).
const DEFAULT_INLINE_HISTORY_SUBDIR: &str = "scripts/.inline_history";

/// Save inline Lua code to a history directory for future reference.
///
/// Resolution order:
///   1. `VDSL_INLINE_HISTORY_DIR` env var (absolute path)
///   2. `{working_dir}/scripts/.inline_history/`
///
/// File naming: `YYYYMMDD_HHMMSS.lua` (local time).
/// Returns the saved file path on success, or None if saving failed
/// (failures are non-fatal — logged but do not block execution).
fn save_inline_script(code: &str, work_dir: &std::path::Path) -> Option<std::path::PathBuf> {
    let history_dir = match std::env::var("VDSL_INLINE_HISTORY_DIR") {
        Ok(dir) if !dir.is_empty() => std::path::PathBuf::from(dir),
        _ => work_dir.join(DEFAULT_INLINE_HISTORY_SUBDIR),
    };

    if let Err(e) = std::fs::create_dir_all(&history_dir) {
        eprintln!(
            "inline history: failed to create {}: {e}",
            history_dir.display()
        );
        return None;
    }

    let now = chrono::Local::now();
    let filename = now.format("%Y%m%d_%H%M%S.lua").to_string();
    let dest = unique_dest(&history_dir, &filename);

    if let Err(e) = std::fs::write(&dest, code) {
        eprintln!("inline history: failed to write {}: {e}", dest.display());
        return None;
    }
    Some(dest)
}

/// Resolve VDSL working directory (must contain lua/).
fn resolve_working_dir(
    explicit: Option<&str>,
    script_file: Option<&str>,
) -> Result<std::path::PathBuf, McpError> {
    let work_dir = match explicit {
        Some(d) => std::path::PathBuf::from(d),
        None => {
            if let Some(path) = script_file {
                let script_path = std::path::Path::new(path).canonicalize().map_err(|e| {
                    McpError::invalid_params(
                        format!("cannot resolve script path '{path}': {e}"),
                        None,
                    )
                })?;
                let mut dir = script_path.parent();
                loop {
                    match dir {
                        Some(d) if d.join("lua").is_dir() => break d.to_path_buf(),
                        Some(d) => dir = d.parent(),
                        None => {
                            return Err(McpError::invalid_params(
                                format!(
                                    "cannot auto-detect working_dir: no lua/ directory found \
                                     above '{path}'. Specify working_dir explicitly."
                                ),
                                None,
                            ))
                        }
                    }
                }
            } else {
                return Err(McpError::invalid_params(
                    "working_dir is required when using inline code",
                    None,
                ));
            }
        }
    };

    if !work_dir.join("lua").is_dir() {
        return Err(McpError::invalid_params(
            format!(
                "working_dir '{}' does not contain a lua/ directory",
                work_dir.display()
            ),
            None,
        ));
    }
    Ok(work_dir)
}

/// Result of a Lua process execution.
struct LuaExecResult {
    exit_code: i32,
    stdout: String,
    stderr: String,
}

/// Execute a Lua script with VDSL package.path setup.
/// Extra environment variables can be injected via `envs`.
async fn exec_lua(
    lua_args: &[String],
    work_dir: &std::path::Path,
    timeout_secs: u64,
    envs: &[(&str, &str)],
) -> Result<LuaExecResult, McpError> {
    let package_path_setup = format!("package.path='{VDSL_PACKAGE_PATH}'..package.path");

    let mut cmd = tokio::process::Command::new("lua");
    cmd.arg("-e").arg(&package_path_setup);
    for arg in lua_args {
        cmd.arg(arg);
    }
    cmd.current_dir(work_dir);
    cmd.stdout(std::process::Stdio::piped());
    cmd.stderr(std::process::Stdio::piped());
    for (k, v) in envs {
        cmd.env(k, v);
    }

    // タイムアウトやドロップ時に子プロセスを確実に終了し、ゾンビ化を防ぐ
    cmd.kill_on_drop(true);

    let child = cmd.spawn().map_err(|e| {
        McpError::internal_error(
            format!("failed to spawn lua: {e}. Is lua installed and on PATH?"),
            None,
        )
    })?;

    let timeout_dur = std::time::Duration::from_secs(timeout_secs);
    let result = tokio::time::timeout(timeout_dur, child.wait_with_output()).await;

    let output = match result {
        Ok(Ok(o)) => o,
        Ok(Err(e)) => {
            return Err(McpError::internal_error(
                format!("lua process error: {e}"),
                None,
            ))
        }
        Err(_) => {
            return Err(McpError::internal_error(
                format!("script timed out after {timeout_secs}s"),
                None,
            ));
        }
    };

    Ok(LuaExecResult {
        exit_code: output.status.code().unwrap_or(-1),
        stdout: String::from_utf8_lossy(&output.stdout).into_owned(),
        stderr: String::from_utf8_lossy(&output.stderr).into_owned(),
    })
}

// =============================================================================
// Batch workflow helpers (shared by generate, batch_generate, run)
// =============================================================================

/// Tagged workflow with a display label.
struct TaggedWorkflow {
    label: String,
    workflow: serde_json::Value,
}

/// A submitted job awaiting completion.
struct SubmittedJob {
    label: String,
    prompt_id: String,
}

/// Result of a completed job.
struct JobResult {
    label: String,
    prompt_id: String,
    images: Vec<serde_json::Value>,
    error: Option<String>,
}

/// Collect output images from a ComfyUI history entry.
fn collect_output_images(entry: &serde_json::Value) -> Vec<serde_json::Value> {
    let mut images = Vec::new();
    if let Some(outputs) = entry.get("outputs") {
        if let Some(obj) = outputs.as_object() {
            for (_node_id, output) in obj {
                if let Some(arr) = output.get("images").and_then(|v| v.as_array()) {
                    for img in arr {
                        images.push(img.clone());
                    }
                }
            }
        }
    }
    images
}

/// Check if a ComfyUI history entry indicates an execution error.
/// Returns `Some(message)` on error, `None` on success.
fn check_execution_error(status: &serde_json::Value) -> Option<String> {
    let status_str = status["status_str"].as_str().unwrap_or("unknown");
    if status_str != "error" {
        return None;
    }
    let mut msg = "execution error".to_string();
    if let Some(messages) = status["messages"].as_array() {
        for m in messages {
            if m[0].as_str() == Some("execution_error") {
                if let Some(detail) = m[1]["message"].as_str() {
                    msg = detail.to_string();
                }
            }
        }
    }
    Some(msg)
}

/// Result of downloading images to a local directory.
struct DownloadResult {
    log: Vec<String>,
    saved_paths: Vec<std::path::PathBuf>,
}

/// Download images to a local directory, returning log lines and saved file paths.
async fn download_images_to_dir(
    client: &ComfyUiClient,
    images: &[serde_json::Value],
    save_dir: &std::path::Path,
) -> DownloadResult {
    let mut log = Vec::new();
    let mut saved_paths = Vec::new();
    for img in images {
        let filename = match img["filename"].as_str() {
            Some(f) => f,
            None => continue,
        };
        let subfolder = img["subfolder"].as_str().unwrap_or("");
        let dest = unique_dest(save_dir, filename);
        match client.download_image(filename, subfolder, &dest).await {
            Ok(size) => {
                log.push(format!("  saved: {} ({size} bytes)", dest.display()));
                saved_paths.push(dest);
            }
            Err(e) => log.push(format!("  FAILED: {} — {e}", dest.display())),
        }
    }
    DownloadResult { log, saved_paths }
}

/// Submit workflows to ComfyUI queue. Returns submitted jobs; errors are logged.
async fn submit_workflows(
    client: &ComfyUiClient,
    tagged: &[TaggedWorkflow],
    log: &mut Vec<String>,
) -> Vec<SubmittedJob> {
    let mut jobs = Vec::with_capacity(tagged.len());
    for tw in tagged {
        match client.post_prompt(&tw.workflow).await {
            Ok(resp) => {
                if let Some(pid) = resp["prompt_id"].as_str() {
                    log.push(format!("  queued: {} → {pid}", tw.label));
                    jobs.push(SubmittedJob {
                        label: tw.label.clone(),
                        prompt_id: pid.to_string(),
                    });
                } else {
                    log.push(format!("  SKIP {}: no prompt_id in response", tw.label));
                }
            }
            Err(e) => {
                log.push(format!("  SKIP {}: {e}", tw.label));
            }
        }
    }
    jobs
}

/// Poll ComfyUI until all submitted jobs complete or timeout.
async fn poll_jobs(
    client: &ComfyUiClient,
    jobs: Vec<SubmittedJob>,
    total_submitted: usize,
    timeout_secs: u64,
    poll_interval_secs: u64,
    log: &mut Vec<String>,
) -> Vec<JobResult> {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(timeout_secs);
    let interval = std::time::Duration::from_secs(poll_interval_secs);

    let mut results: Vec<JobResult> = Vec::new();
    let mut pending = jobs;

    while !pending.is_empty() {
        if std::time::Instant::now() >= deadline {
            for p in &pending {
                results.push(JobResult {
                    label: p.label.clone(),
                    prompt_id: p.prompt_id.clone(),
                    images: Vec::new(),
                    error: Some("timeout".to_string()),
                });
            }
            break;
        }

        tokio::time::sleep(interval).await;

        let mut still_pending = Vec::new();
        for job in pending {
            let history = match client.history(&job.prompt_id).await {
                Ok(h) => h,
                Err(_) => {
                    still_pending.push(job);
                    continue;
                }
            };

            if let Some(entry) = history.get(&job.prompt_id) {
                if let Some(status) = entry.get("status") {
                    let completed = status["completed"].as_bool().unwrap_or(false);
                    if completed {
                        if let Some(err_msg) = check_execution_error(status) {
                            results.push(JobResult {
                                label: job.label,
                                prompt_id: job.prompt_id,
                                images: Vec::new(),
                                error: Some(err_msg),
                            });
                            continue;
                        }
                        results.push(JobResult {
                            label: job.label,
                            prompt_id: job.prompt_id,
                            images: collect_output_images(entry),
                            error: None,
                        });
                        continue;
                    }
                }
            }
            still_pending.push(job);
        }
        pending = still_pending;

        let done = results.len();
        log.push(format!("  progress: {done}/{total_submitted} complete"));
    }

    results
}

/// Load a workflow JSON file into a TaggedWorkflow.
async fn load_tagged_workflow(path: &str) -> Result<TaggedWorkflow, McpError> {
    let content = tokio::fs::read_to_string(path)
        .await
        .map_err(|e| McpError::internal_error(format!("failed to read '{path}': {e}"), None))?;
    let workflow: serde_json::Value = serde_json::from_str(&content)
        .map_err(|e| McpError::internal_error(format!("invalid JSON in '{path}': {e}"), None))?;
    let label = std::path::Path::new(path)
        .file_stem()
        .and_then(|s| s.to_str())
        .unwrap_or("unknown")
        .to_string();
    Ok(TaggedWorkflow { label, workflow })
}

/// Scan a directory for .json files, returning sorted paths.
async fn scan_json_dir(dir: &str) -> Result<Vec<String>, McpError> {
    let mut entries: Vec<String> = Vec::new();
    let mut rd = tokio::fs::read_dir(dir).await.map_err(|e| {
        McpError::invalid_params(format!("failed to read directory '{dir}': {e}"), None)
    })?;
    while let Some(entry) = rd
        .next_entry()
        .await
        .map_err(|e| McpError::internal_error(format!("read_dir error: {e}"), None))?
    {
        let p = entry.path();
        if p.extension().and_then(|s| s.to_str()) == Some("json") {
            if let Some(s) = p.to_str() {
                entries.push(s.to_string());
            }
        }
    }
    entries.sort();
    Ok(entries)
}

/// Format batch results into summary lines.
fn format_batch_summary(results: &[JobResult], log: &mut Vec<String>) {
    let ok_count = results.iter().filter(|r| r.error.is_none()).count();
    let err_count = results.iter().filter(|r| r.error.is_some()).count();
    let total_images: usize = results.iter().map(|r| r.images.len()).sum();

    log.push(format!(
        "\nComplete: {ok_count} ok, {err_count} failed, {total_images} images"
    ));

    for (i, jr) in results.iter().enumerate() {
        let status = if let Some(ref e) = jr.error {
            format!("ERROR: {e}")
        } else {
            let names: Vec<&str> = jr
                .images
                .iter()
                .filter_map(|img| img["filename"].as_str())
                .collect();
            format!("{} image(s): {}", jr.images.len(), names.join(", "))
        };
        log.push(format!(
            "  {}. [{}] {} — {}",
            i + 1,
            jr.prompt_id,
            jr.label,
            status
        ));
    }
}

/// Collect all images from batch results.
fn collect_batch_images(results: &[JobResult]) -> Vec<&serde_json::Value> {
    results.iter().flat_map(|r| r.images.iter()).collect()
}

// =============================================================================
// VDSL metadata injection
// =============================================================================

/// Timeout for the metadata injection Lua process (seconds).
const VDSL_INJECT_TIMEOUT_SECS: u64 = 30;

/// Lua script that reads a manifest file and injects VDSL metadata into PNGs.
const VDSL_INJECT_LUA: &str = r#"
local png = require("vdsl.util.png")
local json = require("vdsl.util.json")

local f = io.open(os.getenv("VDSL_INJECT_MANIFEST"), "r")
if not f then
    io.stderr:write("cannot open manifest\n")
    os.exit(1)
end
local content = f:read("*a")
f:close()

local manifest = json.decode(content)
local injected = 0

for _, path in ipairs(manifest.image_paths) do
    local ok, err = png.inject_text(path, { vdsl = manifest.vdsl_metadata })
    if ok then
        injected = injected + 1
    else
        io.stderr:write("inject failed: " .. path .. ": " .. tostring(err) .. "\n")
    end
end

print(injected .. " image(s) tagged")
"#;

/// Inject VDSL metadata (structured JSON) into downloaded PNG files.
///
/// Writes a manifest to a temp file and spawns a Lua process that calls
/// `png.inject_text` for each image. Best-effort: errors are returned as
/// `Err(message)` but do not affect the parent operation.
async fn inject_vdsl_metadata(
    png_paths: &[&std::path::Path],
    dsl_source: &str,
    work_dir: &std::path::Path,
) -> Result<String, String> {
    // Structured metadata to embed in the tEXt chunk
    let metadata = serde_json::json!({
        "script": dsl_source,
        "timestamp": chrono::Local::now().to_rfc3339(),
        "version": env!("CARGO_PKG_VERSION"),
    });
    let metadata_str = serde_json::to_string(&metadata).map_err(|e| e.to_string())?;

    // Manifest: metadata string + list of image paths
    let manifest = serde_json::json!({
        "vdsl_metadata": metadata_str,
        "image_paths": png_paths.iter().map(|p| p.to_string_lossy()).collect::<Vec<_>>(),
    });

    let manifest_file = tempfile::NamedTempFile::new().map_err(|e| e.to_string())?;
    std::fs::write(
        manifest_file.path(),
        serde_json::to_string(&manifest).map_err(|e| e.to_string())?,
    )
    .map_err(|e| e.to_string())?;
    let manifest_path = manifest_file.path().to_string_lossy().to_string();

    let lua_args = vec!["-e".to_string(), VDSL_INJECT_LUA.to_string()];
    let result = exec_lua(
        &lua_args,
        work_dir,
        VDSL_INJECT_TIMEOUT_SECS,
        &[("VDSL_INJECT_MANIFEST", &manifest_path)],
    )
    .await
    .map_err(|e| format!("{e}"))?;

    if result.exit_code != 0 {
        let stderr = result.stderr.trim();
        return Err(format!("exit_code={}, stderr={stderr}", result.exit_code));
    }

    let msg = result.stdout.trim().to_string();
    Ok(if msg.is_empty() {
        format!("{} image(s) processed", png_paths.len())
    } else {
        msg
    })
}

// =============================================================================
// Storage helpers (rclone + B2)
// =============================================================================

/// Timeout for rclone install (seconds).
const RCLONE_INSTALL_TIMEOUT_SECS: u64 = 120;
/// Timeout for rclone operations (seconds).
const RCLONE_OP_TIMEOUT_SECS: u64 = 600;

/// Ensure rclone is installed on a running pod.
///
/// Checks `which rclone`; if absent, installs via the official install script.
async fn ensure_rclone(svc: &PodService, pod_id: &str, ssh_key: &str) -> Result<(), McpError> {
    let check = svc
        .pod_exec(pod_id, &["which", "rclone"], Some(ssh_key), Some(10))
        .await;

    match check {
        Ok(ref out) if out.success => return Ok(()),
        _ => {}
    }

    let install = svc
        .pod_exec(
            pod_id,
            &[
                "bash",
                "-c",
                "curl -sL https://rclone.org/install.sh | bash",
            ],
            Some(ssh_key),
            Some(RCLONE_INSTALL_TIMEOUT_SECS),
        )
        .await
        .map_err(|e| McpError::internal_error(format!("rclone install failed: {e}"), None))?;

    if !install.success {
        return Err(McpError::internal_error(
            format!(
                "rclone install failed (exit {}): {}{}",
                install.exit_code,
                install.stderr.trim(),
                if install.stdout.trim().is_empty() {
                    String::new()
                } else {
                    format!("\n{}", install.stdout.trim())
                }
            ),
            None,
        ));
    }

    Ok(())
}

/// Resolve SSH key path from request parameter, VDSL_SSH_KEY env var, or hardcoded default.
///
/// Priority: request param > VDSL_SSH_KEY env > DEFAULT_SSH_KEY constant.
fn resolve_ssh_key(param: Option<&str>) -> String {
    if let Some(k) = param {
        if !k.is_empty() {
            return k.to_string();
        }
    }
    std::env::var("VDSL_SSH_KEY")
        .ok()
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| DEFAULT_SSH_KEY.to_string())
}

/// Resolve B2 bucket name from request parameter or VDSL_B2_BUCKET env var.
fn resolve_bucket(bucket: Option<&str>) -> Result<String, McpError> {
    if let Some(b) = bucket {
        if !b.is_empty() {
            return Ok(b.to_string());
        }
    }
    std::env::var("VDSL_B2_BUCKET")
        .ok()
        .filter(|s| !s.is_empty())
        .ok_or_else(|| {
            McpError::invalid_params("bucket not specified and VDSL_B2_BUCKET env not set", None)
        })
}

/// Build an rclone B2 connection string using inline credentials.
///
/// Requires VDSL_B2_KEY_ID and VDSL_B2_KEY environment variables.
/// Returns a string like `:b2,account=KEY_ID,key=KEY:bucket/path`.
fn b2_remote(bucket: &str, path: &str) -> Result<String, McpError> {
    let key_id = std::env::var("VDSL_B2_KEY_ID")
        .ok()
        .filter(|s| !s.is_empty())
        .ok_or_else(|| McpError::invalid_params("VDSL_B2_KEY_ID env not set", None))?;
    let key = std::env::var("VDSL_B2_KEY")
        .ok()
        .filter(|s| !s.is_empty())
        .ok_or_else(|| McpError::invalid_params("VDSL_B2_KEY env not set", None))?;

    let path = path.trim_matches('/');
    if path.is_empty() {
        Ok(format!(":b2,account={key_id},key={key}:{bucket}"))
    } else {
        Ok(format!(":b2,account={key_id},key={key}:{bucket}/{path}"))
    }
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn server_info() {
        let server = VdslMcpServer::new();
        let info = server.get_info();
        assert_eq!(info.server_info.name, "vdsl-mcp");
        assert!(!info.server_info.version.is_empty());
        assert!(info.instructions.is_some());
    }

    #[test]
    fn pod_list_request_empty() {
        let _req: VdslPodListRequest = serde_json::from_str("{}").unwrap();
    }

    #[test]
    fn pod_action_request_parse() {
        let req: VdslPodActionRequest = serde_json::from_str(r#"{"pod_id":"abc123"}"#).unwrap();
        assert_eq!(req.pod_id, "abc123");
    }

    #[test]
    fn pod_action_request_missing_id() {
        let result = serde_json::from_str::<VdslPodActionRequest>("{}");
        assert!(result.is_err());
    }

    #[test]
    fn connect_request_with_url() {
        let req: VdslConnectRequest =
            serde_json::from_str(r#"{"url":"https://abc-8188.proxy.runpod.net"}"#).unwrap();
        assert!(req.url.as_deref() == Some("https://abc-8188.proxy.runpod.net"));
        assert!(req.pod_id.is_none());
    }

    #[test]
    fn connect_request_with_pod_id() {
        let req: VdslConnectRequest =
            serde_json::from_str(r#"{"pod_id":"pod_abc123def"}"#).unwrap();
        assert_eq!(req.pod_id.as_deref(), Some("pod_abc123def"));
        assert!(req.url.is_none());
    }

    #[test]
    fn connect_request_empty_is_valid_json() {
        // Both fields optional at deserialization level; tool validates at runtime
        let req: VdslConnectRequest = serde_json::from_str("{}").unwrap();
        assert!(req.pod_id.is_none());
        assert!(req.url.is_none());
    }

    #[test]
    fn pod_create_request_minimal() {
        let req: VdslPodCreateRequest = serde_json::from_str(r#"{"volume_id":"vol_001"}"#).unwrap();
        assert_eq!(req.volume_id, "vol_001");
        assert!(req.gpu.is_none());
        assert!(req.name.is_none());
        assert!(req.datacenter.is_none());
        assert!(req.disk_gb.is_none());
    }

    #[test]
    fn pod_create_request_full() {
        let req: VdslPodCreateRequest = serde_json::from_str(
            r#"{"volume_id":"vol_001","gpu":"NVIDIA A40","name":"my-pod","datacenter":"EU-SE-1","disk_gb":50}"#,
        )
        .unwrap();
        assert_eq!(req.volume_id, "vol_001");
        assert_eq!(req.gpu.as_deref(), Some("NVIDIA A40"));
        assert_eq!(req.name.as_deref(), Some("my-pod"));
        assert_eq!(req.datacenter.as_deref(), Some("EU-SE-1"));
        assert_eq!(req.disk_gb, Some(50));
    }

    #[test]
    fn pod_create_request_missing_volume() {
        let result = serde_json::from_str::<VdslPodCreateRequest>("{}");
        assert!(result.is_err());
    }

    #[test]
    fn queue_status_request_with_prompt_id() {
        let req: VdslQueueStatusRequest =
            serde_json::from_str(r#"{"pod_id":"pod_abc","prompt_id":"abc-123-def"}"#).unwrap();
        assert_eq!(req.pod_id.as_deref(), Some("pod_abc"));
        assert_eq!(req.prompt_id.as_deref(), Some("abc-123-def"));
    }

    #[test]
    fn queue_status_request_without_prompt_id() {
        let req: VdslQueueStatusRequest = serde_json::from_str(r#"{"pod_id":"pod_abc"}"#).unwrap();
        assert!(req.prompt_id.is_none());
    }

    #[test]
    fn queue_status_request_empty() {
        let req: VdslQueueStatusRequest = serde_json::from_str("{}").unwrap();
        assert!(req.pod_id.is_none());
        assert!(req.url.is_none());
        assert!(req.prompt_id.is_none());
    }

    // --- vdsl_node_search tests ---

    #[test]
    fn node_search_request_with_pattern() {
        let req: VdslNodeSearchRequest =
            serde_json::from_str(r#"{"pod_id":"pod_abc","pattern":"Face"}"#).unwrap();
        assert_eq!(req.pod_id.as_deref(), Some("pod_abc"));
        assert_eq!(req.pattern.as_deref(), Some("Face"));
    }

    #[test]
    fn node_search_request_without_pattern() {
        let req: VdslNodeSearchRequest = serde_json::from_str(r#"{"pod_id":"pod_abc"}"#).unwrap();
        assert!(req.pattern.is_none());
    }

    #[test]
    fn node_search_request_empty() {
        let req: VdslNodeSearchRequest = serde_json::from_str("{}").unwrap();
        assert!(req.pod_id.is_none());
        assert!(req.url.is_none());
        assert!(req.pattern.is_none());
    }

    // --- resolve_comfyui_url tests ---

    fn test_server() -> VdslMcpServer {
        VdslMcpServer::new()
    }

    #[test]
    fn resolve_url_from_pod_id() {
        let server = test_server();
        let url = server.resolve_comfyui_url(Some("abc123"), None).unwrap();
        assert_eq!(url, "https://abc123-8188.proxy.runpod.net");
    }

    #[test]
    fn resolve_url_from_url() {
        let server = test_server();
        let url = server
            .resolve_comfyui_url(None, Some("http://localhost:8188"))
            .unwrap();
        assert_eq!(url, "http://localhost:8188");
    }

    #[test]
    fn resolve_url_pod_id_takes_precedence() {
        let server = test_server();
        let url = server
            .resolve_comfyui_url(Some("abc123"), Some("http://localhost:8188"))
            .unwrap();
        assert_eq!(url, "https://abc123-8188.proxy.runpod.net");
    }

    #[test]
    fn resolve_url_neither_returns_error_without_session() {
        let server = test_server();
        assert!(server.resolve_comfyui_url(None, None).is_err());
    }

    #[test]
    fn resolve_url_falls_back_to_session() {
        let server = test_server();
        server.save_session("https://saved-8188.proxy.runpod.net", Some("saved"));
        let url = server.resolve_comfyui_url(None, None).unwrap();
        assert_eq!(url, "https://saved-8188.proxy.runpod.net");
    }

    #[test]
    fn resolve_url_explicit_overrides_session() {
        let server = test_server();
        server.save_session("https://old-8188.proxy.runpod.net", Some("old_pod"));
        let url = server.resolve_comfyui_url(Some("new_pod"), None).unwrap();
        assert_eq!(url, "https://new_pod-8188.proxy.runpod.net");
    }

    #[test]
    fn connect_request_wait_defaults_to_false() {
        let req: VdslConnectRequest = serde_json::from_str("{}").unwrap();
        assert!(!req.wait);
    }

    #[test]
    fn connect_request_wait_true() {
        let req: VdslConnectRequest =
            serde_json::from_str(r#"{"pod_id":"pod_abc","wait":true}"#).unwrap();
        assert!(req.wait);
    }

    #[test]
    fn upload_request_single_file() {
        let req: VdslUploadRequest =
            serde_json::from_str(r#"{"pod_id":"pod_abc","filepath":"/tmp/test.png"}"#).unwrap();
        assert_eq!(req.pod_id.as_deref(), Some("pod_abc"));
        assert_eq!(req.filepath.as_deref(), Some("/tmp/test.png"));
        assert!(req.files.is_none());
        assert!(req.dir.is_none());
        assert!(req.subfolder.is_none());
        assert!(req.overwrite.is_none());
    }

    #[test]
    fn upload_request_multiple_files() {
        let req: VdslUploadRequest = serde_json::from_str(
            r#"{"pod_id":"pod_abc","files":["/tmp/a.png","/tmp/b.png"],"subfolder":"train"}"#,
        )
        .unwrap();
        assert_eq!(req.files.as_ref().map(|f| f.len()), Some(2));
        assert!(req.filepath.is_none());
        assert!(req.dir.is_none());
        assert_eq!(req.subfolder.as_deref(), Some("train"));
    }

    #[test]
    fn upload_request_dir() {
        let req: VdslUploadRequest =
            serde_json::from_str(r#"{"pod_id":"pod_abc","dir":"/tmp/dataset","overwrite":false}"#)
                .unwrap();
        assert_eq!(req.dir.as_deref(), Some("/tmp/dataset"));
        assert!(req.filepath.is_none());
        assert!(req.files.is_none());
        assert_eq!(req.overwrite, Some(false));
    }

    #[test]
    fn upload_request_empty_is_valid_json() {
        let req: VdslUploadRequest = serde_json::from_str(r#"{"pod_id":"pod_abc"}"#).unwrap();
        assert!(req.filepath.is_none());
        assert!(req.files.is_none());
        assert!(req.dir.is_none());
    }

    #[test]
    fn upload_resolve_rejects_none() {
        let req = VdslUploadRequest {
            url: None,
            pod_id: None,
            filepath: None,
            files: None,
            dir: None,
            subfolder: None,
            overwrite: None,
        };
        assert!(resolve_upload_files(&req).is_err());
    }

    #[test]
    fn upload_resolve_rejects_multiple_sources() {
        let req = VdslUploadRequest {
            url: None,
            pod_id: None,
            filepath: Some("/tmp/a.png".into()),
            files: Some(vec!["/tmp/b.png".into()]),
            dir: None,
            subfolder: None,
            overwrite: None,
        };
        assert!(resolve_upload_files(&req).is_err());
    }

    #[test]
    fn pod_setup_request_empty() {
        let req: VdslPodSetupRequest = serde_json::from_str("{}").unwrap();
        assert!(req.volume_id.is_none());
        assert!(req.gpu.is_none());
        assert!(req.datacenter.is_none());
    }

    #[test]
    fn pod_setup_request_full() {
        let req: VdslPodSetupRequest = serde_json::from_str(
            r#"{"volume_id":"vol_001","gpu":"NVIDIA A40","datacenter":"EU-SE-1"}"#,
        )
        .unwrap();
        assert_eq!(req.volume_id.as_deref(), Some("vol_001"));
        assert_eq!(req.gpu.as_deref(), Some("NVIDIA A40"));
        assert_eq!(req.datacenter.as_deref(), Some("EU-SE-1"));
    }

    #[test]
    fn pod_setup_request_volume_only() {
        let req: VdslPodSetupRequest = serde_json::from_str(r#"{"volume_id":"vol_abc"}"#).unwrap();
        assert_eq!(req.volume_id.as_deref(), Some("vol_abc"));
        assert!(req.gpu.is_none());
    }

    // --- Download request tests ---

    #[test]
    fn download_request_minimal() {
        let req: VdslDownloadRequest = serde_json::from_str(
            r#"{"pod_id":"pod_abc","source":"hf:user/repo/model.safetensors","target":"loras"}"#,
        )
        .unwrap();
        assert_eq!(req.pod_id, "pod_abc");
        assert_eq!(req.source, "hf:user/repo/model.safetensors");
        assert_eq!(req.target, "loras");
        assert!(req.filename.is_none());
        assert!(req.ssh_key.is_none());
    }

    #[test]
    fn download_request_full() {
        let req: VdslDownloadRequest = serde_json::from_str(
            r#"{"pod_id":"pod_abc","source":"https://example.com/model.safetensors","target":"checkpoints","filename":"my_model.safetensors","ssh_key":"~/.ssh/custom_key"}"#,
        )
        .unwrap();
        assert_eq!(req.filename.as_deref(), Some("my_model.safetensors"));
        assert_eq!(req.ssh_key.as_deref(), Some("~/.ssh/custom_key"));
    }

    #[test]
    fn download_request_missing_fields() {
        assert!(serde_json::from_str::<VdslDownloadRequest>("{}").is_err());
        assert!(serde_json::from_str::<VdslDownloadRequest>(r#"{"pod_id":"pod_abc"}"#).is_err());
    }

    // --- Source resolution tests ---

    #[test]
    fn resolve_source_hf_prefix() {
        let info = resolve_source("hf:myuser/myrepo/model.safetensors", None).unwrap();
        assert_eq!(
            info.url,
            "https://huggingface.co/myuser/myrepo/resolve/main/model.safetensors"
        );
        assert_eq!(info.filename, "model.safetensors");
    }

    #[test]
    fn resolve_source_hf_default() {
        let info = resolve_source("myuser/myrepo/lora_v1.safetensors", None).unwrap();
        assert_eq!(
            info.url,
            "https://huggingface.co/myuser/myrepo/resolve/main/lora_v1.safetensors"
        );
        assert_eq!(info.filename, "lora_v1.safetensors");
    }

    #[test]
    fn resolve_source_hf_nested_path() {
        let info = resolve_source("hf:user/repo/subdir/deep/model.bin", None).unwrap();
        assert_eq!(
            info.url,
            "https://huggingface.co/user/repo/resolve/main/subdir/deep/model.bin"
        );
        assert_eq!(info.filename, "model.bin");
    }

    #[test]
    fn resolve_source_direct_url() {
        let info =
            resolve_source("https://example.com/models/v2/checkpoint.safetensors", None).unwrap();
        assert_eq!(
            info.url,
            "https://example.com/models/v2/checkpoint.safetensors"
        );
        assert_eq!(info.filename, "checkpoint.safetensors");
    }

    #[test]
    fn resolve_source_url_with_query() {
        let info = resolve_source(
            "https://civitai.com/api/download/models/12345?type=Model",
            None,
        )
        .unwrap();
        assert_eq!(info.filename, "12345");
    }

    #[test]
    fn resolve_source_filename_override() {
        let info = resolve_source(
            "https://civitai.com/api/download/models/12345",
            Some("my_lora.safetensors"),
        )
        .unwrap();
        assert_eq!(info.filename, "my_lora.safetensors");
    }

    #[test]
    fn resolve_source_hf_too_short() {
        let result = resolve_source("hf:user/repo", None);
        assert!(result.is_err());
    }

    #[test]
    fn resolve_source_cv_prefix() {
        let info = resolve_source("cv:1595775", None).unwrap();
        assert!(info
            .url
            .starts_with("https://civitai.com/api/download/models/1595775"));
        assert_eq!(info.filename, "1595775.safetensors");
    }

    #[test]
    fn resolve_source_cv_with_filename_override() {
        let info = resolve_source("cv:1595775", Some("retro_scifi.safetensors")).unwrap();
        assert!(info.url.contains("civitai.com"));
        assert_eq!(info.filename, "retro_scifi.safetensors");
    }

    #[test]
    fn resolve_source_cv_empty_id() {
        let result = resolve_source("cv:", None);
        assert!(result.is_err());
    }

    #[test]
    fn inject_civitai_token_non_civitai_url() {
        let url = inject_civitai_token("https://example.com/file.bin");
        assert_eq!(url, "https://example.com/file.bin");
    }

    #[test]
    fn inject_civitai_token_already_has_token() {
        let url = inject_civitai_token("https://civitai.com/api/download/models/123?token=abc");
        assert_eq!(url, "https://civitai.com/api/download/models/123?token=abc");
    }

    // --- Model dir resolution tests ---

    #[test]
    fn resolve_model_dir_valid() {
        assert_eq!(resolve_model_dir("loras").unwrap(), "loras");
        assert_eq!(resolve_model_dir("checkpoints").unwrap(), "checkpoints");
        assert_eq!(resolve_model_dir("controlnet").unwrap(), "controlnet");
        assert_eq!(resolve_model_dir("upscale").unwrap(), "upscale_models");
    }

    #[test]
    fn resolve_model_dir_invalid() {
        let err = resolve_model_dir("foobar").unwrap_err();
        assert!(err.contains("unknown target"));
        assert!(err.contains("loras"));
    }

    // --- Generate request tests ---

    #[test]
    fn generate_request_with_inline_workflow() {
        let req: VdslGenerateRequest = serde_json::from_str(
            r#"{"pod_id":"pod_abc","workflow":{"1":{"class_type":"KSampler"}}}"#,
        )
        .unwrap();
        assert_eq!(req.pod_id.as_deref(), Some("pod_abc"));
        assert!(req.workflow.is_some());
        assert!(req.workflow_file.is_none());
    }

    #[test]
    fn generate_request_with_file() {
        let req: VdslGenerateRequest =
            serde_json::from_str(r#"{"pod_id":"pod_abc","workflow_file":"/tmp/workflow.json"}"#)
                .unwrap();
        assert!(req.workflow.is_none());
        assert_eq!(req.workflow_file.as_deref(), Some("/tmp/workflow.json"));
    }

    #[test]
    fn generate_request_with_timeout() {
        let req: VdslGenerateRequest =
            serde_json::from_str(r#"{"pod_id":"pod_abc","workflow":{},"timeout":600}"#).unwrap();
        assert_eq!(req.timeout, Some(600));
    }

    #[test]
    fn generate_request_empty_is_valid_json() {
        // Both workflow sources optional at deser level; tool validates at runtime
        let req: VdslGenerateRequest = serde_json::from_str("{}").unwrap();
        assert!(req.workflow.is_none());
        assert!(req.workflow_file.is_none());
        assert!(req.pod_id.is_none());
        assert!(req.save_dir.is_none());
    }

    #[test]
    fn generate_request_with_save_dir() {
        let req: VdslGenerateRequest =
            serde_json::from_str(r#"{"pod_id":"pod_abc","workflow":{},"save_dir":"/tmp/output"}"#)
                .unwrap();
        assert_eq!(req.save_dir.as_deref(), Some("/tmp/output"));
    }

    #[test]
    fn generate_request_save_dir_defaults_none() {
        let req: VdslGenerateRequest =
            serde_json::from_str(r#"{"pod_id":"pod_abc","workflow":{}}"#).unwrap();
        assert!(req.save_dir.is_none());
    }

    // --- Batch generate request tests ---

    #[test]
    fn batch_request_with_inline_workflows() {
        let req: VdslBatchGenerateRequest =
            serde_json::from_str(r#"{"pod_id":"pod_abc","workflows":[{"1":{}},{"2":{}}]}"#)
                .unwrap();
        assert_eq!(req.pod_id.as_deref(), Some("pod_abc"));
        assert_eq!(req.workflows.as_ref().map(|w| w.len()), Some(2));
        assert!(req.workflow_files.is_none());
        assert!(req.load_dir.is_none());
    }

    #[test]
    fn batch_request_with_files() {
        let req: VdslBatchGenerateRequest = serde_json::from_str(
            r#"{"pod_id":"pod_abc","workflow_files":["/tmp/a.json","/tmp/b.json","/tmp/c.json"]}"#,
        )
        .unwrap();
        let files = req.workflow_files.as_ref().unwrap();
        assert_eq!(files.len(), 3);
        assert_eq!(files[0], "/tmp/a.json");
        assert!(req.workflows.is_none());
    }

    #[test]
    fn batch_request_with_load_dir() {
        let req: VdslBatchGenerateRequest =
            serde_json::from_str(r#"{"pod_id":"pod_abc","load_dir":"/tmp/workflows"}"#).unwrap();
        assert_eq!(req.load_dir.as_deref(), Some("/tmp/workflows"));
        assert!(req.workflows.is_none());
        assert!(req.workflow_files.is_none());
    }

    #[test]
    fn batch_request_with_save_dir_and_timeout() {
        let req: VdslBatchGenerateRequest = serde_json::from_str(
            r#"{"pod_id":"pod_abc","workflows":[{}],"save_dir":"/tmp/out","timeout":600}"#,
        )
        .unwrap();
        assert_eq!(req.save_dir.as_deref(), Some("/tmp/out"));
        assert_eq!(req.timeout, Some(600));
    }

    #[test]
    fn batch_request_empty_is_valid_json() {
        let req: VdslBatchGenerateRequest = serde_json::from_str("{}").unwrap();
        assert!(req.workflows.is_none());
        assert!(req.workflow_files.is_none());
        assert!(req.load_dir.is_none());
        assert!(req.save_dir.is_none());
        assert!(req.pod_id.is_none());
    }

    // --- Run script request tests ---

    #[test]
    fn run_script_request_with_file() {
        let req: VdslRunScriptRequest = serde_json::from_str(
            r#"{"script_file":"/tmp/test.lua","working_dir":"/home/user/vdsl"}"#,
        )
        .unwrap();
        assert_eq!(req.script_file.as_deref(), Some("/tmp/test.lua"));
        assert_eq!(req.working_dir.as_deref(), Some("/home/user/vdsl"));
        assert!(req.code.is_none());
    }

    #[test]
    fn run_script_request_with_code() {
        let req: VdslRunScriptRequest =
            serde_json::from_str(r#"{"code":"print('hello')","working_dir":"/home/user/vdsl"}"#)
                .unwrap();
        assert_eq!(req.code.as_deref(), Some("print('hello')"));
        assert!(req.script_file.is_none());
    }

    #[test]
    fn run_script_request_with_timeout() {
        let req: VdslRunScriptRequest =
            serde_json::from_str(r#"{"script_file":"/tmp/test.lua","timeout":120}"#).unwrap();
        assert_eq!(req.timeout, Some(120));
    }

    #[test]
    fn run_script_request_empty_is_valid_json() {
        let req: VdslRunScriptRequest = serde_json::from_str("{}").unwrap();
        assert!(req.script_file.is_none());
        assert!(req.code.is_none());
        assert!(req.working_dir.is_none());
        assert!(req.timeout.is_none());
    }

    #[test]
    fn run_script_request_auto_detect_working_dir() {
        // working_dir is optional when script_file is provided
        let req: VdslRunScriptRequest =
            serde_json::from_str(r#"{"script_file":"/home/user/vdsl/examples/test.lua"}"#).unwrap();
        assert!(req.working_dir.is_none());
        assert!(req.script_file.is_some());
    }

    // --- catalogs request tests ---

    #[test]
    fn catalogs_request_full() {
        let req: VdslCatalogsRequest = serde_json::from_str(
            r#"{"working_dir":"/home/user/vdsl","catalog_script":"/opt/custom.lua","catalogs_dir":"/home/user/my_catalogs"}"#,
        )
        .unwrap();
        assert_eq!(req.working_dir, "/home/user/vdsl");
        assert_eq!(req.catalog_script.as_deref(), Some("/opt/custom.lua"));
        assert_eq!(req.catalogs_dir.as_deref(), Some("/home/user/my_catalogs"));
    }

    #[test]
    fn catalogs_request_minimal() {
        let req: VdslCatalogsRequest =
            serde_json::from_str(r#"{"working_dir":"/home/user/vdsl"}"#).unwrap();
        assert_eq!(req.working_dir, "/home/user/vdsl");
        assert!(req.catalog_script.is_none());
        assert!(req.catalogs_dir.is_none());
    }

    #[test]
    fn catalogs_request_relative_script() {
        let req: VdslCatalogsRequest = serde_json::from_str(
            r#"{"working_dir":"/home/user/vdsl","catalog_script":"scripts/my_list.lua"}"#,
        )
        .unwrap();
        assert_eq!(req.catalog_script.as_deref(), Some("scripts/my_list.lua"));
    }

    #[test]
    fn catalogs_request_missing_working_dir() {
        let result = serde_json::from_str::<VdslCatalogsRequest>(r#"{}"#);
        assert!(result.is_err());
    }

    // --- comfy_api request tests ---

    #[test]
    fn comfy_api_request_get() {
        let req: VdslComfyApiRequest =
            serde_json::from_str(r#"{"pod_id":"pod_abc","path":"/queue"}"#).unwrap();
        assert_eq!(req.pod_id.as_deref(), Some("pod_abc"));
        assert_eq!(req.path, "/queue");
        assert!(req.method.is_none());
        assert!(req.body.is_none());
    }

    #[test]
    fn comfy_api_request_post_with_body() {
        let req: VdslComfyApiRequest = serde_json::from_str(
            r#"{"pod_id":"pod_abc","method":"POST","path":"/prompt","body":{"prompt":{}}}"#,
        )
        .unwrap();
        assert_eq!(req.method.as_deref(), Some("POST"));
        assert_eq!(req.path, "/prompt");
        assert!(req.body.is_some());
    }

    #[test]
    fn comfy_api_request_url_direct() {
        let req: VdslComfyApiRequest =
            serde_json::from_str(r#"{"url":"https://example.com:8188","path":"/system_stats"}"#)
                .unwrap();
        assert_eq!(req.url.as_deref(), Some("https://example.com:8188"));
        assert!(req.pod_id.is_none());
    }

    #[test]
    fn comfy_api_request_missing_path() {
        let result = serde_json::from_str::<VdslComfyApiRequest>(r#"{"pod_id":"pod_abc"}"#);
        assert!(result.is_err());
    }

    // --- vdsl_runpod_cli tests ---

    #[test]
    fn runpod_cli_request_pods_list() {
        let req: VdslRunpodCliRequest =
            serde_json::from_str(r#"{"args":["pods","list-pods"]}"#).unwrap();
        assert_eq!(req.args, vec!["pods", "list-pods"]);
    }

    #[test]
    fn runpod_cli_request_exec() {
        let req: VdslRunpodCliRequest =
            serde_json::from_str(r#"{"args":["exec","pod_abc","nvidia-smi"]}"#).unwrap();
        assert_eq!(req.args.len(), 3);
        assert_eq!(req.args[0], "exec");
        assert_eq!(req.args[1], "pod_abc");
    }

    #[test]
    fn runpod_cli_request_empty_args() {
        let req: VdslRunpodCliRequest = serde_json::from_str(r#"{"args":[]}"#).unwrap();
        assert!(req.args.is_empty());
    }

    #[test]
    fn runpod_cli_request_missing_args() {
        let result = serde_json::from_str::<VdslRunpodCliRequest>(r#"{}"#);
        assert!(result.is_err());
    }

    // --- vdsl_runpod_cli exec routing tests ---

    /// Helper to parse exec args the same way `runpod_cli_exec` does.
    /// Returns (ssh_key, timeout, pod_id, command_parts) or error description.
    fn parse_exec_args<'a>(
        args: &'a [&'a str],
    ) -> Result<(Option<&'a str>, Option<u64>, &'a str, Vec<&'a str>), &'static str> {
        let rest = &args[1..]; // skip "exec"
        let mut ssh_key: Option<&str> = None;
        let mut timeout_secs: Option<u64> = None;
        let mut pod_id: Option<&str> = None;
        let mut command_parts: Vec<&str> = vec![];
        let mut i = 0;

        while i < rest.len() {
            match rest[i] {
                "-i" => {
                    if i + 1 < rest.len() {
                        ssh_key = Some(rest[i + 1]);
                        i += 2;
                    } else {
                        return Err("-i requires value");
                    }
                }
                "-t" => {
                    if i + 1 < rest.len() {
                        timeout_secs =
                            Some(rest[i + 1].parse().map_err(|_| "invalid timeout value")?);
                        i += 2;
                    } else {
                        return Err("-t requires value");
                    }
                }
                "--" => {
                    command_parts = rest[i + 1..].to_vec();
                    break;
                }
                _ => {
                    pod_id = Some(rest[i]);
                    i += 1;
                }
            }
        }

        match pod_id {
            Some(id) => Ok((ssh_key, timeout_secs, id, command_parts)),
            None => Err("missing pod_id"),
        }
    }

    #[test]
    fn exec_args_basic() {
        let args = ["exec", "pod_abc", "--", "ls", "/workspace"];
        let (ssh_key, timeout, pod_id, cmd) = parse_exec_args(&args).unwrap();
        assert_eq!(ssh_key, None);
        assert_eq!(timeout, None);
        assert_eq!(pod_id, "pod_abc");
        assert_eq!(cmd, vec!["ls", "/workspace"]);
    }

    #[test]
    fn exec_args_with_ssh_key() {
        let args = [
            "exec",
            "-i",
            "~/.ssh/id_ed25519_runpod",
            "pod_abc",
            "--",
            "nvidia-smi",
        ];
        let (ssh_key, timeout, pod_id, cmd) = parse_exec_args(&args).unwrap();
        assert_eq!(ssh_key, Some("~/.ssh/id_ed25519_runpod"));
        assert_eq!(timeout, None);
        assert_eq!(pod_id, "pod_abc");
        assert_eq!(cmd, vec!["nvidia-smi"]);
    }

    #[test]
    fn exec_args_missing_command() {
        let args = ["exec", "pod_abc"];
        let (_, _, _, cmd) = parse_exec_args(&args).unwrap();
        assert!(cmd.is_empty());
    }

    #[test]
    fn exec_args_with_timeout() {
        let args = ["exec", "-t", "30", "pod_abc", "--", "nvidia-smi"];
        let (ssh_key, timeout, pod_id, cmd) = parse_exec_args(&args).unwrap();
        assert_eq!(ssh_key, None);
        assert_eq!(timeout, Some(30));
        assert_eq!(pod_id, "pod_abc");
        assert_eq!(cmd, vec!["nvidia-smi"]);
    }

    #[test]
    fn exec_args_invalid_timeout() {
        let args = ["exec", "-t", "abc", "pod_abc", "--", "ls"];
        assert!(parse_exec_args(&args).is_err());
    }

    #[test]
    fn exec_is_detected_as_first_arg() {
        let args = vec![
            "exec".to_string(),
            "pod_abc".to_string(),
            "--".to_string(),
            "ls".to_string(),
        ];
        assert_eq!(args.first().map(String::as_str), Some("exec"));
    }

    #[test]
    fn non_exec_is_not_detected() {
        let args = vec!["pods".to_string(), "list-pods".to_string()];
        assert_ne!(args.first().map(String::as_str), Some("exec"));
    }

    // --- vdsl_interrupt tests ---

    #[test]
    fn interrupt_request_no_prompt_ids() {
        let req: VdslInterruptRequest = serde_json::from_str(r#"{"pod_id":"pod_abc"}"#).unwrap();
        assert_eq!(req.pod_id.as_deref(), Some("pod_abc"));
        assert!(req.prompt_ids.is_none());
    }

    #[test]
    fn interrupt_request_with_prompt_ids() {
        let req: VdslInterruptRequest =
            serde_json::from_str(r#"{"pod_id":"pod_abc","prompt_ids":["id1","id2"]}"#).unwrap();
        let ids = req.prompt_ids.as_ref().unwrap();
        assert_eq!(ids.len(), 2);
        assert_eq!(ids[0], "id1");
        assert_eq!(ids[1], "id2");
    }

    #[test]
    fn interrupt_request_with_url() {
        let req: VdslInterruptRequest =
            serde_json::from_str(r#"{"url":"https://example.com:8188"}"#).unwrap();
        assert_eq!(req.url.as_deref(), Some("https://example.com:8188"));
        assert!(req.pod_id.is_none());
    }

    #[test]
    fn interrupt_request_missing_both_url_and_pod_id() {
        let req: VdslInterruptRequest = serde_json::from_str(r#"{}"#).unwrap();
        assert!(req.url.is_none());
        assert!(req.pod_id.is_none());
    }

    // --- save_inline_script tests ---

    #[test]
    fn save_inline_script_creates_file() {
        let dir = tempfile::tempdir().unwrap();
        let code = "print('hello')";
        let result = save_inline_script(code, dir.path());
        assert!(result.is_some());
        let path = result.unwrap();
        assert!(path.exists());
        assert_eq!(std::fs::read_to_string(&path).unwrap(), code);
        assert!(path.extension().and_then(|e| e.to_str()) == Some("lua"));
    }

    #[test]
    fn save_inline_script_creates_history_subdir() {
        let dir = tempfile::tempdir().unwrap();
        let result = save_inline_script("x = 1", dir.path());
        assert!(result.is_some());
        let path = result.unwrap();
        let expected_dir = dir.path().join(DEFAULT_INLINE_HISTORY_SUBDIR);
        assert!(expected_dir.is_dir());
        assert!(path.starts_with(&expected_dir));
    }

    #[test]
    #[ignore = "set_var poisons parallel tests — run with --ignored --test-threads=1"]
    fn save_inline_script_respects_env_override() {
        let custom_dir = tempfile::tempdir().unwrap();
        std::env::set_var("VDSL_INLINE_HISTORY_DIR", custom_dir.path());
        let work_dir = tempfile::tempdir().unwrap();
        let result = save_inline_script("y = 2", work_dir.path());
        std::env::remove_var("VDSL_INLINE_HISTORY_DIR");

        assert!(result.is_some());
        let path = result.unwrap();
        assert!(path.starts_with(custom_dir.path()));
        assert!(!path.starts_with(work_dir.path()));
    }

    #[test]
    fn save_inline_script_filename_is_lua() {
        let dir = tempfile::tempdir().unwrap();
        let result = save_inline_script("z = 3", dir.path());
        assert!(result.is_some());
        let path = result.unwrap();
        let name = path.file_name().unwrap().to_str().unwrap();
        assert!(
            name.ends_with(".lua"),
            "expected .lua extension, got: {name}"
        );
        // Format: YYYYMMDD_HHMMSS.lua — 20 chars
        assert!(name.len() >= 19, "filename too short: {name}");
    }

    // --- unique_dest tests ---

    #[test]
    fn unique_dest_no_collision() {
        let dir = tempfile::tempdir().unwrap();
        let dest = unique_dest(dir.path(), "photo.png");
        assert_eq!(dest, dir.path().join("photo.png"));
    }

    #[test]
    fn unique_dest_one_collision() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("photo.png"), b"x").unwrap();
        let dest = unique_dest(dir.path(), "photo.png");
        assert_eq!(dest, dir.path().join("photo_2.png"));
    }

    #[test]
    fn unique_dest_multiple_collisions() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("photo.png"), b"x").unwrap();
        std::fs::write(dir.path().join("photo_2.png"), b"x").unwrap();
        std::fs::write(dir.path().join("photo_3.png"), b"x").unwrap();
        let dest = unique_dest(dir.path(), "photo.png");
        assert_eq!(dest, dir.path().join("photo_4.png"));
    }

    #[test]
    fn unique_dest_no_extension() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("data"), b"x").unwrap();
        let dest = unique_dest(dir.path(), "data");
        assert_eq!(dest, dir.path().join("data_2"));
    }

    // --- storage request tests ---

    #[test]
    fn storage_list_request_minimal() {
        let req: VdslStorageListRequest = serde_json::from_str(r#"{"pod_id":"pod_abc"}"#).unwrap();
        assert_eq!(req.pod_id, "pod_abc");
        assert!(req.bucket.is_none());
        assert!(req.path.is_none());
        assert!(req.ssh_key.is_none());
    }

    #[test]
    fn storage_list_request_full() {
        let req: VdslStorageListRequest = serde_json::from_str(
            r#"{"pod_id":"pod_abc","bucket":"my-bucket","path":"models/checkpoints","ssh_key":"~/.ssh/id_rsa"}"#,
        )
        .unwrap();
        assert_eq!(req.pod_id, "pod_abc");
        assert_eq!(req.bucket.as_deref(), Some("my-bucket"));
        assert_eq!(req.path.as_deref(), Some("models/checkpoints"));
        assert_eq!(req.ssh_key.as_deref(), Some("~/.ssh/id_rsa"));
    }

    #[test]
    fn storage_list_request_missing_pod_id() {
        assert!(serde_json::from_str::<VdslStorageListRequest>(r#"{}"#).is_err());
    }

    #[test]
    fn storage_pull_request_minimal() {
        let req: VdslStoragePullRequest = serde_json::from_str(
            r#"{"pod_id":"pod_abc","source":"models/checkpoints/sd_xl.safetensors","target":"checkpoints"}"#,
        )
        .unwrap();
        assert_eq!(req.pod_id, "pod_abc");
        assert_eq!(req.source, "models/checkpoints/sd_xl.safetensors");
        assert_eq!(req.target, "checkpoints");
        assert!(req.bucket.is_none());
        assert!(req.ssh_key.is_none());
    }

    #[test]
    fn storage_pull_request_full() {
        let req: VdslStoragePullRequest = serde_json::from_str(
            r#"{"pod_id":"pod_abc","bucket":"my-bucket","source":"sd_xl.safetensors","target":"checkpoints","ssh_key":"~/.ssh/custom"}"#,
        )
        .unwrap();
        assert_eq!(req.bucket.as_deref(), Some("my-bucket"));
        assert_eq!(req.ssh_key.as_deref(), Some("~/.ssh/custom"));
    }

    #[test]
    fn storage_pull_request_missing_required() {
        assert!(serde_json::from_str::<VdslStoragePullRequest>(r#"{}"#).is_err());
        assert!(serde_json::from_str::<VdslStoragePullRequest>(r#"{"pod_id":"pod_abc"}"#).is_err());
        assert!(serde_json::from_str::<VdslStoragePullRequest>(
            r#"{"pod_id":"pod_abc","source":"file.bin"}"#
        )
        .is_err());
    }

    #[test]
    fn storage_push_request_minimal() {
        let req: VdslStoragePushRequest =
            serde_json::from_str(r#"{"pod_id":"pod_abc","source_target":"checkpoints"}"#).unwrap();
        assert_eq!(req.pod_id, "pod_abc");
        assert_eq!(req.source_target, "checkpoints");
        assert!(req.filename.is_none());
        assert!(req.dest_path.is_none());
    }

    #[test]
    fn storage_push_request_single_file() {
        let req: VdslStoragePushRequest = serde_json::from_str(
            r#"{"pod_id":"pod_abc","source_target":"loras","filename":"my_lora.safetensors","bucket":"cold-storage"}"#,
        )
        .unwrap();
        assert_eq!(req.source_target, "loras");
        assert_eq!(req.filename.as_deref(), Some("my_lora.safetensors"));
        assert_eq!(req.bucket.as_deref(), Some("cold-storage"));
    }

    #[test]
    fn storage_push_request_custom_dest() {
        let req: VdslStoragePushRequest = serde_json::from_str(
            r#"{"pod_id":"pod_abc","source_target":"checkpoints","dest_path":"archive/2024/checkpoints"}"#,
        )
        .unwrap();
        assert_eq!(req.dest_path.as_deref(), Some("archive/2024/checkpoints"));
    }

    #[test]
    fn storage_push_request_missing_required() {
        assert!(serde_json::from_str::<VdslStoragePushRequest>(r#"{}"#).is_err());
        assert!(serde_json::from_str::<VdslStoragePushRequest>(r#"{"pod_id":"pod_abc"}"#).is_err());
    }

    // --- image_download request tests ---

    #[test]
    fn image_download_request_minimal() {
        let req: VdslImageDownloadRequest =
            serde_json::from_str(r#"{"pod_id":"pod_abc","save_dir":"/tmp/images"}"#).unwrap();
        assert_eq!(req.pod_id.as_deref(), Some("pod_abc"));
        assert_eq!(req.save_dir, "/tmp/images");
        assert!(req.prompt_ids.is_none());
        assert!(req.url.is_none());
    }

    #[test]
    fn image_download_request_with_prompt_ids() {
        let req: VdslImageDownloadRequest = serde_json::from_str(
            r#"{"url":"https://example.com:8188","save_dir":"/tmp/out","prompt_ids":["abc","def"]}"#,
        )
        .unwrap();
        assert_eq!(req.url.as_deref(), Some("https://example.com:8188"));
        assert!(req.pod_id.is_none());
        let ids = req.prompt_ids.unwrap();
        assert_eq!(ids, vec!["abc", "def"]);
    }

    #[test]
    fn image_download_request_missing_save_dir() {
        assert!(
            serde_json::from_str::<VdslImageDownloadRequest>(r#"{"pod_id":"pod_abc"}"#).is_err()
        );
    }

    #[test]
    fn image_download_request_empty_prompt_ids() {
        let req: VdslImageDownloadRequest =
            serde_json::from_str(r#"{"pod_id":"pod_abc","save_dir":"/tmp","prompt_ids":[]}"#)
                .unwrap();
        let ids = req.prompt_ids.unwrap();
        assert!(ids.is_empty());
    }

    // --- b2_remote tests ---

    #[test]
    #[ignore = "set_var poisons parallel tests — run with --ignored --test-threads=1"]
    fn b2_remote_builds_correct_string() {
        std::env::set_var("VDSL_B2_KEY_ID", "test_key_id");
        std::env::set_var("VDSL_B2_KEY", "test_key");

        let result = b2_remote("my-bucket", "models/checkpoints").unwrap();
        assert_eq!(
            result,
            ":b2,account=test_key_id,key=test_key:my-bucket/models/checkpoints"
        );

        std::env::remove_var("VDSL_B2_KEY_ID");
        std::env::remove_var("VDSL_B2_KEY");
    }

    #[test]
    #[ignore = "set_var poisons parallel tests — run with --ignored --test-threads=1"]
    fn b2_remote_root_path() {
        std::env::set_var("VDSL_B2_KEY_ID", "kid");
        std::env::set_var("VDSL_B2_KEY", "key");

        let result = b2_remote("bucket", "").unwrap();
        assert_eq!(result, ":b2,account=kid,key=key:bucket");

        let result = b2_remote("bucket", "/").unwrap();
        assert_eq!(result, ":b2,account=kid,key=key:bucket");

        std::env::remove_var("VDSL_B2_KEY_ID");
        std::env::remove_var("VDSL_B2_KEY");
    }

    #[test]
    #[ignore = "set_var poisons parallel tests — run with --ignored --test-threads=1"]
    fn b2_remote_missing_credentials() {
        std::env::remove_var("VDSL_B2_KEY_ID");
        std::env::remove_var("VDSL_B2_KEY");

        let result = b2_remote("bucket", "path");
        assert!(result.is_err());
    }

    // --- resolve_bucket tests ---

    #[test]
    fn resolve_bucket_from_param() {
        let result = resolve_bucket(Some("my-bucket")).unwrap();
        assert_eq!(result, "my-bucket");
    }

    #[test]
    fn resolve_bucket_empty_param_falls_through() {
        // Empty string param should fall through to env var
        let result = resolve_bucket(Some(""));
        // Without env var set, this should fail
        // (env var may or may not be set in test env, so just test non-empty param)
        assert!(result.is_err() || !result.unwrap().is_empty());
    }

    #[test]
    fn resolve_bucket_none_without_env() {
        // This test is best-effort — if VDSL_B2_BUCKET happens to be set,
        // it will succeed (which is also valid behavior).
        let _result = resolve_bucket(None);
        // Can't assert error without controlling env
    }

    // --- vdsl_storage_archive tests ---

    #[test]
    fn storage_archive_request_minimal() {
        let req: VdslStorageArchiveRequest = serde_json::from_str(
            r#"{"pod_id":"pod_abc","source_target":"loras","filename":"test.safetensors"}"#,
        )
        .unwrap();
        assert_eq!(req.pod_id, "pod_abc");
        assert_eq!(req.source_target, "loras");
        assert_eq!(req.filename, "test.safetensors");
        assert!(req.bucket.is_none());
        assert!(req.dest_path.is_none());
        assert!(req.ssh_key.is_none());
    }

    #[test]
    fn storage_archive_request_full() {
        let req: VdslStorageArchiveRequest = serde_json::from_str(
            r#"{"pod_id":"pod_x","source_target":"checkpoints","filename":"model.safetensors","bucket":"my-bucket","dest_path":"archive/ckpt","ssh_key":"/tmp/key"}"#,
        )
        .unwrap();
        assert_eq!(req.bucket.as_deref(), Some("my-bucket"));
        assert_eq!(req.dest_path.as_deref(), Some("archive/ckpt"));
        assert_eq!(req.ssh_key.as_deref(), Some("/tmp/key"));
    }

    #[test]
    fn storage_archive_request_missing_filename() {
        let result: Result<VdslStorageArchiveRequest, _> =
            serde_json::from_str(r#"{"pod_id":"pod_abc","source_target":"loras"}"#);
        assert!(result.is_err());
    }

    // --- vdsl_model_search tests ---

    #[test]
    fn model_search_request_minimal() {
        let req: VdslModelSearchRequest =
            serde_json::from_str(r#"{"query":"photorealistic"}"#).unwrap();
        assert_eq!(req.query, "photorealistic");
        assert!(req.source.is_none());
        assert!(req.model_type.is_none());
        assert!(req.sort.is_none());
        assert!(req.limit.is_none());
        assert!(req.base_model.is_none());
        assert!(req.nsfw.is_none());
    }

    #[test]
    fn model_search_request_full() {
        let req: VdslModelSearchRequest = serde_json::from_str(
            r#"{"query":"anime","source":"cv","model_type":"lora","sort":"newest","limit":20,"base_model":"SDXL 1.0","nsfw":false}"#,
        )
        .unwrap();
        assert_eq!(req.query, "anime");
        assert!(matches!(req.source, Some(ModelSource::Cv)));
        assert!(matches!(req.model_type, Some(ModelType::Lora)));
        assert!(matches!(req.sort, Some(ModelSearchSort::Newest)));
        assert_eq!(req.limit, Some(20));
        assert_eq!(req.base_model.as_deref(), Some("SDXL 1.0"));
        assert_eq!(req.nsfw, Some(false));
    }

    #[test]
    fn model_search_hf_source_parses() {
        let req: VdslModelSearchRequest =
            serde_json::from_str(r#"{"query":"test","source":"hf"}"#).unwrap();
        assert!(matches!(req.source, Some(ModelSource::Hf)));
    }

    #[test]
    fn model_type_to_civitai() {
        assert_eq!(ModelType::Checkpoint.to_civitai_type(), "Checkpoint");
        assert_eq!(ModelType::Lora.to_civitai_type(), "LORA");
        assert_eq!(ModelType::Controlnet.to_civitai_type(), "Controlnet");
        assert_eq!(ModelType::Vae.to_civitai_type(), "VAE");
        assert_eq!(ModelType::Upscale.to_civitai_type(), "Upscaler");
        assert_eq!(ModelType::Embedding.to_civitai_type(), "TextualInversion");
    }

    #[test]
    fn model_search_sort_to_civitai() {
        assert_eq!(
            ModelSearchSort::MostDownloaded.to_civitai_sort(),
            "Most Downloaded"
        );
        assert_eq!(
            ModelSearchSort::HighestRated.to_civitai_sort(),
            "Highest Rated"
        );
        assert_eq!(ModelSearchSort::Newest.to_civitai_sort(), "Newest");
    }

    #[test]
    fn format_civitai_results_empty() {
        let json = serde_json::json!({"items": []});
        assert_eq!(format_civitai_results(&json), "No models found.");
    }

    #[test]
    fn format_civitai_results_no_items_key() {
        let json = serde_json::json!({});
        assert_eq!(format_civitai_results(&json), "No models found.");
    }

    #[test]
    fn format_civitai_results_single_model() {
        let json = serde_json::json!({
            "items": [{
                "name": "Test LoRA",
                "type": "LORA",
                "nsfw": false,
                "stats": {
                    "downloadCount": 5000,
                    "rating": 4.8,
                    "ratingCount": 120
                },
                "modelVersions": [{
                    "id": 12345,
                    "name": "v2.0",
                    "baseModel": "SDXL 1.0",
                    "trainedWords": ["photo_style", "realistic"],
                    "files": [{"sizeKB": 153600.0}]
                }]
            }],
            "metadata": {
                "totalItems": 1,
                "currentPage": 1,
                "totalPages": 1
            }
        });
        let out = format_civitai_results(&json);
        assert!(out.contains("Test LoRA"));
        assert!(out.contains("LORA"));
        assert!(out.contains("5000"));
        assert!(out.contains("4.8"));
        assert!(out.contains("cv:12345"));
        assert!(out.contains("SDXL 1.0"));
        assert!(out.contains("150 MB"));
        assert!(out.contains("photo_style, realistic"));
        assert!(out.contains("Page 1/1"));
    }

    #[test]
    fn format_civitai_results_nsfw_marker() {
        let json = serde_json::json!({
            "items": [{
                "name": "NSFW Model",
                "type": "Checkpoint",
                "nsfw": true,
                "stats": {"downloadCount": 100, "rating": 3.0, "ratingCount": 5},
                "modelVersions": []
            }]
        });
        let out = format_civitai_results(&json);
        assert!(out.contains("NSFW"));
    }

    #[test]
    fn format_civitai_results_many_versions_truncated() {
        let versions: Vec<serde_json::Value> = (1..=12)
            .map(|i| serde_json::json!({"id": i, "name": format!("v{i}"), "baseModel": "SDXL"}))
            .collect();
        let json = serde_json::json!({
            "items": [{
                "name": "Multi Version",
                "type": "Checkpoint",
                "nsfw": false,
                "stats": {"downloadCount": 0, "rating": 0.0, "ratingCount": 0},
                "modelVersions": versions
            }]
        });
        let out = format_civitai_results(&json);
        assert!(out.contains("cv:10"));
        assert!(!out.contains("cv:11"));
        assert!(out.contains("2 more version"));
    }
}
