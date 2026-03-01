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

use crate::application::pod_service::{resolve_api_key, PodService};
use crate::domain::models::{format_model_catalog, parse_model_catalog};
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
}

impl VdslMcpServer {
    fn new() -> Self {
        Self {
            tool_router: Self::tool_router(),
        }
    }

    /// Create a PodService from environment API key.
    fn pod_service() -> Result<PodService, McpError> {
        let api_key = resolve_api_key().map_err(Self::to_mcp_error)?;
        let cli = RunPodCli::new(api_key);
        Ok(PodService::new(cli))
    }

    /// Resolve ComfyUI Bearer token from COMFYUI_TOKEN env var.
    fn comfyui_token() -> Option<String> {
        std::env::var("COMFYUI_TOKEN")
            .ok()
            .filter(|s| !s.is_empty())
    }

    /// Build a ComfyUiClient from URL, with env-based token auth.
    fn comfyui_client(url: String) -> ComfyUiClient {
        ComfyUiClient::new(url, Self::comfyui_token())
    }

    /// Resolve ComfyUI URL from VdslConnectRequest fields.
    fn resolve_comfyui_url(req: &VdslConnectRequest) -> Result<String, McpError> {
        match (req.pod_id.as_deref(), req.url.as_deref()) {
            (Some(id), _) => Ok(proxy_url(id, 8188)),
            (None, Some(u)) => Ok(u.to_string()),
            (None, None) => Err(McpError::invalid_params(
                "either pod_id or url is required",
                None,
            )),
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

    /// Local file path to upload.
    pub filepath: String,

    /// Target subfolder on the ComfyUI server (default: "").
    pub subfolder: Option<String>,

    /// Whether to overwrite existing files (default: true).
    pub overwrite: Option<bool>,
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
        description = "Connect to a ComfyUI instance. Pass a full URL or a RunPod pod ID (proxy URL is auto-constructed). Returns system stats if ComfyUI is reachable.",
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
        let url = Self::resolve_comfyui_url(&req)?;
        let client = Self::comfyui_client(url.clone());
        let stats = client.system_stats().await.map_err(Self::to_mcp_error)?;

        let output = format!(
            "Connected to {url}\n\n{}",
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
                            format!(
                                "{n} volumes found — specify volume_id explicitly.\n\n{list}"
                            ),
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

        let pod_id: String;

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
                svc.start_pod(&pod_id).await.map_err(Self::to_mcp_error)?;
            }
        } else {
            // --- 3. Create new pod ---
            log.push(format!(
                "No existing pod found for volume {volume_id}. Creating..."
            ));

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
            let result = svc.create_pod(&spec_json).await.map_err(Self::to_mcp_error)?;

            pod_id = result["id"]
                .as_str()
                .ok_or_else(|| McpError::invalid_params("created pod has no id", None))?
                .to_string();
            log.push(format!("Created pod: {pod_id}"));
        }

        // --- 4. Poll for ComfyUI readiness ---
        let url = proxy_url(&pod_id, 8188);
        let client = Self::comfyui_client(url.clone());
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

        let output = format!(
            "{}\n\npod_id: {pod_id}\nurl: {url}\n\n{}",
            log.join("\n"),
            serde_json::to_string_pretty(&stats).unwrap_or_else(|_| format!("{stats:?}"))
        );
        Ok(CallToolResult::success(vec![Content::text(output)]))
    }

    #[tool(
        name = "vdsl_models",
        description = "List available models (checkpoints, LoRAs, VAEs, ControlNets, upscalers) on a running ComfyUI instance. Requires a connection URL or pod ID.",
        annotations(
            read_only_hint = true,
            destructive_hint = false,
            open_world_hint = true
        )
    )]
    async fn models(
        &self,
        Parameters(req): Parameters<VdslConnectRequest>,
    ) -> Result<CallToolResult, McpError> {
        let url = Self::resolve_comfyui_url(&req)?;
        let client = Self::comfyui_client(url.clone());
        let object_info = client.object_info().await.map_err(Self::to_mcp_error)?;
        let catalog = parse_model_catalog(&object_info);
        let output = format!("Models on {url}\n\n{}", format_model_catalog(&catalog));
        Ok(CallToolResult::success(vec![Content::text(output)]))
    }

    #[tool(
        name = "vdsl_queue_status",
        description = "Check ComfyUI queue status. With prompt_id: check specific job (pending/running/completed/error). Without: show full queue state.",
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
        let connect_req = VdslConnectRequest {
            url: req.url,
            pod_id: req.pod_id,
        };
        let url = Self::resolve_comfyui_url(&connect_req)?;
        let client = Self::comfyui_client(url.clone());

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
        description = "Upload a local file to a running ComfyUI instance. Used for ControlNet images, training data, etc. Files are uploaded to the input/ directory.",
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
        let connect_req = VdslConnectRequest {
            url: req.url,
            pod_id: req.pod_id,
        };
        let url = Self::resolve_comfyui_url(&connect_req)?;
        let client = Self::comfyui_client(url.clone());

        let filepath = std::path::Path::new(&req.filepath);
        if !filepath.exists() {
            return Err(McpError::invalid_params(
                format!("file not found: {}", filepath.display()),
                None,
            ));
        }

        let subfolder = req.subfolder.as_deref().unwrap_or("");
        let overwrite = req.overwrite.unwrap_or(true);

        let result = client
            .upload_image(filepath, subfolder, overwrite)
            .await
            .map_err(Self::to_mcp_error)?;

        let name = result["name"].as_str().unwrap_or("?");
        let output = format!(
            "Uploaded to {url}: {name}\n\n{}",
            serde_json::to_string_pretty(&result).unwrap_or_else(|_| format!("{result:?}"))
        );
        Ok(CallToolResult::success(vec![Content::text(output)]))
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

    #[test]
    fn resolve_url_from_pod_id() {
        let req = VdslConnectRequest {
            url: None,
            pod_id: Some("abc123".into()),
        };
        let url = VdslMcpServer::resolve_comfyui_url(&req).unwrap();
        assert_eq!(url, "https://abc123-8188.proxy.runpod.net");
    }

    #[test]
    fn resolve_url_from_url() {
        let req = VdslConnectRequest {
            url: Some("http://localhost:8188".into()),
            pod_id: None,
        };
        let url = VdslMcpServer::resolve_comfyui_url(&req).unwrap();
        assert_eq!(url, "http://localhost:8188");
    }

    #[test]
    fn resolve_url_pod_id_takes_precedence() {
        let req = VdslConnectRequest {
            url: Some("http://localhost:8188".into()),
            pod_id: Some("abc123".into()),
        };
        let url = VdslMcpServer::resolve_comfyui_url(&req).unwrap();
        assert_eq!(url, "https://abc123-8188.proxy.runpod.net");
    }

    #[test]
    fn resolve_url_neither_returns_error() {
        let req = VdslConnectRequest {
            url: None,
            pod_id: None,
        };
        assert!(VdslMcpServer::resolve_comfyui_url(&req).is_err());
    }

    #[test]
    fn upload_request_minimal() {
        let req: VdslUploadRequest =
            serde_json::from_str(r#"{"pod_id":"pod_abc","filepath":"/tmp/test.png"}"#).unwrap();
        assert_eq!(req.pod_id.as_deref(), Some("pod_abc"));
        assert_eq!(req.filepath, "/tmp/test.png");
        assert!(req.subfolder.is_none());
        assert!(req.overwrite.is_none());
    }

    #[test]
    fn upload_request_full() {
        let req: VdslUploadRequest = serde_json::from_str(
            r#"{"pod_id":"pod_abc","filepath":"/tmp/test.png","subfolder":"training","overwrite":false}"#,
        )
        .unwrap();
        assert_eq!(req.subfolder.as_deref(), Some("training"));
        assert_eq!(req.overwrite, Some(false));
    }

    #[test]
    fn upload_request_missing_filepath() {
        let result = serde_json::from_str::<VdslUploadRequest>(r#"{"pod_id":"pod_abc"}"#);
        assert!(result.is_err());
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
        let req: VdslPodSetupRequest =
            serde_json::from_str(r#"{"volume_id":"vol_abc"}"#).unwrap();
        assert_eq!(req.volume_id.as_deref(), Some("vol_abc"));
        assert!(req.gpu.is_none());
    }
}
