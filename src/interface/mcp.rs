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
        let result = svc.start_pod(&req.pod_id).await.map_err(Self::to_mcp_error)?;
        let output = serde_json::to_string_pretty(&result)
            .unwrap_or_else(|_| format!("{result:?}"));
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
        let result = svc.stop_pod(&req.pod_id).await.map_err(Self::to_mcp_error)?;
        let output = serde_json::to_string_pretty(&result)
            .unwrap_or_else(|_| format!("{result:?}"));
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
        let url = match (req.pod_id, req.url) {
            (Some(id), _) => proxy_url(&id, 8188),
            (None, Some(u)) => u,
            (None, None) => {
                return Err(McpError::invalid_params(
                    "either pod_id or url is required",
                    None,
                ));
            }
        };

        let client = ComfyUiClient::new(url.clone());
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
        let result = svc.delete_pod(&req.pod_id).await.map_err(Self::to_mcp_error)?;
        let output = serde_json::to_string_pretty(&result)
            .unwrap_or_else(|_| format!("{result:?}"));
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
        let req: VdslPodActionRequest =
            serde_json::from_str(r#"{"pod_id":"abc123"}"#).unwrap();
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
}
