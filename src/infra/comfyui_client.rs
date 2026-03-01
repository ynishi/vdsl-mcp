//! ComfyUI HTTP client.
//!
//! Connects to ComfyUI via RunPod proxy URL and queries API endpoints.
//! Supports optional Bearer token authentication (RunPod proxy auth).

use crate::domain::error::DomainError;

/// ComfyUI HTTP client.
#[derive(Clone)]
pub struct ComfyUiClient {
    base_url: String,
    token: Option<String>,
    http: reqwest::Client,
}

impl ComfyUiClient {
    pub fn new(base_url: String, token: Option<String>) -> Self {
        let http = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(30))
            .build()
            .unwrap_or_else(|_| reqwest::Client::new());
        Self {
            base_url,
            token,
            http,
        }
    }

    /// Build a GET request with optional Bearer auth header.
    fn get_request(&self, url: &str) -> reqwest::RequestBuilder {
        let mut req = self.http.get(url);
        if let Some(token) = &self.token {
            req = req.bearer_auth(token);
        }
        req
    }

    /// GET a JSON endpoint, returning parsed value.
    async fn get_json(&self, path: &str) -> Result<serde_json::Value, DomainError> {
        let url = format!("{}{path}", self.base_url);
        let resp = self.get_request(&url).send().await.map_err(|e| {
            DomainError::ComfyUiConnection(format!("failed to reach {url}: {e}"))
        })?;

        if !resp.status().is_success() {
            return Err(DomainError::ComfyUiConnection(format!(
                "ComfyUI returned HTTP {} for {path}",
                resp.status()
            )));
        }

        resp.json().await.map_err(|e| {
            DomainError::ComfyUiConnection(format!("invalid JSON from {path}: {e}"))
        })
    }

    /// Probe /system_stats to verify ComfyUI is responding.
    pub async fn system_stats(&self) -> Result<serde_json::Value, DomainError> {
        self.get_json("/system_stats").await
    }

    /// List available models from /object_info endpoint.
    pub async fn object_info(&self) -> Result<serde_json::Value, DomainError> {
        self.get_json("/object_info").await
    }

    /// Query job history for a specific prompt.
    ///
    /// Mirrors Lua `Registry:poll()` in `registry.lua` L181-223.
    /// Returns the full `/history/{prompt_id}` response.
    pub async fn history(
        &self,
        prompt_id: &str,
    ) -> Result<serde_json::Value, DomainError> {
        self.get_json(&format!("/history/{prompt_id}")).await
    }

    /// Query the current ComfyUI queue state.
    ///
    /// Returns `{ "queue_running": [...], "queue_pending": [...] }`.
    pub async fn queue(&self) -> Result<serde_json::Value, DomainError> {
        self.get_json("/queue").await
    }

    pub fn base_url(&self) -> &str {
        &self.base_url
    }
}

/// Construct RunPod proxy URL from pod ID.
///
/// Equivalent to Lua `extract_proxy_url()` in runpod.lua L400-410.
pub fn proxy_url(pod_id: &str, port: u16) -> String {
    format!("https://{pod_id}-{port}.proxy.runpod.net")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn proxy_url_default_port() {
        let url = proxy_url("pod_abc123def", 8188);
        assert_eq!(url, "https://pod_abc123def-8188.proxy.runpod.net");
    }

    #[test]
    fn proxy_url_custom_port() {
        let url = proxy_url("abc123", 3000);
        assert_eq!(url, "https://abc123-3000.proxy.runpod.net");
    }

    #[test]
    fn client_without_token() {
        let client = ComfyUiClient::new("http://localhost:8188".into(), None);
        assert_eq!(client.base_url(), "http://localhost:8188");
    }

    #[test]
    fn client_with_token() {
        let client =
            ComfyUiClient::new("http://localhost:8188".into(), Some("mytoken".into()));
        assert_eq!(client.base_url(), "http://localhost:8188");
    }
}
