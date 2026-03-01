//! ComfyUI HTTP client.
//!
//! Connects to ComfyUI via RunPod proxy URL and queries API endpoints.

use crate::domain::error::DomainError;

/// ComfyUI HTTP client.
#[derive(Clone)]
pub struct ComfyUiClient {
    base_url: String,
    http: reqwest::Client,
}

impl ComfyUiClient {
    pub fn new(base_url: String) -> Self {
        let http = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(15))
            .build()
            .unwrap_or_else(|_| reqwest::Client::new());
        Self { base_url, http }
    }

    /// Probe /system_stats to verify ComfyUI is responding.
    pub async fn system_stats(&self) -> Result<serde_json::Value, DomainError> {
        let url = format!("{}/system_stats", self.base_url);
        let resp = self.http.get(&url).send().await.map_err(|e| {
            DomainError::ComfyUiConnection(format!("failed to reach {url}: {e}"))
        })?;

        if !resp.status().is_success() {
            return Err(DomainError::ComfyUiConnection(format!(
                "ComfyUI returned HTTP {}",
                resp.status()
            )));
        }

        resp.json().await.map_err(|e| {
            DomainError::ComfyUiConnection(format!("invalid JSON from /system_stats: {e}"))
        })
    }

    /// List available models from /object_info endpoint.
    pub async fn object_info(&self) -> Result<serde_json::Value, DomainError> {
        let url = format!("{}/object_info", self.base_url);
        let resp = self.http.get(&url).send().await.map_err(|e| {
            DomainError::ComfyUiConnection(format!("failed to reach {url}: {e}"))
        })?;

        if !resp.status().is_success() {
            return Err(DomainError::ComfyUiConnection(format!(
                "ComfyUI returned HTTP {}",
                resp.status()
            )));
        }

        resp.json().await.map_err(|e| {
            DomainError::ComfyUiConnection(format!("invalid JSON from /object_info: {e}"))
        })
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
}
