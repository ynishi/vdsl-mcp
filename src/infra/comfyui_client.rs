//! ComfyUI HTTP client.
//!
//! Connects to ComfyUI via RunPod proxy URL and queries API endpoints.
//! Supports optional Bearer token authentication (RunPod proxy auth).

use std::path::Path;

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
        let resp =
            self.get_request(&url).send().await.map_err(|e| {
                DomainError::ComfyUiConnection(format!("failed to reach {url}: {e}"))
            })?;

        if !resp.status().is_success() {
            return Err(DomainError::ComfyUiConnection(format!(
                "ComfyUI returned HTTP {} for {path}",
                resp.status()
            )));
        }

        resp.json()
            .await
            .map_err(|e| DomainError::ComfyUiConnection(format!("invalid JSON from {path}: {e}")))
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
    pub async fn history(&self, prompt_id: &str) -> Result<serde_json::Value, DomainError> {
        self.get_json(&format!("/history/{prompt_id}")).await
    }

    /// Query the current ComfyUI queue state.
    ///
    /// Returns `{ "queue_running": [...], "queue_pending": [...] }`.
    pub async fn queue(&self) -> Result<serde_json::Value, DomainError> {
        self.get_json("/queue").await
    }

    /// POST a workflow to `/prompt` and return the response (contains `prompt_id`).
    ///
    /// Mirrors Lua `Registry:queue()` in `registry.lua` L128-138.
    pub async fn post_prompt(
        &self,
        prompt: &serde_json::Value,
    ) -> Result<serde_json::Value, DomainError> {
        let url = format!("{}/prompt", self.base_url);
        let body = serde_json::json!({ "prompt": prompt });

        let mut req = self.http.post(&url).json(&body);
        if let Some(token) = &self.token {
            req = req.bearer_auth(token);
        }

        let resp = req
            .send()
            .await
            .map_err(|e| DomainError::ComfyUiConnection(format!("failed to POST /prompt: {e}")))?;

        if !resp.status().is_success() {
            let status = resp.status();
            let body_text = resp.text().await.unwrap_or_default();
            return Err(DomainError::ComfyUiConnection(format!(
                "POST /prompt returned HTTP {status}: {body_text}"
            )));
        }

        resp.json().await.map_err(|e| {
            DomainError::ComfyUiConnection(format!("invalid JSON from POST /prompt: {e}"))
        })
    }

    /// Upload a file to ComfyUI via `POST /upload/image` (multipart/form-data).
    ///
    /// Mirrors Lua `Registry:upload()` in `registry.lua` L276-295.
    pub async fn upload_image(
        &self,
        filepath: &Path,
        subfolder: &str,
        overwrite: bool,
    ) -> Result<serde_json::Value, DomainError> {
        let url = format!("{}/upload/image", self.base_url);

        let filename = filepath
            .file_name()
            .and_then(|n| n.to_str())
            .ok_or_else(|| {
                DomainError::ComfyUiConnection(format!("invalid filename: {}", filepath.display()))
            })?
            .to_string();

        let file_bytes = tokio::fs::read(filepath).await.map_err(|e| {
            DomainError::ComfyUiConnection(format!("failed to read {}: {e}", filepath.display()))
        })?;

        let file_part = reqwest::multipart::Part::bytes(file_bytes)
            .file_name(filename)
            .mime_str("application/octet-stream")
            .map_err(|e| DomainError::ComfyUiConnection(format!("mime error: {e}")))?;

        let form = reqwest::multipart::Form::new()
            .part("image", file_part)
            .text("subfolder", subfolder.to_string())
            .text("type", "input")
            .text("overwrite", if overwrite { "true" } else { "false" });

        let mut req = self.http.post(&url).multipart(form);
        if let Some(token) = &self.token {
            req = req.bearer_auth(token);
        }

        let resp = req
            .send()
            .await
            .map_err(|e| DomainError::ComfyUiConnection(format!("upload failed: {e}")))?;

        if !resp.status().is_success() {
            return Err(DomainError::ComfyUiConnection(format!(
                "upload returned HTTP {}",
                resp.status()
            )));
        }

        resp.json()
            .await
            .map_err(|e| DomainError::ComfyUiConnection(format!("invalid JSON from upload: {e}")))
    }

    /// Download an output image from ComfyUI via `GET /view`.
    ///
    /// Fetches `/view?filename=...&subfolder=...&type=output` and saves raw bytes
    /// to `dest_path`. Creates parent directories if needed.
    pub async fn download_image(
        &self,
        filename: &str,
        subfolder: &str,
        dest_path: &Path,
    ) -> Result<u64, DomainError> {
        let mut url = format!(
            "{}/view?filename={}&type=output",
            self.base_url,
            urlencoding::encode(filename),
        );
        if !subfolder.is_empty() {
            url.push_str(&format!("&subfolder={}", urlencoding::encode(subfolder)));
        }

        let resp = self.get_request(&url).send().await.map_err(|e| {
            DomainError::ComfyUiConnection(format!("failed to GET /view for {filename}: {e}"))
        })?;

        if !resp.status().is_success() {
            return Err(DomainError::ComfyUiConnection(format!(
                "GET /view returned HTTP {} for {filename}",
                resp.status()
            )));
        }

        let bytes = resp.bytes().await.map_err(|e| {
            DomainError::ComfyUiConnection(format!("failed to read image bytes: {e}"))
        })?;

        if let Some(parent) = dest_path.parent() {
            tokio::fs::create_dir_all(parent).await.map_err(|e| {
                DomainError::ComfyUiConnection(format!(
                    "failed to create directory {}: {e}",
                    parent.display()
                ))
            })?;
        }

        tokio::fs::write(dest_path, &bytes).await.map_err(|e| {
            DomainError::ComfyUiConnection(format!("failed to write {}: {e}", dest_path.display()))
        })?;

        Ok(bytes.len() as u64)
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
        let client = ComfyUiClient::new("http://localhost:8188".into(), Some("mytoken".into()));
        assert_eq!(client.base_url(), "http://localhost:8188");
    }
}
