use crate::domain::error::DomainError;
use crate::infra::runpod_cli::RunPodCli;

use super::error::AppError;

/// RunPod Pod management use cases.
pub struct PodService {
    cli: RunPodCli,
}

impl PodService {
    pub fn new(cli: RunPodCli) -> Self {
        Self { cli }
    }

    /// List all pods.
    pub async fn list_pods(&self) -> Result<Vec<serde_json::Value>, AppError> {
        self.cli.list_pods().await.map_err(AppError::from)
    }

    /// Start (or resume) a pod.
    pub async fn start_pod(&self, pod_id: &str) -> Result<serde_json::Value, AppError> {
        self.cli.start_pod(pod_id).await.map_err(AppError::from)
    }

    /// Stop a pod.
    pub async fn stop_pod(&self, pod_id: &str) -> Result<serde_json::Value, AppError> {
        self.cli.stop_pod(pod_id).await.map_err(AppError::from)
    }

    /// Delete a pod permanently.
    pub async fn delete_pod(&self, pod_id: &str) -> Result<serde_json::Value, AppError> {
        self.cli.delete_pod(pod_id).await.map_err(AppError::from)
    }

    /// List network volumes.
    pub async fn list_volumes(&self) -> Result<Vec<serde_json::Value>, AppError> {
        self.cli.list_volumes().await.map_err(AppError::from)
    }
}

/// Resolve RunPod API key from environment.
pub fn resolve_api_key() -> Result<String, DomainError> {
    std::env::var("RUNPOD_API_KEY")
        .map_err(|_| DomainError::ApiKeyMissing)
        .and_then(|k| {
            if k.is_empty() {
                Err(DomainError::ApiKeyMissing)
            } else {
                Ok(k)
            }
        })
}
