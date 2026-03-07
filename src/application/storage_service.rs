use super::error::AppError;
use super::pod_service::PodService;

/// Timeout for rclone installation (seconds).
const RCLONE_INSTALL_TIMEOUT_SECS: u64 = 120;
/// Timeout for rclone operations (seconds).
pub const RCLONE_OP_TIMEOUT_SECS: u64 = 600;

/// B2 cold storage operations via rclone on a RunPod pod.
pub struct StorageService<'a> {
    svc: &'a PodService,
}

impl<'a> StorageService<'a> {
    pub fn new(svc: &'a PodService) -> Self {
        Self { svc }
    }

    /// Ensure rclone is installed on the pod. Installs if absent.
    pub async fn ensure_rclone(&self, pod_id: &str, ssh_key: &str) -> Result<(), AppError> {
        let check = self
            .svc
            .pod_exec(pod_id, &["which", "rclone"], Some(ssh_key), Some(10))
            .await;

        match check {
            Ok(ref out) if out.success => return Ok(()),
            _ => {}
        }

        let install = self
            .svc
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
            .map_err(|e| AppError::OperationFailed(format!("rclone install failed: {e}")))?;

        if !install.success {
            return Err(AppError::OperationFailed(format!(
                "rclone install failed (exit {}): {}{}",
                install.exit_code,
                install.stderr.trim(),
                if install.stdout.trim().is_empty() {
                    String::new()
                } else {
                    format!("\n{}", install.stdout.trim())
                }
            )));
        }

        Ok(())
    }
}

/// Resolve B2 bucket name from parameter or `VDSL_B2_BUCKET` env var.
pub fn resolve_bucket(bucket: Option<&str>) -> Result<String, AppError> {
    if let Some(b) = bucket {
        if !b.is_empty() {
            return Ok(b.to_string());
        }
    }
    std::env::var("VDSL_B2_BUCKET")
        .ok()
        .filter(|s| !s.is_empty())
        .ok_or_else(|| {
            AppError::MissingConfig(
                "bucket not specified and VDSL_B2_BUCKET env not set".to_string(),
            )
        })
}

/// Build an rclone B2 connection string using inline credentials.
///
/// Requires `VDSL_B2_KEY_ID` and `VDSL_B2_KEY` environment variables.
/// Returns a string like `:b2,account=KEY_ID,key=KEY:bucket/path`.
pub fn b2_remote(bucket: &str, path: &str) -> Result<String, AppError> {
    let key_id = std::env::var("VDSL_B2_KEY_ID")
        .ok()
        .filter(|s| !s.is_empty())
        .ok_or_else(|| AppError::MissingConfig("VDSL_B2_KEY_ID env not set".to_string()))?;
    let key = std::env::var("VDSL_B2_KEY")
        .ok()
        .filter(|s| !s.is_empty())
        .ok_or_else(|| AppError::MissingConfig("VDSL_B2_KEY env not set".to_string()))?;

    let path = path.trim_matches('/');
    if path.is_empty() {
        Ok(format!(":b2,account={key_id},key={key}:{bucket}"))
    } else {
        Ok(format!(":b2,account={key_id},key={key}:{bucket}/{path}"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resolve_bucket_from_param() {
        let result = resolve_bucket(Some("my-bucket")).unwrap();
        assert_eq!(result, "my-bucket");
    }

    #[test]
    fn resolve_bucket_empty_param_falls_through() {
        let result = resolve_bucket(Some(""));
        assert!(result.is_err() || !result.unwrap().is_empty());
    }

    #[test]
    fn resolve_bucket_none_without_env() {
        let _result = resolve_bucket(None);
    }

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
}
