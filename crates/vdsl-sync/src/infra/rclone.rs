//! Rclone-based storage backend.
//!
//! Executes `rclone` CLI commands for file transfer to/from cloud storage.
//! Supports any rclone-compatible remote (B2, S3, GCS, etc.).

use std::ffi::OsStr;
use std::path::Path;

use async_trait::async_trait;
use secrecy::{ExposeSecret, SecretBox};
use tokio::process::Command;

use super::backend::{RemoteFile, StorageBackend};
use crate::domain::error::SyncError;

/// Rclone CLI storage backend.
///
/// Uses inline credentials via `:backend,key=val:bucket/path` syntax
/// to avoid requiring global rclone config files.
///
/// The remote string is wrapped in [`Secret`] to prevent accidental
/// logging of embedded credentials.
///
/// All operations are fully async via `tokio::process::Command`.
pub struct RcloneBackend {
    /// Rclone remote string, e.g. `:b2,account=KEY_ID,key=KEY:bucket`.
    /// Wrapped in Secret to prevent accidental credential exposure in logs.
    remote: SecretBox<String>,
}

impl RcloneBackend {
    /// Create a new RcloneBackend with the given remote string.
    ///
    /// # Example
    /// ```no_run
    /// # use vdsl_sync::infra::rclone::RcloneBackend;
    /// let backend = RcloneBackend::new(":b2,account=key_id,key=secret:my-bucket");
    /// ```
    pub fn new(remote: impl Into<String>) -> Self {
        Self {
            remote: SecretBox::new(Box::new(remote.into())),
        }
    }

    /// Build the full remote path for a given relative path.
    ///
    /// Validates against CLI flag injection (`-` prefix) and
    /// path traversal (`..` segments).
    fn remote_path(&self, path: &str) -> Result<String, SyncError> {
        let path = path.trim_matches('/');
        // Reject paths that look like CLI flags (argument injection)
        if path.starts_with('-') {
            return Err(SyncError::TransferFailed(format!(
                "invalid remote path (starts with '-'): {path}"
            )));
        }
        // Reject path traversal attempts
        if path.split('/').any(|seg| seg == "..") {
            return Err(SyncError::TransferFailed(format!(
                "invalid remote path (contains '..' traversal): {path}"
            )));
        }
        let remote = self.remote.expose_secret();
        if path.is_empty() {
            Ok(remote.clone())
        } else {
            Ok(format!("{remote}/{path}"))
        }
    }

    /// Execute an rclone command asynchronously and return stdout on success.
    ///
    /// Accepts `&[&OsStr]` to preserve non-UTF-8 paths (e.g. local file paths)
    /// without lossy conversion.
    async fn exec(&self, args: &[&OsStr]) -> Result<String, SyncError> {
        let output = Command::new("rclone")
            .args(args)
            .output()
            .await
            .map_err(|e| SyncError::TransferFailed(format!("rclone exec failed: {e}")))?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(SyncError::TransferFailed(format!(
                "rclone failed (exit {}): {}",
                output
                    .status
                    .code()
                    .map_or_else(|| "unknown".to_string(), |c| c.to_string()),
                stderr.trim()
            )));
        }

        Ok(String::from_utf8_lossy(&output.stdout).to_string())
    }
}

#[async_trait]
impl StorageBackend for RcloneBackend {
    async fn push(&self, local_path: &Path, remote_path: &str) -> Result<(), SyncError> {
        let dest = self.remote_path(remote_path)?;
        self.exec(&[
            OsStr::new("copyto"),
            local_path.as_os_str(),
            OsStr::new(&dest),
        ])
        .await?;
        Ok(())
    }

    async fn pull(&self, remote_path: &str, local_path: &Path) -> Result<(), SyncError> {
        let src = self.remote_path(remote_path)?;
        // Ensure parent directory exists
        if let Some(parent) = local_path.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }
        self.exec(&[
            OsStr::new("copyto"),
            OsStr::new(&src),
            local_path.as_os_str(),
        ])
        .await?;
        Ok(())
    }

    async fn list(&self, remote_path: &str) -> Result<Vec<RemoteFile>, SyncError> {
        let target = self.remote_path(remote_path)?;
        let output = self
            .exec(&[
                OsStr::new("lsf"),
                OsStr::new("--format"),
                OsStr::new("ps"),
                OsStr::new(&target),
            ])
            .await?;

        let mut files = Vec::new();
        for line in output.lines() {
            // Format: "path;size"
            if let Some((path, size_str)) = line.split_once(';') {
                let size = match size_str.trim().parse::<u64>() {
                    Ok(s) => Some(s),
                    Err(e) => {
                        tracing::debug!(
                            path = path,
                            raw_size = size_str.trim(),
                            error = %e,
                            "rclone lsf: size parse failed, treating as unknown"
                        );
                        None
                    }
                };
                files.push(RemoteFile {
                    path: path.to_string(),
                    size,
                });
            }
        }
        Ok(files)
    }

    /// Note: returns `Ok(false)` on any rclone error (including network failures).
    /// This is a best-effort check — callers must not rely on `false` meaning
    /// "confirmed absent". Use `push`/`pull` for authoritative operations.
    async fn exists(&self, remote_path: &str) -> Result<bool, SyncError> {
        let target = self.remote_path(remote_path)?;
        let result = self.exec(&[OsStr::new("lsf"), OsStr::new(&target)]).await;
        match result {
            Ok(output) => Ok(!output.trim().is_empty()),
            Err(e) => {
                tracing::debug!(
                    remote_path = remote_path,
                    error = %e,
                    "rclone exists check failed, returning false"
                );
                Ok(false)
            }
        }
    }

    fn backend_type(&self) -> &str {
        "rclone"
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn remote_path_construction() {
        let b = RcloneBackend::new(":b2,account=kid,key=k:bucket");
        assert_eq!(
            b.remote_path("models/ckpt.safetensors").unwrap(),
            ":b2,account=kid,key=k:bucket/models/ckpt.safetensors"
        );
        assert_eq!(
            b.remote_path("/leading/slash").unwrap(),
            ":b2,account=kid,key=k:bucket/leading/slash"
        );
        assert_eq!(b.remote_path("").unwrap(), ":b2,account=kid,key=k:bucket");
    }

    #[test]
    fn remote_path_rejects_flag_like_input() {
        let b = RcloneBackend::new("remote:bucket");
        assert!(b.remote_path("--config=/etc/rclone.conf").is_err());
        assert!(b.remote_path("-v").is_err());
    }

    #[test]
    fn remote_path_rejects_traversal() {
        let b = RcloneBackend::new("remote:bucket");
        assert!(b.remote_path("../../etc/passwd").is_err());
        assert!(b.remote_path("foo/../bar").is_err());
        assert!(b.remote_path("..").is_err());
        // Single dot is OK (current directory reference, harmless)
        assert!(b.remote_path("./valid").is_ok());
        // "..." is not "..", should be OK
        assert!(b.remote_path("a/.../b").is_ok());
    }

    #[test]
    fn backend_type() {
        let b = RcloneBackend::new("remote:bucket");
        assert_eq!(b.backend_type(), "rclone");
    }
}
