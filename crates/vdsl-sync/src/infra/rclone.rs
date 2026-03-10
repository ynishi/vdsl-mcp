//! Rclone-based storage backend.
//!
//! Executes `rclone` CLI commands for file transfer to/from cloud storage.
//! Supports any rclone-compatible remote (B2, S3, GCS, etc.).
//!
//! Commands are executed via a [`RemoteShell`],
//! enabling transfer from different hosts (local machine, GPU pod, etc.).

use std::path::Path;

use async_trait::async_trait;
use secrecy::{ExposeSecret, SecretBox};

use super::backend::{RemoteFile, StorageBackend};
use super::shell::{LocalShell, RemoteShell};
use crate::domain::error::SyncError;

/// Rclone CLI storage backend.
///
/// Uses inline credentials via `:backend,key=val:bucket/path` syntax
/// to avoid requiring global rclone config files.
///
/// The remote string is wrapped in [`SecretBox`] to prevent accidental
/// logging of embedded credentials.
///
/// Commands are executed via a [`RemoteShell`], defaulting to [`LocalShell`].
pub struct RcloneBackend {
    /// Rclone remote string, e.g. `:b2,account=KEY_ID,key=KEY:bucket`.
    /// Wrapped in Secret to prevent accidental credential exposure in logs.
    remote: SecretBox<String>,
    /// Shell for executing rclone commands.
    shell: Box<dyn RemoteShell>,
}

impl RcloneBackend {
    /// Create a new RcloneBackend with the given remote string.
    ///
    /// Uses [`LocalShell`] for command execution (backward compatible).
    ///
    /// # Example
    /// ```no_run
    /// # use vdsl_sync::infra::rclone::RcloneBackend;
    /// let backend = RcloneBackend::new(":b2,account=key_id,key=secret:my-bucket");
    /// ```
    pub fn new(remote: impl Into<String>) -> Self {
        Self {
            remote: SecretBox::new(Box::new(remote.into())),
            shell: Box::new(LocalShell),
        }
    }

    /// Create with a custom [`RemoteShell`] (e.g. PodShell for GPU pod execution).
    pub fn with_shell(remote: impl Into<String>, shell: Box<dyn RemoteShell>) -> Self {
        Self {
            remote: SecretBox::new(Box::new(remote.into())),
            shell,
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

    /// Execute an rclone command via the configured shell.
    async fn exec_rclone(&self, args: &[&str]) -> Result<String, SyncError> {
        let mut full_args = vec!["rclone"];
        full_args.extend_from_slice(args);

        let output = self.shell.exec(&full_args, None).await?;

        if !output.success {
            return Err(SyncError::TransferFailed(format!(
                "rclone failed (exit {}): {}",
                output
                    .exit_code
                    .map_or("signal".to_string(), |c| c.to_string()),
                output.stderr.trim()
            )));
        }

        Ok(output.stdout)
    }
}

#[async_trait]
impl StorageBackend for RcloneBackend {
    async fn push(&self, local_path: &Path, remote_path: &str) -> Result<(), SyncError> {
        let dest = self.remote_path(remote_path)?;
        let local_str = local_path.to_str().ok_or_else(|| {
            SyncError::TransferFailed(format!(
                "local path is not valid UTF-8: {}",
                local_path.to_string_lossy()
            ))
        })?;
        self.exec_rclone(&["copyto", local_str, &dest]).await?;
        Ok(())
    }

    async fn pull(&self, remote_path: &str, local_path: &Path) -> Result<(), SyncError> {
        let src = self.remote_path(remote_path)?;
        // Ensure parent directory exists
        if let Some(parent) = local_path.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }
        let local_str = local_path.to_str().ok_or_else(|| {
            SyncError::TransferFailed(format!(
                "local path is not valid UTF-8: {}",
                local_path.to_string_lossy()
            ))
        })?;
        self.exec_rclone(&["copyto", &src, local_str]).await?;
        Ok(())
    }

    async fn list(&self, remote_path: &str) -> Result<Vec<RemoteFile>, SyncError> {
        let target = self.remote_path(remote_path)?;
        let output = self
            .exec_rclone(&["lsf", "--format", "ps", &target])
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
        let result = self.exec_rclone(&["lsf", &target]).await;
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
