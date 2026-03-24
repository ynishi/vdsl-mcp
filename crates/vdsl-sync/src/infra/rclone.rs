//! Rclone-based storage backend.
//!
//! Executes `rclone` CLI commands for file transfer to/from cloud storage.
//! Supports any rclone-compatible remote (B2, S3, GCS, etc.).
//!
//! Commands are executed via a [`RemoteShell`],
//! enabling transfer from different hosts (local machine, GPU pod, etc.).

use std::collections::HashMap;
use std::path::Path;

use async_trait::async_trait;
use chrono::{DateTime, NaiveDateTime, Utc};
use secrecy::{ExposeSecret, SecretBox};

use super::backend::{RemoteFile, StorageBackend};
use super::shell::{LocalShell, RemoteShell};
use crate::application::error::SyncError;
use crate::infra::error::InfraError;

/// Default rclone command timeout (seconds).
///
/// Applies to individual rclone operations (copyto, lsf, deletefile).
/// Batch operations (`rclone copy --files-from`) use a scaled timeout.
const DEFAULT_RCLONE_TIMEOUT_SECS: u64 = 300;

/// Environment variable to override the default rclone timeout.
///
/// Set via MCP server config or shell environment.
/// Value: timeout in seconds (e.g. `VDSL_RCLONE_TIMEOUT=600`).
const RCLONE_TIMEOUT_ENV: &str = "VDSL_RCLONE_TIMEOUT";

/// Minimum timeout floor (seconds). Prevents misconfiguration.
const MIN_RCLONE_TIMEOUT_SECS: u64 = 10;

/// Per-file additional timeout for batch operations (seconds).
///
/// Batch timeout = base_timeout + (file_count * BATCH_PER_FILE_TIMEOUT_SECS).
/// Prevents large batches from hitting the timeout while small batches
/// still fail fast on network issues.
const BATCH_PER_FILE_TIMEOUT_SECS: u64 = 30;

/// Resolve rclone timeout from: explicit > env > default.
///
/// Priority:
/// 1. `explicit` — passed at construction (Config / API)
/// 2. `VDSL_RCLONE_TIMEOUT` env var — set via MCP or shell
/// 3. `DEFAULT_RCLONE_TIMEOUT_SECS` — compile-time default
///
/// Floor: `MIN_RCLONE_TIMEOUT_SECS` (guards against misconfiguration).
fn resolve_timeout(explicit: Option<u64>) -> u64 {
    let raw = explicit
        .or_else(|| {
            std::env::var(RCLONE_TIMEOUT_ENV)
                .ok()
                .and_then(|v| v.parse::<u64>().ok())
        })
        .unwrap_or(DEFAULT_RCLONE_TIMEOUT_SECS);
    raw.max(MIN_RCLONE_TIMEOUT_SECS)
}

/// Rclone CLI storage backend.
///
/// Uses inline credentials via `:backend,key=val:bucket/path` syntax
/// to avoid requiring global rclone config files.
///
/// The remote string is wrapped in [`SecretBox`] to prevent accidental
/// logging of embedded credentials.
///
/// Commands are executed via a [`RemoteShell`], defaulting to [`LocalShell`].
///
/// # Timeout configuration
///
/// Resolution order: explicit (`with_timeout`) > env (`VDSL_RCLONE_TIMEOUT`) > default (300s).
/// Batch operations scale the timeout by file count.
pub struct RcloneBackend {
    /// Rclone remote string, e.g. `:b2,account=KEY_ID,key=KEY:bucket`.
    /// Wrapped in Secret to prevent accidental credential exposure in logs.
    remote: SecretBox<String>,
    /// Shell for executing rclone commands.
    shell: Box<dyn RemoteShell>,
    /// Per-command timeout in seconds.
    timeout_secs: u64,
}

impl RcloneBackend {
    /// Create a new RcloneBackend with the given remote string.
    ///
    /// Timeout: env `VDSL_RCLONE_TIMEOUT` or default 300s.
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
            timeout_secs: resolve_timeout(None),
        }
    }

    /// Create with a custom [`RemoteShell`] (e.g. PodShell for GPU pod execution).
    pub fn with_shell(remote: impl Into<String>, shell: Box<dyn RemoteShell>) -> Self {
        Self {
            remote: SecretBox::new(Box::new(remote.into())),
            shell,
            timeout_secs: resolve_timeout(None),
        }
    }

    /// Set an explicit timeout (seconds), overriding env and default.
    ///
    /// Useful when constructing from parsed config values.
    /// Floor: 10 seconds.
    pub fn with_timeout(mut self, timeout_secs: u64) -> Self {
        self.timeout_secs = resolve_timeout(Some(timeout_secs));
        self
    }

    /// Build the full remote path for a given relative path.
    ///
    /// Validates against CLI flag injection (`-` prefix) and
    /// path traversal (`..` segments).
    fn remote_path(&self, path: &str) -> Result<String, SyncError> {
        let path = path.trim_matches('/');
        // Reject paths that look like CLI flags (argument injection)
        if path.starts_with('-') {
            return Err(InfraError::Transfer {
                reason: format!("invalid remote path (starts with '-'): {path}"),
            }
            .into());
        }
        // Reject path traversal attempts
        if path.split('/').any(|seg| seg == "..") {
            return Err(InfraError::Transfer {
                reason: format!("invalid remote path (contains '..' traversal): {path}"),
            }
            .into());
        }
        let remote = self.remote.expose_secret();
        if path.is_empty() {
            Ok(remote.clone())
        } else {
            Ok(format!("{remote}/{path}"))
        }
    }

    /// Execute an rclone command via the configured shell.
    ///
    /// Uses the configured timeout (`with_timeout` > `VDSL_RCLONE_TIMEOUT` > 300s).
    /// Callers needing a different timeout (e.g. batch) should use `exec_rclone_with_timeout`.
    async fn exec_rclone(&self, args: &[&str]) -> Result<String, SyncError> {
        self.exec_rclone_with_timeout(args, self.timeout_secs).await
    }

    /// Execute an rclone command with an explicit timeout.
    async fn exec_rclone_with_timeout(
        &self,
        args: &[&str],
        timeout_secs: u64,
    ) -> Result<String, SyncError> {
        let mut full_args = vec!["rclone"];
        full_args.extend_from_slice(args);

        let output = self.shell.exec(&full_args, Some(timeout_secs)).await?;

        if !output.success {
            return Err(InfraError::Transfer {
                reason: format!(
                    "rclone failed (exit {}): {}",
                    output
                        .exit_code
                        .map_or("signal".to_string(), |c| c.to_string()),
                    output.stderr.trim()
                ),
            }
            .into());
        }

        Ok(output.stdout)
    }
}

#[async_trait]
impl StorageBackend for RcloneBackend {
    async fn push(&self, local_path: &Path, remote_path: &str) -> Result<(), SyncError> {
        let dest = self.remote_path(remote_path)?;
        let local_str = local_path.to_str().ok_or_else(|| -> SyncError {
            InfraError::Transfer {
                reason: format!(
                    "local path is not valid UTF-8: {}",
                    local_path.to_string_lossy()
                ),
            }
            .into()
        })?;
        self.exec_rclone(&["copyto", local_str, &dest]).await?;
        Ok(())
    }

    async fn pull(&self, remote_path: &str, local_path: &Path) -> Result<(), SyncError> {
        let src = self.remote_path(remote_path)?;
        // Ensure parent directory exists via shell (works for both local and remote hosts)
        if let Some(parent) = local_path.parent() {
            if let Some(parent_str) = parent.to_str() {
                if !parent_str.is_empty() {
                    let _ = self
                        .shell
                        .exec(&["mkdir", "-p", parent_str], Some(10))
                        .await;
                }
            }
        }
        let local_str = local_path.to_str().ok_or_else(|| -> SyncError {
            InfraError::Transfer {
                reason: format!(
                    "local path is not valid UTF-8: {}",
                    local_path.to_string_lossy()
                ),
            }
            .into()
        })?;
        self.exec_rclone(&["copyto", &src, local_str]).await?;
        Ok(())
    }

    async fn list(&self, remote_path: &str) -> Result<Vec<RemoteFile>, SyncError> {
        let target = self.remote_path(remote_path)?;
        // Format: "path;size;modtime" — modtime is ISO 8601 (e.g. "2024-01-15T10:30:00.000000000")
        // --files-only excludes directory markers (B2/S3 "folders" are 0-byte objects
        // that would be registered as files, causing phantom delete transfers).
        let output = self
            .exec_rclone(&["lsf", "--format", "pst", "--files-only", &target])
            .await?;

        let mut files = Vec::new();
        for line in output.lines() {
            let parts: Vec<&str> = line.splitn(3, ';').collect();
            if parts.len() < 2 {
                continue;
            }
            let path = parts[0];
            let size = match parts[1].trim().parse::<u64>() {
                Ok(s) => Some(s),
                Err(e) => {
                    tracing::debug!(
                        path = path,
                        raw_size = parts[1].trim(),
                        error = %e,
                        "rclone lsf: size parse failed, treating as unknown"
                    );
                    None
                }
            };
            let modified_at = parts.get(2).and_then(|ts| parse_rclone_timestamp(ts));
            files.push(RemoteFile {
                path: path.to_string(),
                size,
                modified_at,
            });
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

    async fn delete(&self, remote_path: &str) -> Result<(), SyncError> {
        let target = self.remote_path(remote_path)?;
        // rclone deletefile removes a single file (not directory).
        // --retries 1 to fail fast on permanent errors.
        //
        // Per StorageBackend::delete contract: "Ok(()) if the file was
        // deleted or didn't exist". rclone exit 4 = "not found" satisfies
        // the postcondition (file is absent at dest), so we treat it as Ok.
        match self
            .exec_rclone(&["deletefile", &target, "--retries", "1"])
            .await
        {
            Ok(_) => Ok(()),
            Err(e) => {
                let msg = e.to_string();
                // rclone exit code 4 = object/directory not found.
                // The delete goal (file absent at dest) is already met.
                if msg.contains("exit 4") || msg.contains("not found") {
                    tracing::debug!(
                        remote_path = remote_path,
                        "rclone deletefile: object already absent, treating as success"
                    );
                    Ok(())
                } else {
                    Err(e)
                }
            }
        }
    }

    /// Batch push using `rclone copy --files-from`.
    ///
    /// Single rclone process for N files: one auth handshake, internal
    /// parallelism via `--transfers`. Significantly faster than N individual
    /// `rclone copyto` calls, especially on high-latency links (home ISP upload).
    ///
    /// Returns per-file Ok/Err. On rclone success (exit 0), all files are Ok.
    /// On failure, checks which files actually arrived at dest to determine
    /// per-file status.
    async fn push_batch(
        &self,
        src_root: &Path,
        dest_root: &str,
        relative_paths: &[String],
    ) -> HashMap<String, Result<(), SyncError>> {
        if relative_paths.is_empty() {
            return HashMap::new();
        }

        // Validate dest_root (same rules as remote_path)
        if self.remote_path(dest_root).is_err() {
            let reason = format!("invalid dest_root for batch push: {dest_root}");
            return relative_paths
                .iter()
                .map(|p| {
                    (
                        p.clone(),
                        Err(SyncError::from(InfraError::Transfer {
                            reason: reason.clone(),
                        })),
                    )
                })
                .collect();
        }

        let src_root_str = match src_root.to_str() {
            Some(s) => s,
            None => {
                let reason = format!(
                    "src_root is not valid UTF-8: {}",
                    src_root.to_string_lossy()
                );
                return relative_paths
                    .iter()
                    .map(|p| {
                        (
                            p.clone(),
                            Err(SyncError::from(InfraError::Transfer {
                                reason: reason.clone(),
                            })),
                        )
                    })
                    .collect();
            }
        };

        let dest_full = self.remote_path(dest_root).expect("validated above");

        // Scaled timeout: base + per-file allowance
        let batch_timeout =
            self.timeout_secs + (relative_paths.len() as u64 * BATCH_PER_FILE_TIMEOUT_SECS);

        // Build shell script that creates --files-from on the execution host,
        // runs rclone, then cleans up. Works on both local and remote shells.
        let file_list = relative_paths.join("\n");
        let list_filename = format!("vdsl-batch-{}.txt", uuid::Uuid::new_v4().as_simple());
        let script = format!(
            "cat <<'__VDSL_EOF__' > /tmp/{list_filename}\n\
             {file_list}\n\
             __VDSL_EOF__\n\
             rclone copy {src_root_str} {dest_full} \
               --files-from /tmp/{list_filename} --transfers 8; \
             _rc=$?; rm -f /tmp/{list_filename}; exit $_rc"
        );

        let result = self.shell.exec_script(&script, Some(batch_timeout)).await;

        match result {
            Ok(output) if output.success => {
                relative_paths.iter().map(|p| (p.clone(), Ok(()))).collect()
            }
            Ok(output) => {
                let err_msg = format!(
                    "rclone failed (exit {}): {}",
                    output
                        .exit_code
                        .map_or("signal".to_string(), |c| c.to_string()),
                    output.stderr.trim()
                );
                relative_paths
                    .iter()
                    .map(|p| {
                        (
                            p.clone(),
                            Err(SyncError::from(InfraError::Transfer {
                                reason: format!("batch push failed: {err_msg}"),
                            })),
                        )
                    })
                    .collect()
            }
            Err(e) => {
                // Partial failure. Report all as failed with the batch error.
                // A more sophisticated impl could check dest existence per file,
                // but that adds N API calls — defeating the batch purpose.
                let err_msg = e.to_string();
                relative_paths
                    .iter()
                    .map(|p| {
                        (
                            p.clone(),
                            Err(SyncError::from(InfraError::Transfer {
                                reason: format!("batch push failed: {err_msg}"),
                            })),
                        )
                    })
                    .collect()
            }
        }
    }

    /// Batch pull using `rclone copy --files-from`.
    ///
    /// Mirror of `push_batch` but reversed: pulls from remote to local (or Pod filesystem).
    /// Single rclone process with `--transfers 8` for internal parallelism.
    async fn pull_batch(
        &self,
        src_root: &str,
        dest_root: &Path,
        relative_paths: &[String],
    ) -> HashMap<String, Result<(), SyncError>> {
        if relative_paths.is_empty() {
            return HashMap::new();
        }

        // Validate src_root (remote path)
        let src_full = match self.remote_path(src_root) {
            Ok(s) => s,
            Err(_) => {
                let reason = format!("invalid src_root for batch pull: {src_root}");
                return Self::all_batch_err(relative_paths, &reason);
            }
        };

        let dest_root_str = match dest_root.to_str() {
            Some(s) => s,
            None => {
                let reason = format!(
                    "dest_root is not valid UTF-8: {}",
                    dest_root.to_string_lossy()
                );
                return Self::all_batch_err(relative_paths, &reason);
            }
        };

        // Scaled timeout: base + per-file allowance
        let batch_timeout =
            self.timeout_secs + (relative_paths.len() as u64 * BATCH_PER_FILE_TIMEOUT_SECS);

        // Build shell script that creates --files-from on the execution host,
        // runs rclone, then cleans up. Works on both local and remote shells.
        let file_list = relative_paths.join("\n");
        let list_filename = format!("vdsl-pull-batch-{}.txt", uuid::Uuid::new_v4().as_simple());
        let script = format!(
            "cat <<'__VDSL_EOF__' > /tmp/{list_filename}\n\
             {file_list}\n\
             __VDSL_EOF__\n\
             rclone copy {src_full} {dest_root_str} \
               --files-from /tmp/{list_filename} --transfers 8; \
             _rc=$?; rm -f /tmp/{list_filename}; exit $_rc"
        );

        let result = self.shell.exec_script(&script, Some(batch_timeout)).await;

        match result {
            Ok(output) if output.success => {
                relative_paths.iter().map(|p| (p.clone(), Ok(()))).collect()
            }
            Ok(output) => {
                let err_msg = format!(
                    "rclone failed (exit {}): {}",
                    output
                        .exit_code
                        .map_or("signal".to_string(), |c| c.to_string()),
                    output.stderr.trim()
                );
                Self::all_batch_err(relative_paths, &format!("batch pull failed: {err_msg}"))
            }
            Err(e) => Self::all_batch_err(relative_paths, &format!("batch pull failed: {e}")),
        }
    }

    fn supports_batch(&self) -> bool {
        true
    }

    fn backend_type(&self) -> &str {
        "rclone"
    }
}

impl RcloneBackend {
    /// Helper: build all-error result map for batch operations.
    fn all_batch_err(
        relative_paths: &[String],
        reason: &str,
    ) -> HashMap<String, Result<(), SyncError>> {
        relative_paths
            .iter()
            .map(|p| {
                (
                    p.clone(),
                    Err(SyncError::from(InfraError::Transfer {
                        reason: reason.to_string(),
                    })),
                )
            })
            .collect()
    }
}

/// Parse rclone's timestamp format into `DateTime<Utc>`.
///
/// rclone lsf `%t` outputs ISO 8601 with nanoseconds:
/// `"2024-01-15T10:30:00.000000000"` (no timezone — always UTC).
fn parse_rclone_timestamp(s: &str) -> Option<DateTime<Utc>> {
    let trimmed = s.trim();
    // Try full nanosecond format first, then fall back to simpler formats
    NaiveDateTime::parse_from_str(trimmed, "%Y-%m-%dT%H:%M:%S%.f")
        .or_else(|_| NaiveDateTime::parse_from_str(trimmed, "%Y-%m-%dT%H:%M:%S"))
        .ok()
        .map(|naive| naive.and_utc())
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

    #[test]
    fn parse_rclone_timestamp_nanoseconds() {
        let ts = parse_rclone_timestamp("2024-01-15T10:30:00.123456789");
        assert!(ts.is_some());
        let dt = ts.unwrap();
        assert_eq!(dt.year(), 2024);
        assert_eq!(dt.month(), 1);
        assert_eq!(dt.day(), 15);
        assert_eq!(dt.hour(), 10);
        assert_eq!(dt.minute(), 30);
    }

    #[test]
    fn parse_rclone_timestamp_no_fraction() {
        let ts = parse_rclone_timestamp("2024-01-15T10:30:00");
        assert!(ts.is_some());
    }

    #[test]
    fn parse_rclone_timestamp_invalid() {
        assert!(parse_rclone_timestamp("not-a-date").is_none());
        assert!(parse_rclone_timestamp("").is_none());
    }

    use chrono::Datelike;
    use chrono::Timelike;
}
