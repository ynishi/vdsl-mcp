//! Rclone-based storage backend.
//!
//! Executes `rclone` CLI commands for file transfer to/from cloud storage.
//! Supports any rclone-compatible remote (B2, S3, GCS, etc.).
//!
//! Commands are executed via a [`RemoteShell`],
//! enabling transfer from different hosts (local machine, GPU pod, etc.).

use std::collections::HashMap;
use std::path::Path;
use std::sync::Mutex as StdMutex;

use async_trait::async_trait;
use chrono::{DateTime, NaiveDateTime, Utc};
use secrecy::{ExposeSecret, SecretBox};

use super::backend::{ProgressFn, RemoteFile, StorageBackend};
use super::shell::{LocalShell, RemoteShell};
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

/// Extra rclone flags for SFTP remotes to reduce protocol overhead.
///
/// SFTP performs per-file round-trips for modtime setting and hash verification
/// after each transfer. On high-latency / low-bandwidth links (home ISP upload),
/// this overhead dominates — observed 100 KB/s vs 1 MB/s on the same link via
/// other protocols. These flags eliminate the extra round-trips:
///
/// - `--sftp-set-modtime=false`: skip post-transfer SFTP setstat for mtime
///   (vdsl-sync uses sha256 for identity, not mtime)
/// - `--sftp-disable-hashcheck`: skip post-transfer sha256sum via SFTP exec
///   (vdsl-sync runs its own batch_inspect for verification)
const SFTP_OPTIMIZATION_FLAGS: &[&str] = &["--sftp-set-modtime=false", "--sftp-disable-hashcheck"];

/// Chunk size for SFTP batch transfers.
///
/// Large SFTP batches (thousands of files) cause rclone to stall due to
/// SFTP session management overhead. Chunking into smaller batches with
/// per-chunk progress logging and retry prevents hangs and provides visibility.
///
/// Non-SFTP backends (B2, S3) handle large batches natively — no chunking.
const SFTP_BATCH_CHUNK_SIZE: usize = 100;

/// Maximum retries per chunk on failure.
const BATCH_CHUNK_MAX_RETRIES: u32 = 1;

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
    /// Progress callback for reporting chunk completion during batch transfers.
    progress: StdMutex<Option<ProgressFn>>,
}

impl RcloneBackend {
    /// Create a new RcloneBackend with the given remote string.
    ///
    /// Timeout: env `VDSL_RCLONE_TIMEOUT` or default 300s.
    /// Uses [`LocalShell`] for command execution (backward compatible).
    ///
    /// # Example
    /// ```no_run
    /// # use vdsl_sync::RcloneBackend;
    /// let backend = RcloneBackend::new(":b2,account=key_id,key=secret:my-bucket");
    /// ```
    pub fn new(remote: impl Into<String>) -> Self {
        Self {
            remote: SecretBox::new(Box::new(remote.into())),
            shell: Box::new(LocalShell),
            timeout_secs: resolve_timeout(None),
            progress: StdMutex::new(None),
        }
    }

    /// Create with a custom [`RemoteShell`] (e.g. PodShell for GPU pod execution).
    pub fn with_shell(remote: impl Into<String>, shell: Box<dyn RemoteShell>) -> Self {
        Self {
            remote: SecretBox::new(Box::new(remote.into())),
            shell,
            timeout_secs: resolve_timeout(None),
            progress: StdMutex::new(None),
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
    fn remote_path(&self, path: &str) -> Result<String, InfraError> {
        let path = path.trim_matches('/');
        // Reject paths that look like CLI flags (argument injection)
        if path.starts_with('-') {
            return Err(InfraError::Transfer {
                reason: format!("invalid remote path (starts with '-'): {path}"),
            });
        }
        // Reject path traversal attempts
        if path.split('/').any(|seg| seg == "..") {
            return Err(InfraError::Transfer {
                reason: format!("invalid remote path (contains '..' traversal): {path}"),
            });
        }
        let remote = self.remote.expose_secret();
        if path.is_empty() {
            Ok(remote.clone())
        } else {
            Ok(format!("{remote}/{path}"))
        }
    }

    /// Whether this backend targets an SFTP remote.
    ///
    /// Used to inject SFTP-specific optimization flags that eliminate
    /// per-file round-trips (modtime set, hash check).
    fn is_sftp(&self) -> bool {
        self.remote.expose_secret().starts_with(":sftp")
    }

    /// Execute an rclone command via the configured shell.
    ///
    /// Uses the configured timeout (`with_timeout` > `VDSL_RCLONE_TIMEOUT` > 300s).
    /// Callers needing a different timeout (e.g. batch) should use `exec_rclone_with_timeout`.
    async fn exec_rclone(&self, args: &[&str]) -> Result<String, InfraError> {
        self.exec_rclone_with_timeout(args, self.timeout_secs).await
    }

    /// Execute an rclone command with an explicit timeout.
    ///
    /// Automatically appends SFTP optimization flags when the remote
    /// is an SFTP target (see [`SFTP_OPTIMIZATION_FLAGS`]).
    async fn exec_rclone_with_timeout(
        &self,
        args: &[&str],
        timeout_secs: u64,
    ) -> Result<String, InfraError> {
        let mut full_args = vec!["rclone"];
        full_args.extend_from_slice(args);
        if self.is_sftp() {
            full_args.extend_from_slice(SFTP_OPTIMIZATION_FLAGS);
        }

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
            });
        }

        Ok(output.stdout)
    }
}

#[async_trait]
impl StorageBackend for RcloneBackend {
    async fn push(&self, local_path: &Path, remote_path: &str) -> Result<(), InfraError> {
        let dest = self.remote_path(remote_path)?;
        let local_str = local_path.to_str().ok_or_else(|| -> InfraError {
            InfraError::Transfer {
                reason: format!(
                    "local path is not valid UTF-8: {}",
                    local_path.to_string_lossy()
                ),
            }
        })?;
        self.exec_rclone(&["copyto", local_str, &dest]).await?;
        Ok(())
    }

    async fn pull(&self, remote_path: &str, local_path: &Path) -> Result<(), InfraError> {
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
        let local_str = local_path.to_str().ok_or_else(|| -> InfraError {
            InfraError::Transfer {
                reason: format!(
                    "local path is not valid UTF-8: {}",
                    local_path.to_string_lossy()
                ),
            }
        })?;
        self.exec_rclone(&["copyto", &src, local_str]).await?;
        Ok(())
    }

    async fn list(&self, remote_path: &str) -> Result<Vec<RemoteFile>, InfraError> {
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
    async fn exists(&self, remote_path: &str) -> Result<bool, InfraError> {
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

    async fn delete(&self, remote_path: &str) -> Result<(), InfraError> {
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

    /// Move a file to an archive location via `rclone moveto`.
    ///
    /// Used for soft-delete on cold storage: the file is relocated (not copied)
    /// to an archive prefix, preserving content with a new path/revision.
    /// `rclone moveto` is atomic at the object-store level for B2/S3.
    async fn archive_move(
        &self,
        src_remote_path: &str,
        archive_remote_path: &str,
    ) -> Result<(), InfraError> {
        let src = self.remote_path(src_remote_path)?;
        let dest = self.remote_path(archive_remote_path)?;
        match self
            .exec_rclone(&["moveto", &src, &dest, "--retries", "1"])
            .await
        {
            Ok(_) => Ok(()),
            Err(e) => {
                let msg = e.to_string();
                // rclone exit 4 (not found) = already absent at src.
                // Archive goal (original absent) is satisfied, though the
                // revision record is missing. Treat as success for idempotence.
                if msg.contains("exit 4") || msg.contains("not found") {
                    tracing::debug!(
                        src = src_remote_path,
                        dest = archive_remote_path,
                        "rclone moveto: src already absent, treating as success"
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
    /// For SFTP remotes, splits into chunks of [`SFTP_BATCH_CHUNK_SIZE`] files
    /// with per-chunk progress logging and retry. Non-SFTP backends run as
    /// a single batch (rclone handles large batches natively for B2/S3).
    ///
    /// Returns per-file Ok/Err.
    async fn push_batch(
        &self,
        src_root: &Path,
        dest_root: &str,
        relative_paths: &[String],
    ) -> HashMap<String, Result<(), InfraError>> {
        if relative_paths.is_empty() {
            return HashMap::new();
        }

        let dest_full = match self.remote_path(dest_root) {
            Ok(d) => d,
            Err(_) => {
                let reason = format!("invalid dest_root for batch push: {dest_root}");
                return Self::all_batch_err(relative_paths, &reason);
            }
        };

        let src_root_str = match src_root.to_str() {
            Some(s) => s.to_string(),
            None => {
                let reason = format!(
                    "src_root is not valid UTF-8: {}",
                    src_root.to_string_lossy()
                );
                return Self::all_batch_err(relative_paths, &reason);
            }
        };

        self.exec_batch_chunked(
            relative_paths,
            "push",
            |chunk, list_filename, sftp_flags, _chunk_timeout| {
                let file_list = chunk.join("\n");
                let src = &src_root_str;
                let dest = &dest_full;
                format!(
                    "cat <<'__VDSL_EOF__' > /tmp/{list_filename}\n\
                     {file_list}\n\
                     __VDSL_EOF__\n\
                     rclone copy {src} {dest} \
                       --files-from /tmp/{list_filename} --transfers 8{sftp_flags}; \
                     _rc=$?; rm -f /tmp/{list_filename}; exit $_rc"
                )
            },
        )
        .await
    }

    /// Batch pull using `rclone copy --files-from`.
    ///
    /// For SFTP remotes, splits into chunks with progress logging and retry.
    async fn pull_batch(
        &self,
        src_root: &str,
        dest_root: &Path,
        relative_paths: &[String],
    ) -> HashMap<String, Result<(), InfraError>> {
        if relative_paths.is_empty() {
            return HashMap::new();
        }

        let src_full = match self.remote_path(src_root) {
            Ok(s) => s,
            Err(_) => {
                let reason = format!("invalid src_root for batch pull: {src_root}");
                return Self::all_batch_err(relative_paths, &reason);
            }
        };

        let dest_root_str = match dest_root.to_str() {
            Some(s) => s.to_string(),
            None => {
                let reason = format!(
                    "dest_root is not valid UTF-8: {}",
                    dest_root.to_string_lossy()
                );
                return Self::all_batch_err(relative_paths, &reason);
            }
        };

        self.exec_batch_chunked(
            relative_paths,
            "pull",
            |chunk, list_filename, sftp_flags, _chunk_timeout| {
                let file_list = chunk.join("\n");
                let src = &src_full;
                let dest = &dest_root_str;
                format!(
                    "cat <<'__VDSL_EOF__' > /tmp/{list_filename}\n\
                     {file_list}\n\
                     __VDSL_EOF__\n\
                     rclone copy {src} {dest} \
                       --files-from /tmp/{list_filename} --transfers 8{sftp_flags}; \
                     _rc=$?; rm -f /tmp/{list_filename}; exit $_rc"
                )
            },
        )
        .await
    }

    /// Batch delete using `rclone delete --files-from`.
    ///
    /// For SFTP remotes, splits into chunks with progress logging and retry.
    async fn delete_batch(
        &self,
        remote_root: &str,
        relative_paths: &[String],
    ) -> HashMap<String, Result<(), InfraError>> {
        if relative_paths.is_empty() {
            return HashMap::new();
        }

        let remote_full = match self.remote_path(remote_root) {
            Ok(r) => r,
            Err(_) => {
                return Self::all_batch_err(
                    relative_paths,
                    &format!("invalid remote_root for batch delete: {remote_root}"),
                );
            }
        };

        self.exec_batch_chunked(
            relative_paths,
            "delete",
            |chunk, list_filename, sftp_flags, _chunk_timeout| {
                let file_list = chunk.join("\n");
                let dest = &remote_full;
                format!(
                    "cat <<'__VDSL_EOF__' > /tmp/{list_filename}\n\
                     {file_list}\n\
                     __VDSL_EOF__\n\
                     rclone delete {dest} \
                       --files-from /tmp/{list_filename} --transfers 8{sftp_flags}; \
                     _rc=$?; rm -f /tmp/{list_filename}; exit $_rc"
                )
            },
        )
        .await
    }

    /// Batch archive-move using `rclone move --files-from`.
    ///
    /// Moves files from `src_root` to `archive_dest_root` preserving relative
    /// paths. Uses the same chunked execution as other batch operations.
    async fn archive_move_batch(
        &self,
        src_root: &str,
        archive_dest_root: &str,
        relative_paths: &[String],
    ) -> HashMap<String, Result<(), InfraError>> {
        if relative_paths.is_empty() {
            return HashMap::new();
        }

        let src_full = match self.remote_path(src_root) {
            Ok(r) => r,
            Err(_) => {
                return Self::all_batch_err(
                    relative_paths,
                    &format!("invalid src_root for batch archive_move: {src_root}"),
                );
            }
        };

        let dest_full = match self.remote_path(archive_dest_root) {
            Ok(r) => r,
            Err(_) => {
                return Self::all_batch_err(
                    relative_paths,
                    &format!(
                        "invalid archive_dest_root for batch archive_move: {archive_dest_root}"
                    ),
                );
            }
        };

        self.exec_batch_chunked(
            relative_paths,
            "archive_move",
            |chunk, list_filename, sftp_flags, _chunk_timeout| {
                let file_list = chunk.join("\n");
                let src = &src_full;
                let dest = &dest_full;
                format!(
                    "cat <<'__VDSL_EOF__' > /tmp/{list_filename}\n\
                     {file_list}\n\
                     __VDSL_EOF__\n\
                     rclone move {src} {dest} \
                       --files-from /tmp/{list_filename} --transfers 8{sftp_flags}; \
                     _rc=$?; rm -f /tmp/{list_filename}; exit $_rc"
                )
            },
        )
        .await
    }

    fn supports_batch(&self) -> bool {
        true
    }

    fn backend_type(&self) -> &str {
        "rclone"
    }

    fn set_progress_callback(&self, callback: Option<ProgressFn>) {
        if let Ok(mut guard) = self.progress.lock() {
            *guard = callback;
        }
    }

    async fn ensure(&self) -> Result<(), InfraError> {
        // Step 1: rclone バイナリの存在確認
        let check = self.shell.exec(&["which", "rclone"], Some(10)).await;
        let rclone_found = matches!(&check, Ok(out) if out.success);

        if !rclone_found {
            // Step 2: インストール試行（.deb直接ダウンロード — unzip依存なし）
            tracing::info!("rclone not found, attempting install via .deb package");
            let install_script = concat!(
                "curl -sL https://downloads.rclone.org/rclone-current-linux-amd64.deb -o /tmp/rclone.deb",
                " && dpkg -i /tmp/rclone.deb",
                " && rm -f /tmp/rclone.deb",
            );
            let install_result = self.shell.exec_script(install_script, Some(120)).await;

            match &install_result {
                Ok(out) if out.success => {
                    tracing::info!("rclone installed successfully via .deb");
                }
                Ok(out) => {
                    // dpkg失敗 → install.sh にフォールバック
                    tracing::debug!(
                        exit_code = out.exit_code,
                        stderr = out.stderr.trim(),
                        "dpkg install failed, falling back to install.sh"
                    );
                    let fallback = self
                        .shell
                        .exec_script("curl -sL https://rclone.org/install.sh | bash", Some(120))
                        .await;
                    match &fallback {
                        Ok(o) if o.success => {
                            tracing::info!("rclone installed successfully via install.sh");
                        }
                        Ok(o) => {
                            return Err(InfraError::Init(format!(
                                "rclone install failed (exit {}): {}",
                                o.exit_code.unwrap_or(-1),
                                o.stderr.trim()
                            )));
                        }
                        Err(e) => {
                            return Err(InfraError::Init(format!(
                                "rclone install.sh exec failed: {e}"
                            )));
                        }
                    }
                }
                Err(e) => {
                    return Err(InfraError::Init(format!(
                        "rclone .deb install exec failed: {e}"
                    )));
                }
            }

            // Step 3: インストール後の再確認
            let recheck = self.shell.exec(&["which", "rclone"], Some(10)).await;
            match &recheck {
                Ok(out) if out.success => {}
                _ => {
                    return Err(InfraError::Init(
                        "rclone still not found after install attempt".to_string(),
                    ));
                }
            }
        }

        // Step 4: 接続テスト（rclone lsf でバケットルートにアクセス）
        let remote = self.remote.expose_secret();
        self.exec_rclone_with_timeout(&["lsf", "--max-depth", "1", remote], 30)
            .await
            .map_err(|e| InfraError::Init(format!("rclone connectivity test failed: {e}")))?;

        Ok(())
    }
}

impl RcloneBackend {
    /// SFTP optimization flags as a space-separated string for shell scripts.
    ///
    /// Returns `" --sftp-set-modtime=false --sftp-disable-hashcheck"` for SFTP,
    /// empty string otherwise.
    fn sftp_flags_for_script(&self) -> &'static str {
        if self.is_sftp() {
            " --sftp-set-modtime=false --sftp-disable-hashcheck"
        } else {
            ""
        }
    }

    /// Chunk size for this backend. SFTP uses small chunks; others run all-at-once.
    fn batch_chunk_size(&self) -> usize {
        if self.is_sftp() {
            SFTP_BATCH_CHUNK_SIZE
        } else {
            usize::MAX // no chunking for non-SFTP
        }
    }

    /// Execute a batch operation in chunks with progress logging and retry.
    ///
    /// `build_script` receives (chunk_paths, list_filename, sftp_flags, chunk_timeout)
    /// and returns the shell script to execute.
    ///
    /// For SFTP: splits into [`SFTP_BATCH_CHUNK_SIZE`] chunks, logs progress
    /// per chunk, retries failed chunks once.
    /// For non-SFTP: runs as a single batch (chunk_size = usize::MAX).
    async fn exec_batch_chunked<F>(
        &self,
        relative_paths: &[String],
        operation: &str,
        build_script: F,
    ) -> HashMap<String, Result<(), InfraError>>
    where
        F: Fn(&[String], &str, &str, u64) -> String,
    {
        let chunk_size = self.batch_chunk_size();
        let sftp_flags = self.sftp_flags_for_script();
        let total = relative_paths.len();
        let chunks: Vec<&[String]> = relative_paths.chunks(chunk_size).collect();
        let num_chunks = chunks.len();

        if num_chunks > 1 {
            tracing::info!(
                operation,
                total,
                num_chunks,
                chunk_size,
                "batch_{operation}: chunked transfer start"
            );
        }

        let mut all_results = HashMap::with_capacity(total);
        let mut completed = 0usize;

        for (i, chunk) in chunks.iter().enumerate() {
            let chunk_num = i + 1;
            let chunk_timeout =
                self.timeout_secs + (chunk.len() as u64 * BATCH_PER_FILE_TIMEOUT_SECS);
            let list_filename =
                format!("vdsl-{operation}-{}.txt", uuid::Uuid::new_v4().as_simple());

            if num_chunks > 1 {
                tracing::info!(
                    operation,
                    chunk = chunk_num,
                    num_chunks,
                    files = chunk.len(),
                    completed,
                    total,
                    "batch_{operation}: chunk start"
                );
            }

            let script = build_script(chunk, &list_filename, sftp_flags, chunk_timeout);

            let mut attempt = 0u32;
            let chunk_result = loop {
                let result = self.shell.exec_script(&script, Some(chunk_timeout)).await;

                match &result {
                    Ok(output) if output.success => break Ok(()),
                    Ok(output) => {
                        let err_msg = format!(
                            "rclone failed (exit {}): {}",
                            output
                                .exit_code
                                .map_or("signal".to_string(), |c| c.to_string()),
                            output.stderr.trim()
                        );
                        if attempt < BATCH_CHUNK_MAX_RETRIES {
                            attempt += 1;
                            tracing::warn!(
                                operation,
                                chunk = chunk_num,
                                attempt,
                                error = %err_msg,
                                "batch_{operation}: chunk failed, retrying"
                            );
                            continue;
                        }
                        break Err(format!("batch {operation} failed: {err_msg}"));
                    }
                    Err(e) => {
                        if attempt < BATCH_CHUNK_MAX_RETRIES {
                            attempt += 1;
                            tracing::warn!(
                                operation,
                                chunk = chunk_num,
                                attempt,
                                error = %e,
                                "batch_{operation}: chunk failed, retrying"
                            );
                            continue;
                        }
                        break Err(format!("batch {operation} failed: {e}"));
                    }
                }
            };

            match chunk_result {
                Ok(()) => {
                    for p in *chunk {
                        all_results.insert(p.clone(), Ok(()));
                    }
                    completed += chunk.len();
                }
                Err(reason) => {
                    for p in *chunk {
                        all_results.insert(
                            p.clone(),
                            Err(InfraError::Transfer {
                                reason: reason.clone(),
                            }),
                        );
                    }
                    // Continue with next chunks — don't abort the entire batch
                    tracing::error!(
                        operation,
                        chunk = chunk_num,
                        failed_files = chunk.len(),
                        reason = %reason,
                        "batch_{operation}: chunk failed after retries, continuing"
                    );
                }
            }

            // Report progress via callback (if set).
            if let Ok(guard) = self.progress.lock() {
                if let Some(cb) = guard.as_ref() {
                    cb(&format!(
                        "{operation}: chunk {chunk_num}/{num_chunks} ({completed}/{total})"
                    ));
                }
            }

            if num_chunks > 1 {
                tracing::info!(
                    operation,
                    chunk = chunk_num,
                    num_chunks,
                    completed,
                    total,
                    "batch_{operation}: chunk done"
                );
            }
        }

        if num_chunks > 1 {
            let failed = total - completed;
            tracing::info!(
                operation,
                total,
                completed,
                failed,
                "batch_{operation}: all chunks done"
            );
        }

        all_results
    }

    /// Helper: build all-error result map for batch operations.
    fn all_batch_err(
        relative_paths: &[String],
        reason: &str,
    ) -> HashMap<String, Result<(), InfraError>> {
        relative_paths
            .iter()
            .map(|p| {
                (
                    p.clone(),
                    Err(InfraError::Transfer {
                        reason: reason.to_string(),
                    }),
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

    #[test]
    fn is_sftp_detection() {
        let sftp = RcloneBackend::new(":sftp,host=1.2.3.4,port=22,user=root:");
        assert!(sftp.is_sftp());
        assert_eq!(
            sftp.sftp_flags_for_script(),
            " --sftp-set-modtime=false --sftp-disable-hashcheck"
        );

        let b2 = RcloneBackend::new(":b2,account=kid,key=k:bucket");
        assert!(!b2.is_sftp());
        assert_eq!(b2.sftp_flags_for_script(), "");
    }

    use chrono::Datelike;
    use chrono::Timelike;
}
