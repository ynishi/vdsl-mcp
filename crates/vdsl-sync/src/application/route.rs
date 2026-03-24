//! TransferRoute — directed transfer route between two locations.
//!
//! Lives in the application layer because it holds infrastructure types
//! ([`StorageBackend`], [`RemoteShell`]). The domain layer only knows
//! about the topology via [`Topology`](crate::domain::plan::Topology).
//!
//! Encapsulates "how to move a file from src to dest", including
//! path resolution for both ends and backend delegation.

use std::path::{Path, PathBuf};

use std::collections::HashMap;

use crate::application::error::SyncError;
use crate::domain::location::LocationId;
use crate::infra::backend::StorageBackend;
use crate::infra::error::InfraError;
use crate::infra::shell::RemoteShell;

/// A file discovered during source-side scan.
#[derive(Debug, Clone)]
pub struct SrcFile {
    /// Relative path from `src_file_root`.
    pub relative_path: String,
    /// File size in bytes (when available).
    pub size: Option<u64>,
    /// Last modification time (when available from backend metadata).
    pub modified_at: Option<chrono::DateTime<chrono::Utc>>,
}

/// Direction of file transfer relative to the rclone remote.
///
/// - `Push`: src is a "local" filesystem path, dest is a rclone remote path.
///   `backend.push(src_path, dest_path)` → `rclone copyto <local> <remote>`
///
/// - `Pull`: src is a rclone remote path, dest is a "local" filesystem path.
///   `backend.pull(src_path, dest_path)` → `rclone copyto <remote> <local>`
///
/// "Local" here means local to the host running rclone — which may be a Pod
/// if the backend uses a `RemoteShell`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum TransferDirection {
    #[default]
    Push,
    Pull,
}

/// A directed transfer route between two locations.
///
/// # Path model
///
/// - `src_file_root`: base directory on the src location's host.
///   For local: `/Users/.../output`. For pod: `/workspace/comfyui/output`.
/// - `dest_file_root`: base directory on the dest location's host.
///   For cloud: `"vdsl/output"`. For local: `/Users/.../output`.
///
/// Full paths:
/// - src:  `src_file_root / relative_path`
/// - dest: `dest_file_root / relative_path`
///
/// # Source shell
///
/// `src_shell` enables operations on the source host: file existence checks,
/// hash computation, etc. For local sources this is `None` (use filesystem).
/// For remote sources (pod, NAS), provide a `RemoteShell` implementation.
///
/// # Backend responsibility
///
/// `backend.push(src_full_path, dest_full_path)` is called.
/// The Backend internally uses a `RemoteShell` to determine
/// WHERE the command (e.g. rclone) runs.
pub struct TransferRoute {
    src: LocationId,
    dest: LocationId,
    src_file_root: PathBuf,
    dest_file_root: PathBuf,
    backend: Box<dyn StorageBackend>,
    /// Shell for source-side file operations (existence check, hash).
    /// `None` when src is local (use filesystem directly).
    src_shell: Option<Box<dyn RemoteShell>>,
    direction: TransferDirection,
    /// Estimated transfer time per GB for this route.
    /// Used by the topology resolver to prefer cheaper routes.
    /// Default: 1.0 (neutral).
    time_per_gb: f64,
    /// Static priority (lower = preferred when costs are equal).
    /// Default: 100 (neutral).
    priority: u32,
}

impl TransferRoute {
    /// Create a push-direction route (default).
    ///
    /// Chain `.direction()` and `.with_src_shell()` for pull-direction or
    /// remote-source routes:
    ///
    /// ```ignore
    /// // Push, local source (default)
    /// TransferRoute::new(src, dest, src_root, dest_root, backend)
    ///
    /// // Pull direction
    /// TransferRoute::new(src, dest, src_root, dest_root, backend)
    ///     .direction(TransferDirection::Pull)
    ///
    /// // Remote source with shell
    /// TransferRoute::new(src, dest, src_root, dest_root, backend)
    ///     .with_src_shell(shell)
    ///
    /// // Pull + remote source
    /// TransferRoute::new(src, dest, src_root, dest_root, backend)
    ///     .direction(TransferDirection::Pull)
    ///     .with_src_shell(shell)
    /// ```
    pub fn new(
        src: LocationId,
        dest: LocationId,
        src_file_root: PathBuf,
        dest_file_root: PathBuf,
        backend: Box<dyn StorageBackend>,
    ) -> Self {
        Self {
            src,
            dest,
            src_file_root,
            dest_file_root,
            backend,
            src_shell: None,
            direction: TransferDirection::Push,
            time_per_gb: 1.0,
            priority: 100,
        }
    }

    /// Set the transfer direction (default: Push).
    ///
    /// Pull direction: `backend.pull()` is called instead of `push()`.
    /// Use for cloud→local or cloud→pod routes where the rclone remote
    /// is the source.
    pub fn direction(mut self, direction: TransferDirection) -> Self {
        self.direction = direction;
        self
    }

    /// Set the source shell for remote source operations.
    ///
    /// Enables file existence checks and hash computation on the source
    /// host (e.g., a GPU pod via SSH). Without a shell, the source is
    /// assumed to be locally accessible.
    pub fn with_src_shell(mut self, shell: Box<dyn RemoteShell>) -> Self {
        self.src_shell = Some(shell);
        self
    }

    /// Set the transfer cost properties for this route.
    ///
    /// `time_per_gb`: estimated seconds per GB (lower = cheaper, preferred).
    /// `priority`: static tiebreaker (lower = preferred when costs are equal).
    ///
    /// Used internally by the topology resolver to compute optimal transfer
    /// trees. Routes with higher cost are used only when cheaper paths are
    /// unavailable.
    pub fn with_cost(mut self, time_per_gb: f64, priority: u32) -> Self {
        self.time_per_gb = time_per_gb;
        self.priority = priority;
        self
    }

    /// Estimated transfer time per GB.
    pub fn time_per_gb(&self) -> f64 {
        self.time_per_gb
    }

    /// Static priority (lower = preferred).
    pub fn priority(&self) -> u32 {
        self.priority
    }

    pub fn src(&self) -> &LocationId {
        &self.src
    }

    pub fn dest(&self) -> &LocationId {
        &self.dest
    }

    pub fn src_file_root(&self) -> &Path {
        &self.src_file_root
    }

    pub fn dest_file_root(&self) -> &Path {
        &self.dest_file_root
    }

    /// Whether this route is a pull-direction transfer.
    ///
    /// Pull routes have a remote (e.g. rclone) source that cannot be checked
    /// via local filesystem. Source file existence is guaranteed by the
    /// preceding push transfer's Completed state.
    pub fn is_pull(&self) -> bool {
        self.direction == TransferDirection::Pull
    }

    /// Whether this route has a source shell for remote operations.
    ///
    /// When false and the route is Pull direction, the source is Cloud storage
    /// where per-file hash computation requires downloading. In this case,
    /// metadata-based change detection (size comparison) should be used instead.
    pub fn has_src_shell(&self) -> bool {
        self.src_shell.is_some()
    }

    /// Whether the source is Cloud storage (Pull + no shell).
    ///
    /// Cloud sources cannot compute content hashes without downloading files.
    /// Use metadata-based change detection (size, mtime) instead.
    pub fn is_cloud_source(&self) -> bool {
        self.is_pull() && !self.has_src_shell()
    }

    /// Transfer a file along this route.
    ///
    /// Resolves both src and dest full paths from the relative path,
    /// validates against path traversal, then delegates to the backend.
    ///
    /// - Push direction: `backend.push(src_local_path, dest_remote_str)`
    /// - Pull direction: `backend.pull(src_remote_str, dest_local_path)`
    pub async fn transfer(&self, relative_path: &str) -> Result<(), SyncError> {
        Self::validate_relative_path(relative_path)?;

        let src_path = self.src_file_root.join(relative_path);
        let dest_path = Self::safe_join(&self.dest_file_root, relative_path);

        match self.direction {
            TransferDirection::Push => {
                let dest_str = dest_path.to_str().ok_or_else(|| -> SyncError {
                    InfraError::Transfer {
                        reason: format!(
                            "dest path is not valid UTF-8: {}",
                            dest_path.to_string_lossy()
                        ),
                    }
                    .into()
                })?;
                self.backend.push(&src_path, dest_str).await
            }
            TransferDirection::Pull => {
                let src_str = src_path.to_str().ok_or_else(|| -> SyncError {
                    InfraError::Transfer {
                        reason: format!(
                            "src path is not valid UTF-8: {}",
                            src_path.to_string_lossy()
                        ),
                    }
                    .into()
                })?;
                self.backend.pull(src_str, &dest_path).await
            }
        }
    }

    /// Delete a file at the destination of this route.
    ///
    /// For push-direction routes, deletes from the remote (dest_file_root).
    /// For pull-direction routes, deletes from the local dest.
    pub async fn delete(&self, relative_path: &str) -> Result<(), SyncError> {
        Self::validate_relative_path(relative_path)?;
        let dest_path = Self::safe_join(&self.dest_file_root, relative_path);

        match self.direction {
            TransferDirection::Push => {
                // dest is remote — use backend.delete
                let dest_str = dest_path.to_str().ok_or_else(|| -> SyncError {
                    InfraError::Transfer {
                        reason: format!(
                            "dest path is not valid UTF-8: {}",
                            dest_path.to_string_lossy()
                        ),
                    }
                    .into()
                })?;
                self.backend.delete(dest_str).await
            }
            TransferDirection::Pull => {
                // dest is local — remove from filesystem
                match tokio::fs::remove_file(&dest_path).await {
                    Ok(()) => Ok(()),
                    Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
                    Err(e) => Err(SyncError::from(e)),
                }
            }
        }
    }

    /// Batch transfer multiple files along this route in a single operation.
    ///
    /// Uses `backend.push_batch()` with `--files-from` for rclone backends.
    /// Only supports Push direction (Sync transfers). Pull and Delete
    /// operations should use individual `transfer()` / `delete()` calls.
    ///
    /// Returns per-file Ok/Err results keyed by relative path.
    pub async fn transfer_batch(
        &self,
        relative_paths: &[String],
    ) -> HashMap<String, Result<(), SyncError>> {
        if relative_paths.is_empty() {
            return HashMap::new();
        }

        // Validate all paths first
        for rel in relative_paths {
            if Self::validate_relative_path(rel).is_err() {
                return relative_paths
                    .iter()
                    .map(|p| {
                        (
                            p.clone(),
                            Err(SyncError::OutsideSyncRoot { path: p.clone() }),
                        )
                    })
                    .collect();
            }
        }

        match self.direction {
            TransferDirection::Push => {
                let dest_root_str = self.dest_file_root.to_str().unwrap_or_default();
                self.backend
                    .push_batch(&self.src_file_root, dest_root_str, relative_paths)
                    .await
            }
            TransferDirection::Pull => {
                let src_root_str = self.src_file_root.to_str().unwrap_or_default();
                self.backend
                    .pull_batch(src_root_str, &self.dest_file_root, relative_paths)
                    .await
            }
        }
    }

    /// Whether the backend supports efficient batch operations.
    pub fn supports_batch(&self) -> bool {
        self.backend.supports_batch()
    }

    /// The backend held by this route (for list/exists/pull operations).
    pub fn backend(&self) -> &dyn StorageBackend {
        self.backend.as_ref()
    }

    /// The source shell, if this route has a remote source.
    pub fn src_shell(&self) -> Option<&dyn RemoteShell> {
        self.src_shell.as_deref()
    }

    /// Check whether the source file exists for this route.
    ///
    /// - Local source: uses `tokio::fs::try_exists`
    /// - Remote source: uses `src_shell` to run `test -f <path>`
    ///
    /// Returns `Ok(true)` if file exists, `Ok(false)` if not.
    pub async fn src_file_exists(&self, relative_path: &str) -> Result<bool, SyncError> {
        Self::validate_relative_path(relative_path)?;
        let full_path = self.src_file_root.join(relative_path);

        match &self.src_shell {
            None => {
                // Local source: filesystem check
                tokio::fs::try_exists(&full_path)
                    .await
                    .map_err(SyncError::from)
            }
            Some(shell) => {
                // Remote source: `test -f <path>` via shell
                let path_str = full_path.to_str().ok_or_else(|| -> SyncError {
                    InfraError::Transfer {
                        reason: format!(
                            "src path is not valid UTF-8: {}",
                            full_path.to_string_lossy()
                        ),
                    }
                    .into()
                })?;
                let output = shell.exec(&["test", "-f", path_str], Some(10)).await?;
                Ok(output.success)
            }
        }
    }

    /// Check whether the destination file exists for this route.
    ///
    /// - Push (dest = cloud): uses `backend.exists()`
    /// - Pull (dest = local): uses `tokio::fs::try_exists`
    ///
    /// Returns `Ok(true)` if file exists, `Ok(false)` if not.
    pub async fn dest_file_exists(&self, relative_path: &str) -> Result<bool, SyncError> {
        Self::validate_relative_path(relative_path)?;
        let dest_path = Self::safe_join(&self.dest_file_root, relative_path);

        match self.direction {
            TransferDirection::Push => {
                // Dest is cloud — use backend
                let dest_str = dest_path.to_str().ok_or_else(|| -> SyncError {
                    InfraError::Transfer {
                        reason: format!(
                            "dest path is not valid UTF-8: {}",
                            dest_path.to_string_lossy()
                        ),
                    }
                    .into()
                })?;
                self.backend.exists(dest_str).await
            }
            TransferDirection::Pull => {
                // Dest is local — filesystem check
                tokio::fs::try_exists(&dest_path)
                    .await
                    .map_err(SyncError::from)
            }
        }
    }

    /// Inspect a source file: compute hash and size via RemoteShell.
    ///
    /// - Local source: delegates to the provided `local_hasher`
    /// - Remote source: uses `sha256sum` + `stat --format=%s` (GNU coreutils) via shell
    ///
    /// Returns `(file_hash, file_size)`. `content_hash` is not available
    /// for remote files (would require PNG parsing on remote host).
    ///
    /// # Platform assumption
    ///
    /// `stat --format=%s` is GNU coreutils syntax (Linux).
    /// BSD `stat` uses `-f%z` instead. RunPod containers use Linux,
    /// so this is safe for the current use case. If BSD support is
    /// needed, detect the platform or use `wc -c < file` as fallback.
    ///
    /// # WARNING: Hash algorithm mismatch
    ///
    /// Local files are hashed with DJB2 (via `ContentHasher`), while remote
    /// files use SHA-256 (via `sha256sum`). This means **the same file will
    /// produce different `file_hash` values** depending on whether it was
    /// registered locally (`notify()`) or remotely (`notify_remote()`).
    ///
    /// Consequence: `find_duplicate()` cannot detect cross-origin duplicates.
    /// A file notified on pod and the same file notified locally will be
    /// treated as two distinct entries.
    ///
    /// Future fix: unify hash algorithm (e.g. SHA-256 everywhere) or store
    /// algorithm identifier alongside the hash for cross-comparison.
    pub async fn inspect_src_file(
        &self,
        relative_path: &str,
        local_hasher: &dyn crate::infra::hasher::ContentHasher,
    ) -> Result<(crate::infra::hasher::HashResult, Option<u64>), SyncError> {
        Self::validate_relative_path(relative_path)?;
        let full_path = self.src_file_root.join(relative_path);

        match &self.src_shell {
            None => {
                // Local: use ContentHasher (DJB2 + PNG semantic hash)
                let result = local_hasher.hash_file(&full_path)?;
                let size = tokio::fs::metadata(&full_path).await.map(|m| m.len()).ok();
                Ok((result, size))
            }
            Some(shell) => {
                // Remote: sha256sum for file_hash, stat for size
                let path_str = full_path.to_str().ok_or_else(|| -> SyncError {
                    InfraError::Transfer {
                        reason: format!(
                            "src path is not valid UTF-8: {}",
                            full_path.to_string_lossy()
                        ),
                    }
                    .into()
                })?;

                let hash_output = shell.exec(&["sha256sum", path_str], Some(30)).await?;
                if !hash_output.success {
                    return Err(InfraError::Hash {
                        op: "remote",
                        reason: format!(
                            "sha256sum failed on remote: {}",
                            hash_output.stderr.trim()
                        ),
                    }
                    .into());
                }
                let file_hash = hash_output
                    .stdout
                    .split_whitespace()
                    .next()
                    .ok_or_else(|| -> SyncError {
                        InfraError::Hash {
                            op: "remote",
                            reason: "sha256sum returned empty output".into(),
                        }
                        .into()
                    })?
                    .to_string();

                // stat for file size (GNU format)
                let stat_output = shell
                    .exec(&["stat", "--format=%s", path_str], Some(10))
                    .await?;
                let file_size = if stat_output.success {
                    stat_output.stdout.trim().parse::<u64>().ok()
                } else {
                    None
                };

                Ok((
                    crate::infra::hasher::HashResult {
                        file_hash,
                        content_hash: None, // PNG semantic hash not available remotely
                    },
                    file_size,
                ))
            }
        }
    }

    /// List all files at the source location of this route.
    ///
    /// Returns relative paths from `src_file_root`.
    ///
    /// - Push + no src_shell → local filesystem recursive scan
    /// - Push + src_shell → `find <src_file_root> -type f` via shell
    /// - Pull → `backend.list(src_file_root)` (source is on the remote/cloud side)
    pub async fn list_src_files(&self) -> Result<Vec<SrcFile>, SyncError> {
        match self.direction {
            TransferDirection::Pull => {
                // Source is on the remote/cloud side — use backend.list()
                let root_str = self.src_file_root.to_str().ok_or_else(|| -> SyncError {
                    InfraError::Transfer {
                        reason: format!(
                            "src_file_root is not valid UTF-8: {}",
                            self.src_file_root.to_string_lossy()
                        ),
                    }
                    .into()
                })?;
                let remote_files = self.backend.list(root_str).await?;
                Ok(remote_files
                    .into_iter()
                    .map(|rf| SrcFile {
                        relative_path: rf.path,
                        size: rf.size,
                        modified_at: rf.modified_at,
                    })
                    .collect())
            }
            TransferDirection::Push => match &self.src_shell {
                None => {
                    // Local filesystem recursive scan
                    self.list_local_files().await
                }
                Some(shell) => {
                    // Remote host: `find <root> -type f` via shell
                    let root_str = self.src_file_root.to_str().ok_or_else(|| -> SyncError {
                        InfraError::Transfer {
                            reason: format!(
                                "src_file_root is not valid UTF-8: {}",
                                self.src_file_root.to_string_lossy()
                            ),
                        }
                        .into()
                    })?;
                    let output = shell
                        .exec(
                            &["find", root_str, "-type", "f", "-printf", "%P\\n"],
                            Some(60),
                        )
                        .await?;
                    if !output.success {
                        return Err(SyncError::from(InfraError::Transfer {
                            reason: format!(
                                "remote find failed (exit={:?}): {}",
                                output.exit_code,
                                output.stderr.trim()
                            ),
                        }));
                    }
                    Ok(output
                        .stdout
                        .lines()
                        .filter(|l| !l.is_empty())
                        .map(|l| SrcFile {
                            relative_path: l.to_string(),
                            size: None, // size resolved later via inspect_src_file
                            modified_at: None,
                        })
                        .collect())
                }
            },
        }
    }

    /// Recursive local directory listing (relative paths from src_file_root).
    ///
    /// Resilient: individual entry failures (permission denied, broken symlinks,
    /// FUSE mount errors) are logged and skipped — they never abort the scan.
    /// Only the initial `read_dir(root)` failure propagates as `Err`.
    async fn list_local_files(&self) -> Result<Vec<SrcFile>, SyncError> {
        let root = &self.src_file_root;
        if !root.is_dir() {
            return Ok(Vec::new());
        }

        let mut result = Vec::new();
        let mut stack = vec![root.to_path_buf()];

        while let Some(dir) = stack.pop() {
            let mut read_dir = match tokio::fs::read_dir(&dir).await {
                Ok(rd) => rd,
                Err(e) => {
                    // Skip unreadable subdirectories (permission denied, etc.)
                    // but propagate root-level failure.
                    if dir == *root {
                        return Err(SyncError::from(e));
                    }
                    tracing::warn!(
                        dir = %dir.display(),
                        error = %e,
                        "skipping unreadable directory during scan"
                    );
                    continue;
                }
            };
            loop {
                let entry = match read_dir.next_entry().await {
                    Ok(Some(e)) => e,
                    Ok(None) => break,
                    Err(e) => {
                        tracing::warn!(
                            dir = %dir.display(),
                            error = %e,
                            "skipping entry due to read error"
                        );
                        continue;
                    }
                };
                let ft = match entry.file_type().await {
                    Ok(ft) => ft,
                    Err(e) => {
                        tracing::warn!(
                            path = %entry.path().display(),
                            error = %e,
                            "skipping entry: file_type failed"
                        );
                        continue;
                    }
                };
                // Symlinks: follow to resolve actual type.
                // DirEntry::file_type() does NOT follow symlinks on Unix,
                // so symlinks appear as is_symlink()=true, is_file()=false.
                // We follow them via metadata() to get the target type.
                let (effective_ft, is_symlink) = if ft.is_symlink() {
                    match tokio::fs::metadata(entry.path()).await {
                        Ok(meta) => (meta.file_type(), true),
                        Err(e) => {
                            // Broken symlink — target doesn't exist
                            tracing::debug!(
                                path = %entry.path().display(),
                                error = %e,
                                "skipping broken symlink"
                            );
                            continue;
                        }
                    }
                } else {
                    (ft, false)
                };

                if effective_ft.is_dir() {
                    if is_symlink {
                        // Symlink → directory: skip to avoid infinite loops
                        // (e.g. workspace → ../repo creates cycles).
                        tracing::debug!(
                            path = %entry.path().display(),
                            "skipping symlink to directory"
                        );
                    } else {
                        stack.push(entry.path());
                    }
                } else if effective_ft.is_file() {
                    if let Ok(rel) = entry.path().strip_prefix(root) {
                        if let Some(s) = rel.to_str() {
                            let meta = tokio::fs::metadata(entry.path()).await.ok();
                            // Truncate to seconds: SQLite stores timestamps
                            // with SecondsFormat::Secs, so sub-second precision
                            // would cause incremental scan mtime mismatches.
                            let modified_at = meta.as_ref().and_then(|m| {
                                m.modified().ok().map(|st| {
                                    let dt = chrono::DateTime::<chrono::Utc>::from(st);
                                    chrono::DateTime::from_timestamp(dt.timestamp(), 0)
                                        .unwrap_or(dt)
                                })
                            });
                            result.push(SrcFile {
                                relative_path: s.to_string(),
                                size: meta.map(|m| m.len()),
                                modified_at,
                            });
                        }
                    }
                }
                // sockets, fifos, etc. — silently skipped
            }
        }

        Ok(result)
    }

    // --- internal helpers ---

    fn validate_relative_path(path: &str) -> Result<(), SyncError> {
        let path = path.trim_start_matches('/');
        if path.split('/').any(|seg| seg == "..") {
            return Err(SyncError::OutsideSyncRoot {
                path: path.to_string(),
            });
        }
        Ok(())
    }

    /// Safely join a root path with a relative path.
    ///
    /// Trims leading `/` from the relative part to prevent `PathBuf::join`
    /// from replacing the root entirely (Unix absolute path behaviour).
    pub(crate) fn safe_join(root: &Path, relative: &str) -> PathBuf {
        root.join(relative.trim_start_matches('/'))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn safe_join_normal() {
        assert_eq!(
            TransferRoute::safe_join(Path::new("vdsl/output"), "images/001.png"),
            PathBuf::from("vdsl/output/images/001.png")
        );
    }

    #[test]
    fn safe_join_trailing_slash() {
        assert_eq!(
            TransferRoute::safe_join(Path::new("root/"), "file.png"),
            PathBuf::from("root/file.png")
        );
    }

    #[test]
    fn safe_join_leading_slash() {
        assert_eq!(
            TransferRoute::safe_join(Path::new("root"), "/file.png"),
            PathBuf::from("root/file.png")
        );
    }

    #[test]
    fn safe_join_empty_root() {
        assert_eq!(
            TransferRoute::safe_join(Path::new(""), "file.png"),
            PathBuf::from("file.png")
        );
    }

    #[test]
    fn safe_join_both_slashes() {
        assert_eq!(
            TransferRoute::safe_join(Path::new("root/"), "/file.png"),
            PathBuf::from("root/file.png")
        );
    }

    #[test]
    fn validate_rejects_traversal() {
        assert!(TransferRoute::validate_relative_path("../../etc/passwd").is_err());
        assert!(TransferRoute::validate_relative_path("foo/../bar").is_err());
        assert!(TransferRoute::validate_relative_path("..").is_err());
    }

    #[test]
    fn validate_allows_safe_paths() {
        assert!(TransferRoute::validate_relative_path("images/001.png").is_ok());
        assert!(TransferRoute::validate_relative_path("./valid").is_ok());
        assert!(TransferRoute::validate_relative_path("a/.../b").is_ok());
        assert!(TransferRoute::validate_relative_path("").is_ok());
    }
}
