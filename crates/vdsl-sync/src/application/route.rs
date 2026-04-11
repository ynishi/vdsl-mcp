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
    /// Archive root for soft-delete (Push direction only).
    ///
    /// When set, `delete()` / `delete_batch()` call
    /// `backend.archive_move()` to move the target file to
    /// `{archive_root}/{ISO8601_ts}/{relative_path}` instead of
    /// hard-deleting it. Used for cold-storage routes where deleted
    /// file revisions must be preserved.
    archive_root: Option<PathBuf>,
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
            archive_root: None,
        }
    }

    /// Enable archive-on-delete for this route (Push direction only).
    ///
    /// Deleted files are moved to `{archive_root}/{ISO8601_ts}/{relative_path}`
    /// instead of being hard-deleted. The backend must implement `archive_move`.
    pub fn with_archive_root(mut self, archive_root: PathBuf) -> Self {
        self.archive_root = Some(archive_root);
        self
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

    /// Whether this route is a pull-direction transfer.
    ///
    /// Pull routes have a remote (e.g. rclone) source that cannot be checked
    /// via local filesystem. Source file existence is guaranteed by the
    /// preceding push transfer's Completed state.
    pub fn is_pull(&self) -> bool {
        self.direction == TransferDirection::Pull
    }

    /// Archive root for soft-delete (None if archive is disabled).
    pub fn archive_root(&self) -> Option<&Path> {
        self.archive_root.as_deref()
    }

    /// Restore a soft-deleted file from archive back to its original location.
    ///
    /// Reverses `delete()`'s archive_move:
    /// `{archive_root}/{revision}/{relative_path}` → `{dest_file_root}/{relative_path}`.
    ///
    /// Push direction + archive_root must be set; otherwise returns an error.
    pub async fn restore_from_archive(
        &self,
        relative_path: &str,
        revision: &str,
    ) -> Result<(), SyncError> {
        Self::validate_relative_path(relative_path)?;
        let archive_root = self.archive_root.as_ref().ok_or_else(|| -> SyncError {
            InfraError::Transfer {
                reason: format!(
                    "restore_from_archive: route has no archive_root (src={}, dest={})",
                    self.src, self.dest
                ),
            }
            .into()
        })?;
        if self.direction != TransferDirection::Push {
            return Err(InfraError::Transfer {
                reason: "restore_from_archive: only Push routes are supported".into(),
            }
            .into());
        }
        let archive_full = archive_root.join(revision).join(relative_path);
        let dest_full = Self::safe_join(&self.dest_file_root, relative_path);
        let archive_str = archive_full.to_str().ok_or_else(|| -> SyncError {
            InfraError::Transfer {
                reason: format!("archive path not valid UTF-8: {}", archive_full.display()),
            }
            .into()
        })?;
        let dest_str = dest_full.to_str().ok_or_else(|| -> SyncError {
            InfraError::Transfer {
                reason: format!("dest path not valid UTF-8: {}", dest_full.display()),
            }
            .into()
        })?;
        tracing::info!(
            archive = archive_str,
            dest = dest_str,
            "route::restore_from_archive: moveto reverse"
        );
        self.backend
            .archive_move(archive_str, dest_str)
            .await
            .map_err(Into::into)
    }

    /// Access the underlying storage backend.
    pub(crate) fn backend(&self) -> &dyn StorageBackend {
        &*self.backend
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
                self.backend
                    .push(&src_path, dest_str)
                    .await
                    .map_err(Into::into)
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
                self.backend
                    .pull(src_str, &dest_path)
                    .await
                    .map_err(Into::into)
            }
        }
    }

    /// Build the archive remote path for a given relative_path.
    ///
    /// Format: `{archive_root}/{ISO8601_ts}/{relative_path}`
    /// Timestamp is UTC in compact basic form (e.g. `20260408T134500Z`).
    fn build_archive_path(&self, relative_path: &str) -> Option<String> {
        let archive_root = self.archive_root.as_ref()?;
        let ts = chrono::Utc::now().format("%Y%m%dT%H%M%SZ").to_string();
        let archive_dest = archive_root.join(&ts).join(relative_path);
        Some(archive_dest.to_string_lossy().into_owned())
    }

    /// Delete a file at the destination of this route.
    ///
    /// For push-direction routes, deletes from the remote (dest_file_root).
    /// For pull-direction routes, deletes from the local dest.
    /// When `archive_root` is set (Push only), deleted files are moved
    /// to `{archive_root}/{ISO8601_ts}/{relative_path}` instead of being
    /// hard-deleted.
    pub async fn delete(&self, relative_path: &str) -> Result<(), SyncError> {
        Self::validate_relative_path(relative_path)?;
        let dest_path = Self::safe_join(&self.dest_file_root, relative_path);

        match self.direction {
            TransferDirection::Push => {
                // dest is remote — use backend.delete or archive_move
                let dest_str = dest_path.to_str().ok_or_else(|| -> SyncError {
                    InfraError::Transfer {
                        reason: format!(
                            "dest path is not valid UTF-8: {}",
                            dest_path.to_string_lossy()
                        ),
                    }
                    .into()
                })?;
                if let Some(archive_dest) = self.build_archive_path(relative_path) {
                    tracing::debug!(
                        src = dest_str,
                        archive = %archive_dest,
                        "route::delete: archive_move (soft-delete)"
                    );
                    return self
                        .backend
                        .archive_move(dest_str, &archive_dest)
                        .await
                        .map_err(Into::into);
                }
                self.backend.delete(dest_str).await.map_err(Into::into)
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
    /// For sync transfers only. Delete transfers use `delete_batch()`.
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
                    .into_iter()
                    .map(|(k, v)| (k, v.map_err(Into::into)))
                    .collect()
            }
            TransferDirection::Pull => {
                let src_root_str = self.src_file_root.to_str().unwrap_or_default();
                self.backend
                    .pull_batch(src_root_str, &self.dest_file_root, relative_paths)
                    .await
                    .into_iter()
                    .map(|(k, v)| (k, v.map_err(Into::into)))
                    .collect()
            }
        }
    }

    /// Batch delete multiple files along this route in a single operation.
    ///
    /// Uses `backend.delete_batch()` with `rclone delete --files-from`.
    /// For push-direction routes, deletes from dest (remote).
    /// For pull-direction routes, deletes from dest (local) — falls back to individual.
    ///
    /// Returns per-file Ok/Err results keyed by relative path.
    pub async fn delete_batch(
        &self,
        relative_paths: &[String],
    ) -> HashMap<String, Result<(), SyncError>> {
        if relative_paths.is_empty() {
            return HashMap::new();
        }

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
                if let Some(archive_root) = &self.archive_root {
                    // Archive mode: batch move to {archive_root}/{ts}/
                    let ts = chrono::Utc::now().format("%Y%m%dT%H%M%SZ").to_string();
                    let dest_root_str = self.dest_file_root.to_str().unwrap_or_default();
                    let archive_dest = archive_root.join(&ts);
                    let archive_dest_str = archive_dest.to_string_lossy().into_owned();
                    tracing::debug!(
                        src = dest_root_str,
                        archive = %archive_dest_str,
                        count = relative_paths.len(),
                        "route::delete_batch: archive_move_batch (soft-delete)"
                    );
                    return self
                        .backend
                        .archive_move_batch(dest_root_str, &archive_dest_str, relative_paths)
                        .await
                        .into_iter()
                        .map(|(k, v)| (k, v.map_err(Into::into)))
                        .collect();
                }
                let dest_root_str = self.dest_file_root.to_str().unwrap_or_default();
                self.backend
                    .delete_batch(dest_root_str, relative_paths)
                    .await
                    .into_iter()
                    .map(|(k, v)| (k, v.map_err(Into::into)))
                    .collect()
            }
            TransferDirection::Pull => {
                // Pull direction: dest is local filesystem — delete individually
                let mut results = HashMap::with_capacity(relative_paths.len());
                for rel in relative_paths {
                    let result = self.delete(rel).await;
                    results.insert(rel.clone(), result);
                }
                results
            }
        }
    }

    /// Whether the backend supports efficient batch operations.
    pub fn supports_batch(&self) -> bool {
        self.backend.supports_batch()
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

    // --- internal helpers ---

    fn validate_relative_path(path: &str) -> Result<(), SyncError> {
        let path = path.trim_start_matches('/');
        if path.split('/').any(|seg| seg == "..") {
            return Err(SyncError::OutsideSyncRoot {
                path: path.to_string(),
            });
        }
        // Reject control characters (newline, CR, tab, NUL, ...). The rclone
        // batch backend writes file lists into a shell heredoc; a literal
        // newline in a relative path could terminate the heredoc and inject
        // arbitrary shell commands.
        if path.chars().any(|c| c.is_control()) {
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

    #[test]
    fn validate_rejects_control_chars() {
        // Newline could break out of rclone batch heredoc → shell injection.
        assert!(TransferRoute::validate_relative_path("evil\n__VDSL_EOF__\nrm -rf ~\n.png").is_err());
        assert!(TransferRoute::validate_relative_path("foo\nbar.png").is_err());
        assert!(TransferRoute::validate_relative_path("foo\rbar.png").is_err());
        assert!(TransferRoute::validate_relative_path("foo\tbar.png").is_err());
        assert!(TransferRoute::validate_relative_path("foo\0bar.png").is_err());
    }
}
