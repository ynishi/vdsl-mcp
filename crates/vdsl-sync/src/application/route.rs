//! TransferRoute — directed transfer route between two locations.
//!
//! Lives in the application layer because it holds infrastructure types
//! ([`StorageBackend`], [`RemoteShell`]). The domain layer only knows
//! about the edge topology via [`RouteGraph`](crate::domain::graph::RouteGraph).
//!
//! Encapsulates "how to move a file from src to dest", including
//! path resolution for both ends and backend delegation.

use std::path::{Path, PathBuf};

use crate::domain::error::SyncError;
use crate::domain::location::LocationId;
use crate::infra::backend::StorageBackend;
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
}

impl TransferRoute {
    /// Create a route with no source shell (local source, push direction).
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
        }
    }

    /// Create a pull-direction route with no source shell.
    ///
    /// Pull direction: `src_file_root` is a rclone remote path prefix,
    /// `dest_file_root` is a local filesystem path. `backend.pull()` is called
    /// instead of `push()`.
    ///
    /// Use for cloud→local or cloud→pod routes where the rclone remote is the source.
    pub fn pull(
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
            direction: TransferDirection::Pull,
        }
    }

    /// Create a route with a source shell for remote source operations.
    ///
    /// The `src_shell` enables file existence checks and hash computation
    /// on the source host (e.g., a GPU pod via SSH).
    pub fn with_src_shell(
        src: LocationId,
        dest: LocationId,
        src_file_root: PathBuf,
        dest_file_root: PathBuf,
        backend: Box<dyn StorageBackend>,
        src_shell: Box<dyn RemoteShell>,
    ) -> Self {
        Self {
            src,
            dest,
            src_file_root,
            dest_file_root,
            backend,
            src_shell: Some(src_shell),
            direction: TransferDirection::Push,
        }
    }

    /// Create a pull-direction route with a source shell.
    ///
    /// Combines pull direction with remote source inspection.
    /// Use for `cloud→pod` where rclone runs on Pod and pulls from B2.
    pub fn pull_with_src_shell(
        src: LocationId,
        dest: LocationId,
        src_file_root: PathBuf,
        dest_file_root: PathBuf,
        backend: Box<dyn StorageBackend>,
        src_shell: Box<dyn RemoteShell>,
    ) -> Self {
        Self {
            src,
            dest,
            src_file_root,
            dest_file_root,
            backend,
            src_shell: Some(src_shell),
            direction: TransferDirection::Pull,
        }
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
                let dest_str = dest_path.to_str().ok_or_else(|| {
                    SyncError::TransferFailed(format!(
                        "dest path is not valid UTF-8: {}",
                        dest_path.to_string_lossy()
                    ))
                })?;
                self.backend.push(&src_path, dest_str).await
            }
            TransferDirection::Pull => {
                let src_str = src_path.to_str().ok_or_else(|| {
                    SyncError::TransferFailed(format!(
                        "src path is not valid UTF-8: {}",
                        src_path.to_string_lossy()
                    ))
                })?;
                self.backend.pull(src_str, &dest_path).await
            }
        }
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
                    .map_err(SyncError::Io)
            }
            Some(shell) => {
                // Remote source: `test -f <path>` via shell
                let path_str = full_path.to_str().ok_or_else(|| {
                    SyncError::TransferFailed(format!(
                        "src path is not valid UTF-8: {}",
                        full_path.to_string_lossy()
                    ))
                })?;
                let output = shell.exec(&["test", "-f", path_str], Some(10)).await?;
                Ok(output.success)
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
                let path_str = full_path.to_str().ok_or_else(|| {
                    SyncError::TransferFailed(format!(
                        "src path is not valid UTF-8: {}",
                        full_path.to_string_lossy()
                    ))
                })?;

                let hash_output = shell.exec(&["sha256sum", path_str], Some(30)).await?;
                if !hash_output.success {
                    return Err(SyncError::Hash(format!(
                        "sha256sum failed on remote: {}",
                        hash_output.stderr.trim()
                    )));
                }
                let file_hash = hash_output
                    .stdout
                    .split_whitespace()
                    .next()
                    .ok_or_else(|| SyncError::Hash("sha256sum returned empty output".into()))?
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
