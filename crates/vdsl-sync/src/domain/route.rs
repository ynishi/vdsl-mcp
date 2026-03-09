//! TransferRoute — directed transfer route between two locations.
//!
//! Encapsulates "how to move a file from src to dest", including
//! path resolution for both ends and backend delegation.

use std::path::PathBuf;

use crate::domain::error::SyncError;
use crate::domain::location::LocationId;
use crate::infra::backend::StorageBackend;

/// A directed transfer route between two locations.
///
/// # Path model
///
/// - `src_file_root`: base directory on the src location's host.
///   For local: `/Users/.../output`. For pod: `/workspace/comfyui/output`.
/// - `dest_remote_root`: prefix on the dest location.
///   For cloud: `"vdsl/output"`. For pod: `"workspace/comfyui/output"`.
///
/// Full paths:
/// - src:  `src_file_root / relative_path`
/// - dest: `dest_remote_root / relative_path`
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
    dest_remote_root: String,
    backend: Box<dyn StorageBackend>,
}

impl TransferRoute {
    pub fn new(
        src: LocationId,
        dest: LocationId,
        src_file_root: PathBuf,
        dest_remote_root: String,
        backend: Box<dyn StorageBackend>,
    ) -> Self {
        Self {
            src,
            dest,
            src_file_root,
            dest_remote_root,
            backend,
        }
    }

    pub fn src(&self) -> &LocationId {
        &self.src
    }

    pub fn dest(&self) -> &LocationId {
        &self.dest
    }

    pub fn src_file_root(&self) -> &PathBuf {
        &self.src_file_root
    }

    pub fn dest_remote_root(&self) -> &str {
        &self.dest_remote_root
    }

    /// Transfer a file along this route.
    ///
    /// Resolves both src and dest full paths from the relative path,
    /// validates against path traversal, then delegates to backend.push().
    pub async fn transfer(&self, relative_path: &str) -> Result<(), SyncError> {
        Self::validate_relative_path(relative_path)?;

        let src_path = self.src_file_root.join(relative_path);
        let dest_path = Self::join_remote(&self.dest_remote_root, relative_path);

        self.backend.push(&src_path, &dest_path).await
    }

    /// The backend held by this route (for list/exists/pull operations).
    pub fn backend(&self) -> &dyn StorageBackend {
        self.backend.as_ref()
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

    /// Join a remote root with a relative path.
    ///
    /// Public for use by pull_file() in Phase 1.
    pub fn join_remote(root: &str, relative: &str) -> String {
        let root = root.trim_end_matches('/');
        let rel = relative.trim_start_matches('/');
        if root.is_empty() {
            rel.to_string()
        } else {
            format!("{root}/{rel}")
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn join_remote_normal() {
        assert_eq!(
            TransferRoute::join_remote("vdsl/output", "images/001.png"),
            "vdsl/output/images/001.png"
        );
    }

    #[test]
    fn join_remote_trailing_slash() {
        assert_eq!(
            TransferRoute::join_remote("root/", "file.png"),
            "root/file.png"
        );
    }

    #[test]
    fn join_remote_leading_slash() {
        assert_eq!(
            TransferRoute::join_remote("root", "/file.png"),
            "root/file.png"
        );
    }

    #[test]
    fn join_remote_empty_root() {
        assert_eq!(TransferRoute::join_remote("", "file.png"), "file.png");
    }

    #[test]
    fn join_remote_both_slashes() {
        assert_eq!(
            TransferRoute::join_remote("root/", "/file.png"),
            "root/file.png"
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
