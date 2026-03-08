//! Abstract sync state persistence.
//!
//! [`SyncStore`] decouples the application layer from specific databases.
//! Default implementation: SQLite (`infra::sqlite` module, feature-gated).

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::domain::entry::SyncEntry;
use crate::domain::error::SyncError;
use crate::domain::file_type::FileType;
use crate::domain::location::{LocationId, LocationState, SyncSummary};

/// Remote endpoint configuration stored in the sync database.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RemoteConfig {
    /// Location identifier.
    pub location_id: LocationId,
    /// Backend type name: "rclone", "comfyui", "ssh_exec", "s3", ...
    pub backend: String,
    /// Root path on the remote (prefix for all relative paths).
    ///
    /// For rclone: e.g. `"vdsl/output"` → remote files at `<remote>/vdsl/output/<relative_path>`.
    /// For S3: e.g. `"my-bucket/prefix"`.
    pub remote_root: String,
    /// Backend-specific configuration (JSON).
    pub config: serde_json::Value,
    /// Registration timestamp.
    pub created_at: DateTime<Utc>,
}

impl RemoteConfig {
    /// Resolve a relative path to a full remote path.
    ///
    /// Rejects path traversal attempts (`..` segments) to prevent
    /// escaping the remote root directory.
    ///
    /// Note: URL-encoded traversal (`%2e%2e`) is not checked here because
    /// downstream backends (rclone, S3) operate on raw path strings
    /// without percent-decoding.
    pub fn resolve_remote_path(&self, relative_path: &str) -> Result<String, SyncError> {
        let rel = relative_path.trim_start_matches('/');
        if rel.split('/').any(|seg| seg == "..") {
            return Err(SyncError::OutsideSyncRoot {
                path: relative_path.to_string(),
            });
        }
        let root = self.remote_root.trim_end_matches('/');
        if root.is_empty() {
            Ok(rel.to_string())
        } else {
            Ok(format!("{root}/{rel}"))
        }
    }
}

/// Abstract sync state persistence.
///
/// Implementations: [`super::sqlite::SqliteSyncStore`] (default), PostgreSQL (future).
#[async_trait]
pub trait SyncStore: Send + Sync {
    // --- Entry CRUD ---

    /// Insert a new sync entry.
    async fn insert_entry(&self, entry: &SyncEntry) -> Result<(), SyncError>;

    /// Update an existing sync entry (by id).
    async fn update_entry(&self, entry: &SyncEntry) -> Result<(), SyncError>;

    /// Get entry by relative path.
    async fn get_by_path(&self, relative_path: &str) -> Result<Option<SyncEntry>, SyncError>;

    /// Find entry with matching identity hash at a different path (duplicate detection).
    ///
    /// Searches `content_hash` first (semantic match). If no content_hash match,
    /// falls back to `file_hash` (byte-exact match).
    async fn find_duplicate(
        &self,
        file_hash: &str,
        content_hash: Option<&str>,
        exclude_path: &str,
    ) -> Result<Option<SyncEntry>, SyncError>;

    /// Delete entry by relative path. Returns true if entry existed.
    async fn delete_entry(&self, relative_path: &str) -> Result<bool, SyncError>;

    // --- Location state ---

    /// Set the sync state for a specific entry at a specific location.
    async fn set_location_state(
        &self,
        entry_id: &str,
        location: &LocationId,
        state: LocationState,
    ) -> Result<(), SyncError>;

    /// Set or clear the error message for an entry.
    async fn set_error(&self, relative_path: &str, err: Option<&str>) -> Result<(), SyncError>;

    /// Set the synced_at timestamp for an entry.
    async fn set_synced_at(&self, relative_path: &str, ts: DateTime<Utc>) -> Result<(), SyncError>;

    // --- Queries ---

    /// List entries pending sync to a destination location.
    async fn pending(&self, dest: &LocationId) -> Result<Vec<SyncEntry>, SyncError>;

    /// List all tracked entries, optionally filtered by type and limited.
    async fn list(
        &self,
        file_type: Option<FileType>,
        limit: Option<usize>,
    ) -> Result<Vec<SyncEntry>, SyncError>;

    /// Get aggregated sync summary across all locations.
    async fn summary(&self) -> Result<SyncSummary, SyncError>;

    /// List entries with sync errors.
    async fn errors(&self) -> Result<Vec<SyncEntry>, SyncError>;

    // --- Remote management ---

    /// Register a remote endpoint.
    async fn register_remote(&self, remote: &RemoteConfig) -> Result<(), SyncError>;

    /// Get a specific remote by location ID.
    async fn get_remote(&self, location_id: &LocationId)
        -> Result<Option<RemoteConfig>, SyncError>;

    /// List all registered remotes.
    async fn list_remotes(&self) -> Result<Vec<RemoteConfig>, SyncError>;

    /// Remove a remote endpoint. Returns true if it existed.
    async fn remove_remote(&self, location_id: &LocationId) -> Result<bool, SyncError>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resolve_remote_path_with_root() {
        let cfg = RemoteConfig {
            location_id: LocationId::new("cloud").unwrap(),
            backend: "rclone".into(),
            remote_root: "vdsl/output".into(),
            config: serde_json::json!({}),
            created_at: Utc::now(),
        };
        assert_eq!(
            cfg.resolve_remote_path("images/001.png").unwrap(),
            "vdsl/output/images/001.png"
        );
    }

    #[test]
    fn resolve_remote_path_empty_root() {
        let cfg = RemoteConfig {
            location_id: LocationId::new("cloud").unwrap(),
            backend: "rclone".into(),
            remote_root: String::new(),
            config: serde_json::json!({}),
            created_at: Utc::now(),
        };
        assert_eq!(
            cfg.resolve_remote_path("images/001.png").unwrap(),
            "images/001.png"
        );
    }

    #[test]
    fn resolve_remote_path_trims_slashes() {
        let cfg = RemoteConfig {
            location_id: LocationId::new("cloud").unwrap(),
            backend: "rclone".into(),
            remote_root: "root/".into(),
            config: serde_json::json!({}),
            created_at: Utc::now(),
        };
        assert_eq!(
            cfg.resolve_remote_path("/leading.png").unwrap(),
            "root/leading.png"
        );
    }

    #[test]
    fn resolve_remote_path_rejects_traversal() {
        let cfg = RemoteConfig {
            location_id: LocationId::new("cloud").unwrap(),
            backend: "rclone".into(),
            remote_root: "root".into(),
            config: serde_json::json!({}),
            created_at: Utc::now(),
        };
        assert!(cfg.resolve_remote_path("../../etc/passwd").is_err());
        assert!(cfg.resolve_remote_path("foo/../bar").is_err());
        assert!(cfg.resolve_remote_path("..").is_err());
        // Single dot and "..." are safe
        assert!(cfg.resolve_remote_path("./valid").is_ok());
        assert!(cfg.resolve_remote_path("a/.../b").is_ok());
    }
}
