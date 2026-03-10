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
///
/// Path resolution (remote root) is handled by [`TransferRoute`](crate::domain::route::TransferRoute),
/// not by this struct. `RemoteConfig` is purely for persistence metadata.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RemoteConfig {
    /// Location identifier.
    pub location_id: LocationId,
    /// Backend type name: "rclone", "comfyui", "ssh_exec", "s3", ...
    pub backend: String,
    /// Backend-specific configuration (JSON).
    pub config: serde_json::Value,
    /// Registration timestamp.
    pub created_at: DateTime<Utc>,
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
