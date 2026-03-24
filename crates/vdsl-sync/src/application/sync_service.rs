//! SyncService — application-layer orchestrator for sync operations.
//!
//! Coordinates [`FileStore`], [`TransferStore`], [`RemoteStore`],
//! [`TransferEngine`], and [`ContentHasher`] to provide
//! notify/force/status operations.
//!
//! # v2 architecture
//!
//! - **TrackedFile** — file identity (hash, size, type)
//! - **Transfer** — delivery object with state machine (Queued→InFlight→Completed|Failed)
//! - **FileView** — query result combining TrackedFile + PresenceView per location

use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

use super::route::TransferRoute;
use super::transfer_engine::{BatchResult, TransferEngine};
use crate::domain::error::SyncError;
use crate::domain::file_type::FileType;
use crate::domain::location::{LocationId, LocationSummary, SyncSummary};
use crate::domain::retry::RetryPolicy;
use crate::domain::tracked_file::TrackedFile;
use crate::domain::transfer::{Transfer, TransferState};
use crate::domain::view::{FileView, PresenceState, PresenceView};
use crate::infra::file_store::FileStore;
use crate::infra::hasher::{ContentHasher, Djb2Hasher, HashResult};
use crate::infra::remote_store::RemoteStore;
use crate::infra::store::RemoteConfig;
use crate::infra::transfer_store::TransferStore;

/// スキャン時の個別ファイルエラー。
///
/// スキャンは「1ファイルの失敗で全体を中断しない」設計。
/// 失敗したファイルはScanErrorとして蓄積し、呼び出し元に返す。
#[derive(Debug, Clone, serde::Serialize)]
pub struct ScanError {
    /// 問題のあったファイルパス（absoluteまたはrelative）。
    pub path: String,
    /// エラー内容。
    pub error: String,
}

/// `force()` の戻り値。scan結果とtransfer結果を分離保持する。
///
/// scanフェーズとtransferフェーズは独立したエラー源を持つ。
/// 混合するとデバッグ時にどのフェーズで失敗したか判別できない。
#[derive(Debug, Default, serde::Serialize)]
pub struct ForceResult {
    /// スキャンで新規登録されたファイル数。
    pub scanned: usize,
    /// スキャン時に失敗した個別ファイル。
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub scan_errors: Vec<ScanError>,
    /// transfer実行結果（pushed/failed/errors）。
    #[serde(flatten)]
    pub batch: BatchResult,
}

impl ForceResult {
    /// Serialize to [`serde_json::Value`] for cross-boundary transport.
    pub fn to_value(&self) -> Result<serde_json::Value, SyncError> {
        serde_json::to_value(self)
            .map_err(|e| SyncError::Serialization(format!("ForceResult: {e}")))
    }
}

/// Result of a notify operation.
#[derive(Debug, serde::Serialize)]
pub struct NotifyResult {
    pub file: TrackedFile,
    pub is_duplicate: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub duplicate_of: Option<String>,
    pub transfers_created: usize,
}

impl NotifyResult {
    /// Serialize to [`serde_json::Value`] for cross-boundary transport.
    pub fn to_value(&self) -> Result<serde_json::Value, SyncError> {
        serde_json::to_value(self)
            .map_err(|e| SyncError::Serialization(format!("NotifyResult: {e}")))
    }
}

/// Sync service — application-layer orchestrator.
///
/// Coordinates stores (persistence) + transfer engine (routing) + hasher (identity).
///
/// The local file root is derived from routes via [`TransferEngine::local_root`].
pub struct SyncService {
    file_store: Arc<dyn FileStore>,
    transfer_store: Arc<dyn TransferStore>,
    remote_store: Arc<dyn RemoteStore>,
    engine: TransferEngine,
    hasher: Arc<dyn ContentHasher>,
    retry_policy: RetryPolicy,
}

impl SyncService {
    /// Create a new SyncService.
    pub fn new(
        file_store: Arc<dyn FileStore>,
        transfer_store: Arc<dyn TransferStore>,
        remote_store: Arc<dyn RemoteStore>,
        routes: Vec<TransferRoute>,
    ) -> Self {
        Self {
            file_store,
            transfer_store,
            remote_store,
            engine: TransferEngine::new(routes),
            hasher: Arc::new(Djb2Hasher),
            retry_policy: RetryPolicy::default(),
        }
    }

    /// Create with a custom content hasher.
    pub fn with_hasher(
        file_store: Arc<dyn FileStore>,
        transfer_store: Arc<dyn TransferStore>,
        remote_store: Arc<dyn RemoteStore>,
        routes: Vec<TransferRoute>,
        hasher: Arc<dyn ContentHasher>,
    ) -> Self {
        Self {
            file_store,
            transfer_store,
            remote_store,
            engine: TransferEngine::new(routes),
            hasher,
            retry_policy: RetryPolicy::default(),
        }
    }

    /// Access the retry policy.
    pub fn retry_policy(&self) -> &RetryPolicy {
        &self.retry_policy
    }

    /// Set a custom retry policy.
    pub fn set_retry_policy(&mut self, policy: RetryPolicy) {
        self.retry_policy = policy;
    }

    /// Access the transfer engine (for route management, concurrency settings, etc.).
    pub fn engine(&self) -> &TransferEngine {
        &self.engine
    }

    /// Access the transfer engine mutably.
    pub fn engine_mut(&mut self) -> &mut TransferEngine {
        &mut self.engine
    }

    /// Resolve the local file root from routes.
    pub fn local_root(&self) -> Option<&Path> {
        self.engine.local_root()
    }

    // =========================================================================
    // Path helpers
    // =========================================================================

    /// Convert an absolute local path to a relative path from the local file root.
    ///
    /// Returns `Err(OutsideSyncRoot)` if the path is not under the local root,
    /// or if no local-source route is registered.
    pub fn to_relative(&self, absolute_path: &Path) -> Result<String, SyncError> {
        let local_root = self
            .local_root()
            .ok_or_else(|| SyncError::OutsideSyncRoot {
                path: absolute_path.display().to_string(),
            })?;
        let relative =
            absolute_path
                .strip_prefix(local_root)
                .map_err(|_| SyncError::OutsideSyncRoot {
                    path: absolute_path.display().to_string(),
                })?;
        relative.to_str().map(|s| s.to_string()).ok_or_else(|| {
            SyncError::TransferFailed(format!(
                "relative path is not valid UTF-8: {}",
                relative.to_string_lossy()
            ))
        })
    }

    // =========================================================================
    // Public API: notify / force / get / list / status
    // =========================================================================

    /// Scan sync_root for new/modified files and register them.
    ///
    /// Walks the local sync_root directory, computes hashes, and registers
    /// any files not yet tracked or whose hash has changed.
    ///
    /// 1ファイルの失敗で全体を中断しない。失敗はScanErrorとして蓄積し返す。
    async fn scan_and_register(&self) -> Result<(usize, Vec<ScanError>), SyncError> {
        let local_root = match self.local_root() {
            Some(root) => root.to_path_buf(),
            None => return Ok((0, Vec::new())),
        };

        let mut registered = 0usize;
        let mut errors = Vec::new();
        let mut stack = vec![local_root.clone()];

        while let Some(dir) = stack.pop() {
            let mut entries = tokio::fs::read_dir(&dir).await?;
            while let Some(entry) = entries.next_entry().await? {
                let ft = entry.file_type().await?;
                if ft.is_dir() {
                    stack.push(entry.path());
                    continue;
                }
                if !ft.is_file() {
                    continue;
                }

                let path = entry.path();
                let path_display = path.display().to_string();

                // non-UTF-8パスはエラーとして記録
                let path_str = match path.to_str() {
                    Some(s) => s,
                    None => {
                        errors.push(ScanError {
                            path: path_display,
                            error: "path is not valid UTF-8".into(),
                        });
                        continue;
                    }
                };

                let relative_path = match path.strip_prefix(&local_root) {
                    Ok(rel) => match rel.to_str() {
                        Some(s) => s.to_string(),
                        None => {
                            errors.push(ScanError {
                                path: path_display,
                                error: "relative path is not valid UTF-8".into(),
                            });
                            continue;
                        }
                    },
                    Err(e) => {
                        errors.push(ScanError {
                            path: path_display,
                            error: format!("strip_prefix failed: {e}"),
                        });
                        continue;
                    }
                };

                let (hash_result, _file_size) = match self.inspect_file(&path).await {
                    Ok(v) => v,
                    Err(e) => {
                        errors.push(ScanError {
                            path: path_display,
                            error: format!("hash failed: {e}"),
                        });
                        continue;
                    }
                };

                // Check if already tracked with same hash
                if let Some(existing) = self.file_store.get_file_by_path(&relative_path).await? {
                    if existing.file_hash() == hash_result.file_hash {
                        continue; // unchanged
                    }
                }

                let file_type = path
                    .extension()
                    .and_then(|e| e.to_str())
                    .map(FileType::from_extension)
                    .unwrap_or(FileType::Asset);

                match self.notify(path_str, file_type, None).await {
                    Ok(_) => registered += 1,
                    Err(e) => {
                        errors.push(ScanError {
                            path: path_display,
                            error: format!("notify failed: {e}"),
                        });
                    }
                }
            }
        }

        Ok((registered, errors))
    }

    /// Register a new or modified local file in the sync index (by absolute path).
    ///
    /// 1. Creates/updates TrackedFile
    /// 2. Creates Transfer objects for each direct route from local
    ///
    /// Normally called internally by [`force()`](Self::force) during Dir scan.
    /// Direct use is supported for tests and explicit registration scenarios.
    pub async fn notify(
        &self,
        absolute_path: &str,
        file_type: FileType,
        gen_id: Option<&str>,
    ) -> Result<NotifyResult, SyncError> {
        let path = Path::new(absolute_path);
        self.assert_file_exists(path).await?;

        let relative_path = self.to_relative(path)?;
        let (hash_result, file_size) = self.inspect_file(path).await?;

        // Check for duplicate by hash
        if let Some(existing) = self
            .file_store
            .find_duplicate_file(
                &hash_result.file_hash,
                hash_result.content_hash.as_deref(),
                &relative_path,
            )
            .await?
        {
            return Ok(NotifyResult {
                is_duplicate: true,
                duplicate_of: Some(existing.relative_path().to_string()),
                file: existing,
                transfers_created: 0,
            });
        }

        // Create or update TrackedFile
        let file =
            if let Some(mut existing) = self.file_store.get_file_by_path(&relative_path).await? {
                let hash_changed = existing.update_from_scan(
                    file_type,
                    hash_result.file_hash,
                    hash_result.content_hash,
                    file_size.unwrap_or(0),
                    gen_id.map(|s| s.to_string()),
                );
                self.file_store.upsert_file(&existing).await?;

                if hash_changed {
                    // Hash changed → create new transfers
                    let created = self
                        .create_transfers_from(&existing, &LocationId::local())
                        .await?;
                    return Ok(NotifyResult {
                        file: existing,
                        is_duplicate: false,
                        duplicate_of: None,
                        transfers_created: created,
                    });
                }
                // Hash unchanged but metadata updated
                return Ok(NotifyResult {
                    file: existing,
                    is_duplicate: false,
                    duplicate_of: None,
                    transfers_created: 0,
                });
            } else {
                TrackedFile::from_scan(
                    relative_path,
                    file_type,
                    hash_result.file_hash,
                    hash_result.content_hash,
                    file_size.unwrap_or(0),
                    gen_id.map(|s| s.to_string()),
                )?
            };

        self.file_store.upsert_file(&file).await?;
        let created = self
            .create_transfers_from(&file, &LocationId::local())
            .await?;

        Ok(NotifyResult {
            file,
            is_duplicate: false,
            duplicate_of: None,
            transfers_created: created,
        })
    }

    /// Register a file known to exist on a remote source.
    ///
    /// Creates TrackedFile + Transfer objects from the specified source
    /// to all directly reachable destinations.
    ///
    /// Used when files are produced on remote nodes (e.g., GPU pod generates images)
    /// and we want to sync them to other locations.
    pub async fn register_file(
        &self,
        relative_path: String,
        file_type: FileType,
        file_hash: String,
        content_hash: Option<String>,
        file_size: u64,
        src: &LocationId,
    ) -> Result<TrackedFile, SyncError> {
        let file = TrackedFile::from_scan(
            relative_path,
            file_type,
            file_hash,
            content_hash,
            file_size,
            None,
        )?;
        self.file_store.upsert_file(&file).await?;
        self.create_transfers_from(&file, src).await?;
        Ok(file)
    }

    /// Get aggregated sync status.
    pub async fn status(&self) -> Result<SyncSummary, SyncError> {
        let files = self.file_store.list_files(None, None).await?;
        let total_files = files.len();
        let mut total_errors = 0usize;
        let mut locations: HashMap<LocationId, LocationSummary> = HashMap::new();

        for file in &files {
            let transfers = self
                .transfer_store
                .latest_transfers_by_file(file.id())
                .await?;

            // Track which sources we've already counted for this file
            // to avoid double-counting when multiple transfers share the same src.
            let mut seen_srcs = std::collections::HashSet::new();

            for t in &transfers {
                // Source is implicitly Present — count once per file
                if seen_srcs.insert(t.src().clone()) {
                    let src_summary = locations.entry(t.src().clone()).or_default();
                    src_summary.present = src_summary.present.saturating_add(1);
                }

                // Dest state from Transfer + RetryPolicy
                let dest_summary = locations.entry(t.dest().clone()).or_default();
                let presence = PresenceState::from_transfer(t, &self.retry_policy);
                match presence {
                    PresenceState::Present => {
                        dest_summary.present = dest_summary.present.saturating_add(1);
                    }
                    PresenceState::Pending => {
                        dest_summary.pending = dest_summary.pending.saturating_add(1);
                    }
                    PresenceState::Syncing => {
                        dest_summary.syncing = dest_summary.syncing.saturating_add(1);
                    }
                    PresenceState::Failed => {
                        dest_summary.failed = dest_summary.failed.saturating_add(1);
                        total_errors = total_errors.saturating_add(1);
                    }
                    PresenceState::Absent => {
                        // Absent = Transferが存在しないlocation向け。
                        // from_transfer()からは到達しないが、将来
                        // Transfer未作成locationの集計で使用予定。
                        dest_summary.absent = dest_summary.absent.saturating_add(1);
                    }
                }
            }
        }

        Ok(SyncSummary {
            locations,
            total_entries: total_files,
            total_errors,
        })
    }

    /// Force-sync all pending entries across the entire topology.
    ///
    /// 1. Scans sync_root for new/modified files
    /// 2. Retries failed transfers (transient errors within retry limit)
    /// 3. Executes transfers in BFS order (cloud before pod)
    ///
    /// scan失敗とtransfer失敗は [`ForceResult`] で分離して返す。
    pub async fn force(&self) -> Result<ForceResult, SyncError> {
        let (scanned, scan_errors) = self.scan_and_register().await?;
        self.retry_failed().await?;
        let batch = self
            .engine
            .force(self.file_store.as_ref(), self.transfer_store.as_ref())
            .await?;

        Ok(ForceResult {
            scanned,
            scan_errors,
            batch,
        })
    }

    /// Force-sync queued transfers for a specific route (src → dest).
    pub async fn force_route(
        &self,
        src: &LocationId,
        dest: &LocationId,
    ) -> Result<BatchResult, SyncError> {
        self.engine
            .force_route(
                self.file_store.as_ref(),
                self.transfer_store.as_ref(),
                src,
                dest,
            )
            .await
    }

    /// Get a single file's sync state by relative path.
    ///
    /// Returns a [`FileView`] combining TrackedFile metadata with
    /// PresenceView per location (derived from latest Transfers).
    pub async fn get(&self, relative_path: &str) -> Result<Option<FileView>, SyncError> {
        let file = match self.file_store.get_file_by_path(relative_path).await? {
            Some(f) => f,
            None => return Ok(None),
        };

        let view = self.build_file_view(file).await?;
        Ok(Some(view))
    }

    /// List queued transfers for a destination.
    pub async fn pending(&self, dest: &LocationId) -> Result<Vec<Transfer>, SyncError> {
        self.transfer_store.queued_transfers(dest).await
    }

    /// List all tracked files.
    pub async fn list(
        &self,
        filter: Option<FileType>,
        limit: Option<usize>,
    ) -> Result<Vec<TrackedFile>, SyncError> {
        self.file_store.list_files(filter, limit).await
    }

    /// List failed transfers.
    pub async fn errors(&self) -> Result<Vec<Transfer>, SyncError> {
        self.transfer_store.failed_transfers().await
    }

    /// Register a remote endpoint in the store.
    pub async fn register_remote(&self, config: &RemoteConfig) -> Result<(), SyncError> {
        self.remote_store.register_remote(config).await
    }

    /// Remove a remote endpoint from the store and all associated routes.
    pub async fn remove_remote(&mut self, location: &LocationId) -> Result<(), SyncError> {
        self.remote_store.remove_remote(location).await?;
        self.engine.remove_routes_for(location);
        Ok(())
    }

    /// List registered remotes.
    pub async fn list_remotes(&self) -> Result<Vec<RemoteConfig>, SyncError> {
        self.remote_store.list_remotes().await
    }

    /// Register a location: persist remote config + add routes atomically.
    pub async fn register_location(
        &mut self,
        config: &RemoteConfig,
        routes: Vec<TransferRoute>,
    ) -> Result<(), SyncError> {
        self.remote_store.register_remote(config).await?;
        for route in routes {
            self.engine.add_route(route);
        }
        Ok(())
    }

    // =========================================================================
    // Infrastructure helpers
    // =========================================================================

    /// Retry failed transfers that are retryable per the retry policy.
    ///
    /// Creates new Queued transfers for each Failed transfer that:
    /// - Has a Transient error kind
    /// - Has not exceeded max_attempts
    ///
    /// Returns the number of transfers retried.
    async fn retry_failed(&self) -> Result<usize, SyncError> {
        let failed = self.transfer_store.failed_transfers().await?;
        let mut retried = 0usize;

        for t in &failed {
            if t.is_retryable(&self.retry_policy) {
                let new_transfer = t.retry()?;
                self.transfer_store.insert_transfer(&new_transfer).await?;
                retried += 1;
            }
        }

        Ok(retried)
    }

    /// Assert that a file exists, returning a typed error.
    async fn assert_file_exists(&self, path: &Path) -> Result<(), SyncError> {
        match tokio::fs::try_exists(path).await {
            Ok(true) => Ok(()),
            Ok(false) => Err(SyncError::FileNotFound(path.to_path_buf())),
            Err(e) => Err(SyncError::Io(e)),
        }
    }

    /// Compute file hashes and size on a blocking thread.
    async fn inspect_file(&self, path: &Path) -> Result<(HashResult, Option<u64>), SyncError> {
        let hasher = Arc::clone(&self.hasher);
        let hash_path = path.to_path_buf();
        let hash_result = tokio::task::spawn_blocking(move || hasher.hash_file(&hash_path))
            .await
            .map_err(|e| SyncError::Hash(format!("spawn_blocking join failed: {e}")))??;
        let file_size = Some(tokio::fs::metadata(path).await?.len());
        Ok((hash_result, file_size))
    }

    /// Create Transfer objects for all direct routes from `origin`.
    ///
    /// For chain routing (local→cloud→pod): only creates the direct
    /// Transfer (local→cloud). Next-hop (cloud→pod) is created by
    /// TransferEngine on completion.
    async fn create_transfers_from(
        &self,
        file: &TrackedFile,
        origin: &LocationId,
    ) -> Result<usize, SyncError> {
        let graph = self.engine.graph();
        let direct_dests = graph.direct_from(origin);
        let mut created = 0usize;

        for dest in direct_dests {
            if self.engine.find_route(origin, dest).is_some() {
                let transfer = Transfer::new(file.id().to_string(), origin.clone(), dest.clone())?;
                self.transfer_store.insert_transfer(&transfer).await?;
                created += 1;
            }
        }

        Ok(created)
    }

    /// Build a FileView from a TrackedFile by querying latest Transfers.
    async fn build_file_view(&self, file: TrackedFile) -> Result<FileView, SyncError> {
        let transfers = self
            .transfer_store
            .latest_transfers_by_file(file.id())
            .await?;

        let mut presences = Vec::new();
        let mut seen_sources = std::collections::HashSet::new();

        for t in &transfers {
            // Source location is implicitly Present
            if seen_sources.insert(t.src().clone()) {
                presences.push(PresenceView {
                    location: t.src().clone(),
                    state: PresenceState::Present,
                    error: None,
                    synced_at: None,
                    attempt: 0,
                });
            }

            // Dest location state from Transfer + RetryPolicy
            presences.push(PresenceView {
                location: t.dest().clone(),
                state: PresenceState::from_transfer(t, &self.retry_policy),
                error: t.error().map(|s| s.to_string()),
                synced_at: t
                    .finished_at()
                    .filter(|_| t.state() == TransferState::Completed),
                attempt: t.attempt(),
            });
        }

        Ok(FileView { file, presences })
    }
}

// =============================================================================
// SyncServiceBuilder
// =============================================================================

/// Builder for [`SyncService`] with automatic remote registration.
///
/// Collects routes and remote configs, then builds the service in one step.
/// Remote configs are registered in the store during `build()`.
pub struct SyncServiceBuilder {
    file_store: Arc<dyn FileStore>,
    transfer_store: Arc<dyn TransferStore>,
    remote_store: Arc<dyn RemoteStore>,
    routes: Vec<TransferRoute>,
    remotes: Vec<RemoteConfig>,
    hasher: Option<Arc<dyn ContentHasher>>,
    retry_policy: Option<RetryPolicy>,
}

impl SyncServiceBuilder {
    /// Start building a SyncService with the given stores.
    pub fn new(
        file_store: Arc<dyn FileStore>,
        transfer_store: Arc<dyn TransferStore>,
        remote_store: Arc<dyn RemoteStore>,
    ) -> Self {
        Self {
            file_store,
            transfer_store,
            remote_store,
            routes: Vec::new(),
            remotes: Vec::new(),
            hasher: None,
            retry_policy: None,
        }
    }

    /// Add a transfer route.
    pub fn route(mut self, route: TransferRoute) -> Self {
        self.routes.push(route);
        self
    }

    /// Add multiple transfer routes.
    pub fn routes(mut self, routes: impl IntoIterator<Item = TransferRoute>) -> Self {
        self.routes.extend(routes);
        self
    }

    /// Register a remote endpoint (persisted to store on `build()`).
    pub fn remote(mut self, config: RemoteConfig) -> Self {
        self.remotes.push(config);
        self
    }

    /// Set a custom content hasher (default: Djb2Hasher).
    pub fn hasher(mut self, hasher: Arc<dyn ContentHasher>) -> Self {
        self.hasher = Some(hasher);
        self
    }

    /// Set a custom retry policy (default: 3 attempts).
    pub fn retry_policy(mut self, policy: RetryPolicy) -> Self {
        self.retry_policy = Some(policy);
        self
    }

    /// Build the SyncService, registering all remotes in the store.
    pub async fn build(self) -> Result<SyncService, SyncError> {
        // Register all remotes (idempotent)
        for remote in &self.remotes {
            self.remote_store.register_remote(remote).await?;
        }

        let mut service = if let Some(hasher) = self.hasher {
            SyncService::with_hasher(
                self.file_store,
                self.transfer_store,
                self.remote_store,
                self.routes,
                hasher,
            )
        } else {
            SyncService::new(
                self.file_store,
                self.transfer_store,
                self.remote_store,
                self.routes,
            )
        };

        if let Some(policy) = self.retry_policy {
            service.set_retry_policy(policy);
        }

        Ok(service)
    }
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::infra::backend::memory::InMemoryBackend;
    use crate::infra::backend::StorageBackend;
    use std::path::PathBuf;
    use std::sync::Arc;

    #[cfg(feature = "sqlite")]
    use crate::infra::sqlite::SqliteSyncStore;

    // Wrapper to make Arc<InMemoryBackend> implement StorageBackend
    #[async_trait::async_trait]
    impl StorageBackend for Arc<InMemoryBackend> {
        async fn push(&self, local_path: &Path, remote_path: &str) -> Result<(), SyncError> {
            (**self).push(local_path, remote_path).await
        }
        async fn pull(&self, remote_path: &str, local_path: &Path) -> Result<(), SyncError> {
            (**self).pull(remote_path, local_path).await
        }
        async fn list(
            &self,
            remote_path: &str,
        ) -> Result<Vec<crate::infra::backend::RemoteFile>, SyncError> {
            (**self).list(remote_path).await
        }
        async fn exists(&self, remote_path: &str) -> Result<bool, SyncError> {
            (**self).exists(remote_path).await
        }
        fn backend_type(&self) -> &str {
            (**self).backend_type()
        }
    }

    #[cfg(feature = "sqlite")]
    async fn test_service_with_dir(dir: &Path) -> (SyncService, Arc<InMemoryBackend>) {
        let store = SqliteSyncStore::open_in_memory().await.unwrap();
        let cloud_backend = Arc::new(InMemoryBackend::default());

        // Register "cloud" as a remote
        RemoteStore::register_remote(
            &store,
            &RemoteConfig {
                location_id: LocationId::new("cloud").unwrap(),
                backend: "memory".into(),
                config: serde_json::json!({}),
                created_at: chrono::Utc::now(),
            },
        )
        .await
        .unwrap();

        // Create route: local → cloud
        let routes = vec![TransferRoute::new(
            LocationId::local(),
            LocationId::new("cloud").unwrap(),
            dir.to_path_buf(),
            PathBuf::from("remote/output"),
            Box::new(Arc::clone(&cloud_backend)),
        )];

        let store = Arc::new(store);
        let service = SyncService::new(
            store.clone() as Arc<dyn FileStore>,
            store.clone() as Arc<dyn TransferStore>,
            store.clone() as Arc<dyn RemoteStore>,
            routes,
        );
        (service, cloud_backend)
    }

    #[cfg(feature = "sqlite")]
    #[tokio::test]
    async fn notify_new_file() {
        let dir = tempfile::tempdir().unwrap();
        let (service, _backend) = test_service_with_dir(dir.path()).await;

        let path = dir.path().join("test.json");
        std::fs::write(&path, b"{}").unwrap();

        let result = service
            .notify(path.to_str().unwrap(), FileType::Recipe, Some("gen-1"))
            .await
            .unwrap();

        assert!(!result.is_duplicate);
        assert_eq!(result.file.file_type(), FileType::Recipe);
        assert_eq!(result.file.relative_path(), "test.json");
        assert_eq!(result.transfers_created, 1); // local → cloud

        // Verify via get()
        let view = service.get("test.json").await.unwrap().unwrap();
        assert_eq!(
            view.presence_state(&LocationId::local()),
            Some(PresenceState::Present)
        );
        assert_eq!(
            view.presence_state(&LocationId::new("cloud").unwrap()),
            Some(PresenceState::Pending)
        );
    }

    #[cfg(feature = "sqlite")]
    #[tokio::test]
    async fn notify_nonexistent_file() {
        let dir = tempfile::tempdir().unwrap();
        let (service, _) = test_service_with_dir(dir.path()).await;
        let result = service
            .notify("/no/such/file.png", FileType::Image, None)
            .await;
        assert!(matches!(result, Err(SyncError::FileNotFound(_))));
    }

    #[cfg(feature = "sqlite")]
    #[tokio::test]
    async fn notify_outside_root() {
        let dir = tempfile::tempdir().unwrap();
        let other_dir = tempfile::tempdir().unwrap();
        let (service, _) = test_service_with_dir(dir.path()).await;

        let outside = other_dir.path().join("outside.json");
        std::fs::write(&outside, b"{}").unwrap();

        let result = service
            .notify(outside.to_str().unwrap(), FileType::Asset, None)
            .await;
        assert!(matches!(result, Err(SyncError::OutsideSyncRoot { .. })));
    }

    #[cfg(feature = "sqlite")]
    #[tokio::test]
    async fn force_uses_route_transfer() {
        let dir = tempfile::tempdir().unwrap();
        let (service, backend) = test_service_with_dir(dir.path()).await;

        let path = dir.path().join("push.json");
        std::fs::write(&path, b"data").unwrap();

        service
            .notify(path.to_str().unwrap(), FileType::Asset, None)
            .await
            .unwrap();

        let cloud = LocationId::new("cloud").unwrap();
        let result = service.force().await.unwrap();
        assert_eq!(result.batch.pushed, 1);
        assert_eq!(result.batch.failed, 0);
        assert!(result.scan_errors.is_empty());

        // Verify backend received the dest_file_root-prefixed path
        let log = backend.log.lock().await;
        assert_eq!(log.len(), 1);
        match &log[0] {
            crate::infra::backend::memory::Op::Push { remote, .. } => {
                assert_eq!(remote, "remote/output/push.json");
            }
            _ => panic!("expected Push op"),
        }

        // Verify state via FileView
        let view = service.get("push.json").await.unwrap().unwrap();
        assert_eq!(view.presence_state(&cloud), Some(PresenceState::Present));
    }

    #[cfg(feature = "sqlite")]
    #[tokio::test]
    async fn force_failure_records_error() {
        let dir = tempfile::tempdir().unwrap();
        let (service, backend) = test_service_with_dir(dir.path()).await;

        let path = dir.path().join("fail.json");
        std::fs::write(&path, b"data").unwrap();

        service
            .notify(path.to_str().unwrap(), FileType::Asset, None)
            .await
            .unwrap();

        // Set backend to fail
        *backend.fail_next.lock().await = true;

        let result = service.force().await.unwrap();
        assert_eq!(result.batch.failed, 1);
        assert_eq!(result.batch.pushed, 0);

        // Transfer should be Failed
        let errors = service.errors().await.unwrap();
        assert_eq!(errors.len(), 1);
        assert!(errors[0].error().is_some());
    }

    #[cfg(feature = "sqlite")]
    #[tokio::test]
    async fn status_summary() {
        let dir = tempfile::tempdir().unwrap();
        let (service, _) = test_service_with_dir(dir.path()).await;

        for (name, content) in &[("a.json", &b"data_a"[..]), ("b.json", &b"data_b"[..])] {
            let p = dir.path().join(name);
            std::fs::write(&p, content).unwrap();
            service
                .notify(p.to_str().unwrap(), FileType::Asset, None)
                .await
                .unwrap();
        }

        let summary = service.status().await.unwrap();
        assert_eq!(summary.total_entries, 2);

        let local = summary.locations.get(&LocationId::local()).unwrap();
        assert_eq!(local.present, 2);

        let cloud = summary
            .locations
            .get(&LocationId::new("cloud").unwrap())
            .unwrap();
        assert_eq!(cloud.pending, 2);
    }

    #[cfg(feature = "sqlite")]
    #[tokio::test]
    async fn to_relative_and_back() {
        let dir = tempfile::tempdir().unwrap();
        let (service, _) = test_service_with_dir(dir.path()).await;

        let abs = dir.path().join("sub/dir/file.png");
        let rel = service.to_relative(&abs).unwrap();
        assert_eq!(rel, "sub/dir/file.png");

        let back = service.local_root().unwrap().join(&rel);
        assert_eq!(back, abs);
    }

    /// Build a service with pod→cloud route (remote source).
    #[cfg(feature = "sqlite")]
    async fn test_service_with_remote_source(
        _dir: &Path,
        mock_shell: Box<dyn crate::infra::shell::RemoteShell>,
    ) -> (SyncService, Arc<InMemoryBackend>) {
        let store = SqliteSyncStore::open_in_memory().await.unwrap();
        let cloud_backend = Arc::new(InMemoryBackend::default());

        // Register remotes
        for id in &["pod", "cloud"] {
            RemoteStore::register_remote(
                &store,
                &RemoteConfig {
                    location_id: LocationId::new(*id).unwrap(),
                    backend: "memory".into(),
                    config: serde_json::json!({}),
                    created_at: chrono::Utc::now(),
                },
            )
            .await
            .unwrap();
        }

        // Route: pod → cloud (remote source with shell)
        let routes = vec![TransferRoute::with_src_shell(
            LocationId::new("pod").unwrap(),
            LocationId::new("cloud").unwrap(),
            PathBuf::from("/workspace/output"),
            PathBuf::from("vdsl/output"),
            Box::new(Arc::clone(&cloud_backend)),
            mock_shell,
        )];

        let store = Arc::new(store);
        let service = SyncService::new(
            store.clone() as Arc<dyn FileStore>,
            store.clone() as Arc<dyn TransferStore>,
            store.clone() as Arc<dyn RemoteStore>,
            routes,
        );
        (service, cloud_backend)
    }

    #[cfg(feature = "sqlite")]
    #[tokio::test]
    async fn force_remote_source_success() {
        use crate::infra::shell::mock::MockShell;

        let dir = tempfile::tempdir().unwrap();
        let shell = MockShell::new(vec!["/workspace/output/gen-001.png"]);
        let (service, backend) = test_service_with_remote_source(dir.path(), Box::new(shell)).await;

        // Register file from pod with transfer pod→cloud
        let file = service
            .register_file(
                "gen-001.png".into(),
                FileType::Image,
                "hash_remote".into(),
                None,
                1024,
                &LocationId::new("pod").unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(file.relative_path(), "gen-001.png");

        // Force push to cloud — should use pod→cloud route
        let result = service.force().await.unwrap();
        assert_eq!(result.batch.pushed, 1);
        assert_eq!(result.batch.failed, 0);

        // Verify backend received push
        let log = backend.log.lock().await;
        assert_eq!(log.len(), 1);
        match &log[0] {
            crate::infra::backend::memory::Op::Push { remote, .. } => {
                assert_eq!(remote, "vdsl/output/gen-001.png");
            }
            _ => panic!("expected Push op"),
        }

        // Verify state via FileView
        let view = service.get("gen-001.png").await.unwrap().unwrap();
        assert_eq!(
            view.presence_state(&LocationId::new("cloud").unwrap()),
            Some(PresenceState::Present)
        );
    }

    #[cfg(feature = "sqlite")]
    #[tokio::test]
    async fn force_remote_source_file_not_found() {
        use crate::infra::shell::mock::MockShell;

        let dir = tempfile::tempdir().unwrap();
        let shell = MockShell::new(Vec::<String>::new());
        let (service, _backend) =
            test_service_with_remote_source(dir.path(), Box::new(shell)).await;

        // Register file from pod with transfer pod→cloud
        service
            .register_file(
                "missing.png".into(),
                FileType::Image,
                "hash_missing".into(),
                None,
                512,
                &LocationId::new("pod").unwrap(),
            )
            .await
            .unwrap();

        // Force push — file not found on pod, should fail
        let result = service.force().await.unwrap();
        assert_eq!(result.batch.pushed, 0);
        assert_eq!(result.batch.failed, 1);
        assert!(result.batch.errors[0]
            .error
            .contains("source file not found"));
    }
}
