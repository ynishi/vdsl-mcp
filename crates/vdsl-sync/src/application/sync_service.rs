//! SyncService — application-layer orchestrator for sync operations.
//!
//! Coordinates [`SyncStore`], [`TransferRoute`]s, and [`ContentHasher`]
//! to provide register/notify/push/pull/force operations.
//!
//! # Path model
//!
//! All entries are stored with **relative paths** from `local_file_root`.
//! Each route encapsulates src/dest path resolution via `TransferRoute`.
//!
//! # Route-based architecture (Phase 1)
//!
//! Transfer operations use `Vec<TransferRoute>` instead of
//! `HashMap<LocationId, StorageBackend>`. Each route is a directed edge
//! (src → dest) with its own backend and path roots.
//!
//! `notify()` remains Local-only in Phase 1 (`local_file_root` based).
//! Phase 2 will add RemoteShell-based hash computation for non-local notify.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use futures::stream::{self, StreamExt};

use crate::domain::entry::SyncEntry;
use crate::domain::error::SyncError;
use crate::domain::file_type::FileType;
use crate::domain::location::{LocationId, LocationState, SyncSummary};
use crate::domain::route::TransferRoute;
use crate::infra::hasher::{ContentHasher, Djb2Hasher, HashResult};
use crate::infra::store::{RemoteConfig, SyncStore};

/// Result of a notify operation.
#[derive(Debug)]
pub struct NotifyResult {
    pub entry: SyncEntry,
    pub is_duplicate: bool,
    pub duplicate_of: Option<String>,
}

/// Options for registering a file.
#[derive(Debug)]
pub struct RegisterOpts {
    pub file_hash: String,
    pub content_hash: Option<String>,
    pub file_size: Option<u64>,
    pub gen_id: Option<String>,
    pub initial_locations: HashMap<LocationId, LocationState>,
}

/// Result of a register operation.
#[derive(Debug)]
pub enum RegisterResult {
    Created(SyncEntry),
    Updated(SyncEntry),
    Duplicate {
        existing: SyncEntry,
        duplicate_of: String,
    },
}

/// Result of a batch push operation.
#[derive(Debug, Default)]
pub struct BatchResult {
    pub pushed: usize,
    pub failed: usize,
    pub errors: Vec<(String, String)>,
}

/// Sync service — application-layer orchestrator.
///
/// Coordinates store (persistence) + routes (transfer) + hasher (identity).
///
/// `local_file_root` is the base directory for local file operations
/// (notify, inspect, to_relative). Phase 1 only — Phase 2 will remove
/// this field when RemoteShell-based hash computation is implemented.
pub struct SyncService {
    store: Box<dyn SyncStore>,
    routes: Vec<TransferRoute>,
    hasher: Arc<dyn ContentHasher>,
    force_concurrency: usize,
    /// Local file root for notify/inspect operations (Phase 1).
    /// Phase 2 で廃止予定 — RemoteShell 経由のハッシュ計算に移行後。
    local_file_root: PathBuf,
}

impl SyncService {
    /// Default maximum number of concurrent push operations per target.
    const DEFAULT_FORCE_CONCURRENCY: usize = 8;

    /// Create a new SyncService.
    ///
    /// `local_file_root` is required for Phase 1 notify/inspect operations.
    /// Phase 2 will remove this parameter.
    pub fn new(
        local_file_root: PathBuf,
        store: Box<dyn SyncStore>,
        routes: Vec<TransferRoute>,
    ) -> Self {
        Self {
            local_file_root,
            store,
            routes,
            hasher: Arc::new(Djb2Hasher),
            force_concurrency: Self::DEFAULT_FORCE_CONCURRENCY,
        }
    }

    /// Create with a custom content hasher.
    pub fn with_hasher(
        local_file_root: PathBuf,
        store: Box<dyn SyncStore>,
        routes: Vec<TransferRoute>,
        hasher: Arc<dyn ContentHasher>,
    ) -> Self {
        Self {
            local_file_root,
            store,
            routes,
            hasher,
            force_concurrency: Self::DEFAULT_FORCE_CONCURRENCY,
        }
    }

    /// Set the maximum number of concurrent push operations in `force()`.
    ///
    /// Clamped to minimum 1 — `buffer_unordered(0)` would deadlock the stream.
    pub fn set_force_concurrency(&mut self, n: usize) {
        self.force_concurrency = n.max(1);
    }

    /// Add a route at runtime.
    pub fn add_route(&mut self, route: TransferRoute) {
        self.routes.push(route);
    }

    /// Remove all routes targeting a specific destination.
    pub fn remove_routes_for(&mut self, dest: &LocationId) {
        self.routes.retain(|r| r.dest() != dest);
    }

    /// The local file root (Phase 1 — for notify/inspect).
    pub fn local_file_root(&self) -> &Path {
        &self.local_file_root
    }

    // =========================================================================
    // Path helpers
    // =========================================================================

    /// Convert an absolute local path to a relative path from `local_file_root`.
    ///
    /// Returns `Err(OutsideSyncRoot)` if the path is not under `local_file_root`.
    pub fn to_relative(&self, absolute_path: &Path) -> Result<String, SyncError> {
        absolute_path
            .strip_prefix(&self.local_file_root)
            .map(|p| p.to_string_lossy().to_string())
            .map_err(|_| SyncError::OutsideSyncRoot {
                path: absolute_path.display().to_string(),
            })
    }

    // =========================================================================
    // Route lookup
    // =========================================================================

    /// Find a route from src to dest.
    fn find_route(&self, src: &LocationId, dest: &LocationId) -> Option<&TransferRoute> {
        self.routes
            .iter()
            .find(|r| r.src() == src && r.dest() == dest)
    }

    /// Find a route for transferring an entry to the given destination.
    ///
    /// Searches entry.locations for a src that is Present,
    /// then finds a matching route (src, dest) in self.routes.
    ///
    /// Source selection priority:
    /// 1. Local (lowest latency, most reliable file existence check)
    /// 2. Any other Present location with a matching route
    fn find_route_for_entry(
        &self,
        entry: &SyncEntry,
        dest: &LocationId,
    ) -> Option<&TransferRoute> {
        // Priority 1: local → dest
        if entry.location_state(&LocationId::local()) == LocationState::Present {
            if let Some(route) = self.find_route(&LocationId::local(), dest) {
                return Some(route);
            }
        }

        // Priority 2: any other Present src → dest
        for (loc, state) in &entry.locations {
            if loc.is_local() || *state != LocationState::Present {
                continue;
            }
            if let Some(route) = self.find_route(loc, dest) {
                return Some(route);
            }
        }

        None
    }

    /// Collect unique destination LocationIds from all registered routes.
    fn route_destinations(&self) -> Vec<LocationId> {
        let mut dests: Vec<LocationId> = self
            .routes
            .iter()
            .map(|r| r.dest().clone())
            .collect();
        dests.sort();
        dests.dedup();
        dests
    }

    // =========================================================================
    // Lua thin IF: notify / status / force
    // =========================================================================

    /// Notify the service of a new or modified file (by absolute path).
    ///
    /// Phase 1: Local-only. Auto-computes hash, checks for duplicates,
    /// and marks all configured remotes as pending.
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
            .store
            .find_duplicate(
                &hash_result.file_hash,
                hash_result.content_hash.as_deref(),
                &relative_path,
            )
            .await?
        {
            return Ok(NotifyResult {
                is_duplicate: true,
                duplicate_of: Some(existing.relative_path.clone()),
                entry: existing,
            });
        }

        // Build initial locations via domain factory
        let remotes = self.store.list_remotes().await?;
        let remote_locs: Vec<LocationId> = remotes.iter().map(|r| r.location_id.clone()).collect();

        // Check existing entry and delegate to domain
        if let Some(mut existing) = self.store.get_by_path(&relative_path).await? {
            existing.update_metadata(
                file_type,
                hash_result.file_hash,
                hash_result.content_hash,
                file_size,
                gen_id.map(|s| s.to_string()),
            );
            self.store.update_entry(&existing).await?;
            return Ok(NotifyResult {
                entry: existing,
                is_duplicate: false,
                duplicate_of: None,
            });
        }

        // New entry via domain factory
        let entry = SyncEntry::new(
            relative_path,
            file_type,
            hash_result.file_hash,
            hash_result.content_hash,
            file_size,
            gen_id.map(|s| s.to_string()),
            &remote_locs,
        );
        self.store.insert_entry(&entry).await?;
        Ok(NotifyResult {
            entry,
            is_duplicate: false,
            duplicate_of: None,
        })
    }

    /// Get aggregated sync status.
    pub async fn status(&self) -> Result<SyncSummary, SyncError> {
        self.store.summary().await
    }

    /// Force-sync all pending files to a destination (or all route targets if None).
    ///
    /// Uses route-based source selection: for each pending entry, finds a
    /// Present source location with a matching route to the destination.
    ///
    /// # Source selection priority
    ///
    /// 1. Local (lowest latency, TOCTOU-safe file existence check)
    /// 2. Any other Present location with a matching route
    ///
    /// # TOCTOU (Time-of-check to time-of-use)
    ///
    /// When src is local, file existence is checked before transfer.
    /// If the file was deleted, the entry is marked as Absent.
    /// Phase 2 will add RemoteShell-based existence checks for non-local sources.
    pub async fn force(&self, dest: Option<&LocationId>) -> Result<BatchResult, SyncError> {
        let mut result = BatchResult::default();

        let targets: Vec<LocationId> = if let Some(d) = dest {
            vec![d.clone()]
        } else {
            self.route_destinations()
        };

        for target in &targets {
            let pending = self.store.pending(target).await?;

            let outcomes: Vec<_> = stream::iter(pending.into_iter().map(|entry| async move {
                // --- Source selection ---
                let route = self.find_route_for_entry(&entry, target);

                let route = match route {
                    Some(r) => r,
                    None => {
                        // No Present src with a matching route.
                        // Phase 1 fallback: check if local file was deleted.
                        let local_path = self.local_file_root.join(&entry.relative_path);
                        if !local_path.exists() {
                            if let Err(e) = self
                                .store
                                .set_location_state(
                                    &entry.id,
                                    &LocationId::local(),
                                    LocationState::Absent,
                                )
                                .await
                            {
                                tracing::error!(
                                    entry_id = %entry.id,
                                    error = %e,
                                    "failed to mark local as absent"
                                );
                            }
                        }
                        return Err((
                            entry.relative_path,
                            format!("no route available to {target}"),
                        ));
                    }
                };

                // --- File existence check on src ---
                // Phase 1: only local sources get filesystem check.
                // Phase 2: RemoteShell-based check for non-local sources.
                if route.src().is_local() {
                    let src_path = route.src_file_root().join(&entry.relative_path);
                    match tokio::fs::try_exists(&src_path).await {
                        Ok(true) => {}
                        Ok(false) => {
                            let _ = self
                                .store
                                .set_location_state(
                                    &entry.id,
                                    &LocationId::local(),
                                    LocationState::Absent,
                                )
                                .await;
                            return Err((
                                entry.relative_path,
                                "source file not found".into(),
                            ));
                        }
                        Err(e) => {
                            return Err((entry.relative_path, e.to_string()));
                        }
                    }
                }

                // --- Transfer ---
                self.store
                    .set_location_state(&entry.id, target, LocationState::Syncing)
                    .await
                    .map_err(|e| (entry.relative_path.clone(), e.to_string()))?;
                self.store
                    .set_error(&entry.relative_path, None)
                    .await
                    .map_err(|e| (entry.relative_path.clone(), e.to_string()))?;

                match route.transfer(&entry.relative_path).await {
                    Ok(()) => {
                        self.store
                            .set_location_state(&entry.id, target, LocationState::Present)
                            .await
                            .map_err(|e| (entry.relative_path.clone(), e.to_string()))?;
                        self.store
                            .set_synced_at(&entry.relative_path, chrono::Utc::now())
                            .await
                            .map_err(|e| (entry.relative_path.clone(), e.to_string()))?;
                        Ok(())
                    }
                    Err(e) => {
                        let _ = self
                            .store
                            .set_location_state(&entry.id, target, LocationState::Pending)
                            .await;
                        let _ = self
                            .store
                            .set_error(&entry.relative_path, Some(&e.to_string()))
                            .await;
                        Err((entry.relative_path, e.to_string()))
                    }
                }
            }))
            .buffer_unordered(self.force_concurrency)
            .collect()
            .await;

            for outcome in outcomes {
                match outcome {
                    Ok(()) => result.pushed += 1,
                    Err((path, msg)) => {
                        result.failed += 1;
                        result.errors.push((path, msg));
                    }
                }
            }
        }

        Ok(result)
    }

    // =========================================================================
    // Infrastructure helpers
    // =========================================================================

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

    // =========================================================================
    // Detailed operations
    // =========================================================================

    /// Register a file in the sync store (idempotent by relative_path).
    pub async fn register(
        &self,
        relative_path: &str,
        file_type: FileType,
        opts: RegisterOpts,
    ) -> Result<RegisterResult, SyncError> {
        // Check existing by path
        if let Some(mut existing) = self.store.get_by_path(relative_path).await? {
            existing.update_metadata(
                file_type,
                opts.file_hash,
                opts.content_hash,
                opts.file_size,
                opts.gen_id,
            );
            self.store.update_entry(&existing).await?;
            return Ok(RegisterResult::Updated(existing));
        }

        // Check for duplicate by hash
        if let Some(dup) = self
            .store
            .find_duplicate(&opts.file_hash, opts.content_hash.as_deref(), relative_path)
            .await?
        {
            return Ok(RegisterResult::Duplicate {
                duplicate_of: dup.relative_path.clone(),
                existing: dup,
            });
        }

        // New entry
        let entry = SyncEntry::with_locations(
            relative_path.to_string(),
            file_type,
            opts.file_hash,
            opts.content_hash,
            opts.file_size,
            opts.gen_id,
            opts.initial_locations,
        );
        self.store.insert_entry(&entry).await?;
        Ok(RegisterResult::Created(entry))
    }

    /// Pull a file from a remote location to local.
    ///
    /// Phase 1: uses backend.pull() directly (not route.transfer()).
    /// Finds a route from src to local and uses its backend.
    pub async fn pull_file(&self, src: &LocationId, relative_path: &str) -> Result<(), SyncError> {
        let local = LocationId::local();

        // Find route from src to local, or fallback to src's outbound route
        // to get the backend (Phase 1 compatibility).
        let route = self
            .find_route(src, &local)
            .or_else(|| {
                // Fallback: find any route where src is the source
                // (use its backend for pull operation)
                self.routes.iter().find(|r| r.src() == src)
            })
            .ok_or_else(|| SyncError::NoRouteAvailable {
                dest: local.to_string(),
                path: relative_path.to_string(),
            })?;

        // Resolve remote path using route's dest_remote_root or src's remote root
        let remote_path = TransferRoute::join_remote(
            route.dest_remote_root(),
            relative_path,
        );
        let local_path = self.local_file_root.join(relative_path);

        route.backend().pull(&remote_path, &local_path).await?;

        // Auto-register if not tracked, or update state if already tracked
        match self.store.get_by_path(relative_path).await? {
            None => {
                // Compute hash of the pulled file
                let (hash_result, file_size) = self.inspect_file(&local_path).await?;

                let file_type = local_path
                    .extension()
                    .and_then(|e| e.to_str())
                    .map(FileType::from_extension)
                    .unwrap_or(FileType::Asset);

                let mut locations = HashMap::new();
                locations.insert(LocationId::local(), LocationState::Present);
                locations.insert(src.clone(), LocationState::Present);

                let result = self
                    .register(
                        relative_path,
                        file_type,
                        RegisterOpts {
                            file_hash: hash_result.file_hash,
                            content_hash: hash_result.content_hash,
                            file_size,
                            gen_id: None,
                            initial_locations: locations,
                        },
                    )
                    .await?;

                if let RegisterResult::Duplicate { duplicate_of, .. } = result {
                    return Err(SyncError::Duplicate {
                        path: relative_path.to_string(),
                        duplicate_of,
                    });
                }
            }
            Some(existing) => {
                self.store
                    .set_location_state(&existing.id, &LocationId::local(), LocationState::Present)
                    .await?;
                self.store
                    .set_location_state(&existing.id, src, LocationState::Present)
                    .await?;
            }
        }

        Ok(())
    }

    /// Get a single file's sync state by relative path.
    pub async fn get(&self, relative_path: &str) -> Result<Option<SyncEntry>, SyncError> {
        self.store.get_by_path(relative_path).await
    }

    /// List entries pending sync to a destination.
    pub async fn pending(&self, dest: &LocationId) -> Result<Vec<SyncEntry>, SyncError> {
        self.store.pending(dest).await
    }

    /// List all tracked entries.
    pub async fn list(
        &self,
        filter: Option<FileType>,
        limit: Option<usize>,
    ) -> Result<Vec<SyncEntry>, SyncError> {
        self.store.list(filter, limit).await
    }

    /// List entries with errors.
    pub async fn errors(&self) -> Result<Vec<SyncEntry>, SyncError> {
        self.store.errors().await
    }

    /// Register a remote endpoint in the store.
    pub async fn register_remote(&self, config: &RemoteConfig) -> Result<(), SyncError> {
        self.store.register_remote(config).await
    }

    /// Remove a remote endpoint from the store and all associated routes.
    pub async fn remove_remote(&mut self, location: &LocationId) -> Result<(), SyncError> {
        self.store.remove_remote(location).await?;
        self.remove_routes_for(location);
        Ok(())
    }

    /// List registered remotes.
    pub async fn list_remotes(&self) -> Result<Vec<RemoteConfig>, SyncError> {
        self.store.list_remotes().await
    }

    /// Register a generation's output files (by absolute paths).
    pub async fn register_generation(
        &self,
        gen_id: &str,
        output: &str,
        recipe: Option<&str>,
    ) -> Result<Vec<SyncEntry>, SyncError> {
        let mut entries = Vec::new();

        match self.assert_file_exists(Path::new(output)).await {
            Ok(()) => {
                let result = self.notify(output, FileType::Image, Some(gen_id)).await?;
                entries.push(result.entry);
            }
            Err(SyncError::FileNotFound(_)) => {
                tracing::warn!(
                    path = output,
                    "output file not found, skipping registration"
                );
            }
            Err(e) => return Err(e),
        }

        if let Some(recipe_path) = recipe {
            match self.assert_file_exists(Path::new(recipe_path)).await {
                Ok(()) => {
                    let result = self
                        .notify(recipe_path, FileType::Recipe, Some(gen_id))
                        .await?;
                    entries.push(result.entry);
                }
                Err(SyncError::FileNotFound(_)) => {
                    tracing::warn!(
                        path = recipe_path,
                        "recipe file not found, skipping registration"
                    );
                }
                Err(e) => return Err(e),
            }
        }

        Ok(entries)
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

        // Register "cloud" as a remote with a remote_root
        store
            .register_remote(&RemoteConfig {
                location_id: LocationId::new("cloud").unwrap(),
                backend: "memory".into(),
                remote_root: "remote/output".into(),
                config: serde_json::json!({}),
                created_at: chrono::Utc::now(),
            })
            .await
            .unwrap();

        // Create route: local → cloud
        let routes = vec![TransferRoute::new(
            LocationId::local(),
            LocationId::new("cloud").unwrap(),
            dir.to_path_buf(),
            "remote/output".into(),
            Box::new(Arc::clone(&cloud_backend)),
        )];

        let service = SyncService::new(dir.to_path_buf(), Box::new(store), routes);
        (service, cloud_backend)
    }

    #[cfg(feature = "sqlite")]
    #[tokio::test]
    async fn notify_new_file() {
        let dir = tempfile::tempdir().unwrap();
        let (service, _backend) = test_service_with_dir(dir.path()).await;

        // Create a temp file (non-PNG, so hash will be None)
        let path = dir.path().join("test.json");
        std::fs::write(&path, b"{}").unwrap();

        let result = service
            .notify(path.to_str().unwrap(), FileType::Recipe, Some("gen-1"))
            .await
            .unwrap();

        assert!(!result.is_duplicate);
        assert_eq!(result.entry.file_type, FileType::Recipe);
        assert_eq!(result.entry.gen_id.as_deref(), Some("gen-1"));
        assert_eq!(result.entry.relative_path, "test.json");
        assert_eq!(
            result.entry.location_state(&LocationId::local()),
            LocationState::Present
        );
        assert_eq!(
            result
                .entry
                .location_state(&LocationId::new("cloud").unwrap()),
            LocationState::Pending
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
        let batch = service.force(Some(&cloud)).await.unwrap();
        assert_eq!(batch.pushed, 1);
        assert_eq!(batch.failed, 0);

        // Verify backend received the remote_root-prefixed path
        let log = backend.log.lock().await;
        assert_eq!(log.len(), 1);
        match &log[0] {
            crate::infra::backend::memory::Op::Push { remote, .. } => {
                assert_eq!(remote, "remote/output/push.json");
            }
            _ => panic!("expected Push op"),
        }

        // Verify state updated
        let entry = service.get("push.json").await.unwrap().unwrap();
        assert_eq!(entry.location_state(&cloud), LocationState::Present);
        assert!(entry.synced_at.is_some());
    }

    #[cfg(feature = "sqlite")]
    #[tokio::test]
    async fn force_failure_rollback() {
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

        let cloud = LocationId::new("cloud").unwrap();
        let batch = service.force(Some(&cloud)).await.unwrap();
        assert_eq!(batch.failed, 1);
        assert_eq!(batch.pushed, 0);

        // State should revert to pending, error recorded
        let entry = service.get("fail.json").await.unwrap().unwrap();
        assert_eq!(entry.location_state(&cloud), LocationState::Pending);
        assert!(entry.error.is_some());
    }

    #[cfg(feature = "sqlite")]
    #[tokio::test]
    async fn register_idempotent() {
        let dir = tempfile::tempdir().unwrap();
        let (service, _) = test_service_with_dir(dir.path()).await;

        let opts1 = RegisterOpts {
            file_hash: "hash_idem".into(),
            content_hash: None,
            file_size: None,
            gen_id: None,
            initial_locations: HashMap::new(),
        };
        let r1 = service
            .register("idem.json", FileType::Asset, opts1)
            .await
            .unwrap();
        assert!(matches!(r1, RegisterResult::Created(_)));

        let opts2 = RegisterOpts {
            file_hash: "hash_idem".into(),
            content_hash: None,
            file_size: None,
            gen_id: None,
            initial_locations: HashMap::new(),
        };
        let r2 = service
            .register("idem.json", FileType::Asset, opts2)
            .await
            .unwrap();
        assert!(matches!(r2, RegisterResult::Updated(_)));

        // Only 1 entry in store
        let all = service.list(None, None).await.unwrap();
        assert_eq!(all.len(), 1);
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
    }

    #[cfg(feature = "sqlite")]
    #[tokio::test]
    async fn to_relative_and_back() {
        let dir = tempfile::tempdir().unwrap();
        let (service, _) = test_service_with_dir(dir.path()).await;

        let abs = dir.path().join("sub/dir/file.png");
        let rel = service.to_relative(&abs).unwrap();
        assert_eq!(rel, "sub/dir/file.png");

        let back = service.local_file_root().join(&rel);
        assert_eq!(back, abs);
    }

    #[cfg(feature = "sqlite")]
    #[tokio::test]
    async fn find_route_prefers_local() {
        let dir = tempfile::tempdir().unwrap();
        let (service, _) = test_service_with_dir(dir.path()).await;

        // Entry with local=Present
        let entry = SyncEntry::new(
            "test.png".to_string(),
            FileType::Image,
            "hash".into(),
            None,
            None,
            None,
            &[LocationId::new("cloud").unwrap()],
        );

        let cloud = LocationId::new("cloud").unwrap();
        let route = service.find_route_for_entry(&entry, &cloud);
        assert!(route.is_some());
        assert!(route.unwrap().src().is_local());
    }

    #[cfg(feature = "sqlite")]
    #[tokio::test]
    async fn find_route_returns_none_when_no_match() {
        let dir = tempfile::tempdir().unwrap();
        let (service, _) = test_service_with_dir(dir.path()).await;

        // Entry with only "nas" present — no route from nas → cloud
        let entry = SyncEntry::with_locations(
            "test.png".to_string(),
            FileType::Image,
            "hash".into(),
            None,
            None,
            None,
            HashMap::from([
                (LocationId::new("nas").unwrap(), LocationState::Present),
                (LocationId::new("cloud").unwrap(), LocationState::Pending),
            ]),
        );

        let cloud = LocationId::new("cloud").unwrap();
        let route = service.find_route_for_entry(&entry, &cloud);
        assert!(route.is_none());
    }
}
