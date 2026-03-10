//! SyncService — application-layer orchestrator for sync operations.
//!
//! Coordinates [`SyncStore`], [`TransferRoute`]s, and [`ContentHasher`]
//! to provide register/notify/push/pull/force operations.
//!
//! # Path model
//!
//! All entries are stored with **relative paths**.
//! Each route encapsulates src/dest path resolution via `TransferRoute`.
//!
//! # Route-based architecture
//!
//! Transfer operations use `Vec<TransferRoute>` instead of
//! `HashMap<LocationId, StorageBackend>`. Each route is a directed edge
//! (src → dest) with its own backend and path roots.
//!
//! `notify()` resolves the local file root from a route whose src is `local`.
//! `notify_remote()` uses the route's `src_shell` for remote hash computation.

use std::collections::HashMap;
use std::path::Path;
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
/// The local file root is derived from routes whose `src` is `local`.
/// No dedicated field — all path resolution goes through routes.
pub struct SyncService {
    store: Box<dyn SyncStore>,
    routes: Vec<TransferRoute>,
    hasher: Arc<dyn ContentHasher>,
    force_concurrency: usize,
}

impl SyncService {
    /// Default maximum number of concurrent push operations per target.
    const DEFAULT_FORCE_CONCURRENCY: usize = 8;

    /// Create a new SyncService.
    pub fn new(
        store: Box<dyn SyncStore>,
        routes: Vec<TransferRoute>,
    ) -> Self {
        Self {
            store,
            routes,
            hasher: Arc::new(Djb2Hasher),
            force_concurrency: Self::DEFAULT_FORCE_CONCURRENCY,
        }
    }

    /// Create with a custom content hasher.
    pub fn with_hasher(
        store: Box<dyn SyncStore>,
        routes: Vec<TransferRoute>,
        hasher: Arc<dyn ContentHasher>,
    ) -> Self {
        Self {
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

    /// Resolve the local file root from routes.
    ///
    /// Finds the first route whose src is `local` and returns its `src_file_root`.
    /// Returns `None` if no local-source route is registered.
    pub fn local_root(&self) -> Option<&Path> {
        self.routes
            .iter()
            .find(|r| r.src().is_local())
            .map(|r| r.src_file_root().as_path())
    }

    // =========================================================================
    // Path helpers
    // =========================================================================

    /// Convert an absolute local path to a relative path from the local file root.
    ///
    /// Returns `Err(OutsideSyncRoot)` if the path is not under the local root,
    /// or if no local-source route is registered.
    pub fn to_relative(&self, absolute_path: &Path) -> Result<String, SyncError> {
        let local_root = self.local_root().ok_or_else(|| SyncError::OutsideSyncRoot {
            path: absolute_path.display().to_string(),
        })?;
        absolute_path
            .strip_prefix(local_root)
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

    /// Notify the service of a file on a remote location.
    ///
    /// Computes hash/size via `RemoteShell` (sha256sum + stat), then registers
    /// the entry with `origin` as Present and all other remotes as Pending.
    ///
    /// Requires a route FROM `origin` with a `src_shell` configured.
    ///
    /// `relative_path` is relative to the route's `src_file_root`.
    pub async fn notify_remote(
        &self,
        origin: &LocationId,
        relative_path: &str,
        file_type: FileType,
        gen_id: Option<&str>,
    ) -> Result<NotifyResult, SyncError> {
        // Find a route from this origin (to get the shell)
        let route = self
            .routes
            .iter()
            .find(|r| r.src() == origin && r.src_shell().is_some())
            .ok_or_else(|| SyncError::NoRouteAvailable {
                dest: origin.to_string(),
                path: relative_path.to_string(),
            })?;

        // Verify file exists on remote
        if !route.src_file_exists(relative_path).await? {
            return Err(SyncError::FileNotFound(
                route.src_file_root().join(relative_path),
            ));
        }

        // Inspect file via RemoteShell
        let (hash_result, file_size) = route
            .inspect_src_file(relative_path, self.hasher.as_ref())
            .await?;

        // Duplicate check
        if let Some(existing) = self
            .store
            .find_duplicate(
                &hash_result.file_hash,
                hash_result.content_hash.as_deref(),
                relative_path,
            )
            .await?
        {
            return Ok(NotifyResult {
                is_duplicate: true,
                duplicate_of: Some(existing.relative_path.clone()),
                entry: existing,
            });
        }

        // Determine remote locations (all registered remotes except origin)
        let remotes = self.store.list_remotes().await?;
        let other_remotes: Vec<LocationId> = remotes
            .iter()
            .filter(|r| r.location_id != *origin)
            .map(|r| r.location_id.clone())
            .collect();

        // Build initial locations: origin=Present, others=Pending
        let mut locations = HashMap::new();
        locations.insert(origin.clone(), LocationState::Present);
        for remote in &other_remotes {
            locations.insert(remote.clone(), LocationState::Pending);
        }

        // Update existing or create new
        if let Some(mut existing) = self.store.get_by_path(relative_path).await? {
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

        let entry = SyncEntry::with_locations(
            relative_path.to_string(),
            file_type,
            hash_result.file_hash,
            hash_result.content_hash,
            file_size,
            gen_id.map(|s| s.to_string()),
            locations,
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
    /// File existence is checked before transfer via `route.src_file_exists()`.
    /// Local sources use `tokio::fs::try_exists`, remote sources use
    /// `RemoteShell` (`test -f`). If the file was deleted, the source
    /// location is marked as Absent.
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
                        return Err((
                            entry.relative_path,
                            format!("no route available to {target}"),
                        ));
                    }
                };

                // --- File existence check on src ---
                // Local: tokio::fs check. Remote: RemoteShell `test -f`.
                match route.src_file_exists(&entry.relative_path).await {
                    Ok(true) => {}
                    Ok(false) => {
                        let _ = self
                            .store
                            .set_location_state(
                                &entry.id,
                                route.src(),
                                LocationState::Absent,
                            )
                            .await;
                        return Err((
                            entry.relative_path,
                            format!("source file not found on {}", route.src()),
                        ));
                    }
                    Err(e) => {
                        return Err((entry.relative_path, e.to_string()));
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
    /// Requires a route registered from `src` → `local`.
    /// The route's `src_file_root` is used as the remote path root.
    ///
    /// # Route registration for pull
    ///
    /// ```ignore
    /// // Push route: local → cloud
    /// TransferRoute::new(local, cloud, local_dir, "vdsl/output", backend)
    /// // Pull route: cloud → local (explicit reverse)
    /// TransferRoute::new(cloud, local, "vdsl/output", "", backend)
    /// ```
    pub async fn pull_file(&self, src: &LocationId, relative_path: &str) -> Result<(), SyncError> {
        let local = LocationId::local();

        let route = self
            .find_route(src, &local)
            .ok_or_else(|| SyncError::NoRouteAvailable {
                dest: local.to_string(),
                path: relative_path.to_string(),
            })?;

        // remote_path = src_file_root / relative_path
        let remote_path = TransferRoute::safe_join(route.src_file_root(), relative_path);
        let local_path = TransferRoute::safe_join(route.dest_file_root(), relative_path);

        // Ensure parent directory exists
        if let Some(parent) = local_path.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }

        route
            .backend()
            .pull(&remote_path.to_string_lossy(), &local_path)
            .await?;

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
        store
            .register_remote(&RemoteConfig {
                location_id: LocationId::new("cloud").unwrap(),
                backend: "memory".into(),
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
            PathBuf::from("remote/output"),
            Box::new(Arc::clone(&cloud_backend)),
        )];

        let service = SyncService::new(Box::new(store), routes);
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

        // Verify backend received the dest_file_root-prefixed path
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

        let back = service.local_root().unwrap().join(&rel);
        assert_eq!(back, abs);
    }

    #[cfg(feature = "sqlite")]
    #[tokio::test]
    async fn notify_remote_registers_file() {
        use crate::infra::shell::mock::{MockFile, MockShell};

        let dir = tempfile::tempdir().unwrap();
        let shell = MockShell::with_files(vec![(
            "/workspace/output/gen-pod-001.png",
            MockFile::new("abcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890", 2048),
        )]);
        let (service, _backend) =
            test_service_with_remote_source(dir.path(), Box::new(shell)).await;

        let pod = LocationId::new("pod").unwrap();
        let cloud = LocationId::new("cloud").unwrap();

        let result = service
            .notify_remote(&pod, "gen-pod-001.png", FileType::Image, Some("gen-pod"))
            .await
            .unwrap();

        assert!(!result.is_duplicate);
        assert_eq!(result.entry.relative_path, "gen-pod-001.png");
        assert_eq!(result.entry.file_type, FileType::Image);
        assert_eq!(result.entry.gen_id.as_deref(), Some("gen-pod"));
        // Pod should be Present (origin)
        assert_eq!(result.entry.location_state(&pod), LocationState::Present);
        // Cloud should be Pending
        assert_eq!(result.entry.location_state(&cloud), LocationState::Pending);
        // file_hash should be sha256
        assert_eq!(
            result.entry.file_hash,
            "abcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890"
        );
        assert_eq!(result.entry.file_size, Some(2048));
        // content_hash should be None (remote can't compute PNG semantic hash)
        assert!(result.entry.content_hash.is_none());
    }

    #[cfg(feature = "sqlite")]
    #[tokio::test]
    async fn notify_remote_file_not_found() {
        use crate::infra::shell::mock::MockShell;

        let dir = tempfile::tempdir().unwrap();
        let shell = MockShell::new(Vec::<String>::new());
        let (service, _) = test_service_with_remote_source(dir.path(), Box::new(shell)).await;

        let pod = LocationId::new("pod").unwrap();
        let result = service
            .notify_remote(&pod, "nonexistent.png", FileType::Image, None)
            .await;

        assert!(matches!(result, Err(SyncError::FileNotFound(_))));
    }

    #[cfg(feature = "sqlite")]
    #[tokio::test]
    async fn notify_remote_then_force_to_cloud() {
        use crate::infra::shell::mock::{MockFile, MockShell};

        let dir = tempfile::tempdir().unwrap();
        let shell = MockShell::with_files(vec![(
            "/workspace/output/gen-mesh-001.png",
            MockFile::new("sha256_hash_value_placeholder_0000000000000000000000000000000000", 4096),
        )]);
        let (service, backend) =
            test_service_with_remote_source(dir.path(), Box::new(shell)).await;

        let pod = LocationId::new("pod").unwrap();
        let cloud = LocationId::new("cloud").unwrap();

        // Step 1: notify on pod
        service
            .notify_remote(&pod, "gen-mesh-001.png", FileType::Image, Some("gen-mesh"))
            .await
            .unwrap();

        // Step 2: force to cloud (uses pod→cloud route)
        let batch = service.force(Some(&cloud)).await.unwrap();
        assert_eq!(batch.pushed, 1);
        assert_eq!(batch.failed, 0);

        // Verify backend received correct paths
        let log = backend.log.lock().await;
        assert_eq!(log.len(), 1);
        match &log[0] {
            crate::infra::backend::memory::Op::Push { remote, .. } => {
                assert_eq!(remote, "vdsl/output/gen-mesh-001.png");
            }
            _ => panic!("expected Push op"),
        }

        // Both locations should be Present
        let entry = service.get("gen-mesh-001.png").await.unwrap().unwrap();
        assert_eq!(entry.location_state(&pod), LocationState::Present);
        assert_eq!(entry.location_state(&cloud), LocationState::Present);
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

    /// Build a service with local→cloud (push) + cloud→local (pull) routes.
    #[cfg(feature = "sqlite")]
    async fn test_service_with_pull_route(
        dir: &Path,
    ) -> (SyncService, Arc<InMemoryBackend>) {
        let store = SqliteSyncStore::open_in_memory().await.unwrap();
        let cloud_backend = Arc::new(InMemoryBackend::default());

        store
            .register_remote(&RemoteConfig {
                location_id: LocationId::new("cloud").unwrap(),
                backend: "memory".into(),
                config: serde_json::json!({}),
                created_at: chrono::Utc::now(),
            })
            .await
            .unwrap();

        let routes = vec![
            // Push: local → cloud
            TransferRoute::new(
                LocationId::local(),
                LocationId::new("cloud").unwrap(),
                dir.to_path_buf(),
                PathBuf::from("remote/output"),
                Box::new(Arc::clone(&cloud_backend)),
            ),
            // Pull: cloud → local (reverse route)
            TransferRoute::new(
                LocationId::new("cloud").unwrap(),
                LocationId::local(),
                PathBuf::from("remote/output"),
                dir.to_path_buf(),
                Box::new(Arc::clone(&cloud_backend)),
            ),
        ];

        let service = SyncService::new(Box::new(store), routes);
        (service, cloud_backend)
    }

    #[cfg(feature = "sqlite")]
    #[tokio::test]
    async fn pull_file_uses_reverse_route() {
        let dir = tempfile::tempdir().unwrap();
        let (service, backend) = test_service_with_pull_route(dir.path()).await;

        // Pre-register the file as existing on cloud, pending on local
        let cloud = LocationId::new("cloud").unwrap();
        let mut locations = HashMap::new();
        locations.insert(cloud.clone(), LocationState::Present);
        locations.insert(LocationId::local(), LocationState::Pending);

        let opts = RegisterOpts {
            file_hash: "hash_pull".into(),
            content_hash: None,
            file_size: Some(512),
            gen_id: None,
            initial_locations: locations,
        };
        service
            .register("pull-me.json", FileType::Asset, opts)
            .await
            .unwrap();

        // Pull from cloud to local
        let result = service.pull_file(&cloud, "pull-me.json").await;
        assert!(result.is_ok(), "pull_file should succeed: {result:?}");

        // Verify backend received correct remote path
        let log = backend.log.lock().await;
        let pull_ops: Vec<_> = log
            .iter()
            .filter(|op| matches!(op, crate::infra::backend::memory::Op::Pull { .. }))
            .collect();
        assert_eq!(pull_ops.len(), 1);
        match pull_ops[0] {
            crate::infra::backend::memory::Op::Pull { remote, local } => {
                assert_eq!(remote, "remote/output/pull-me.json");
                assert!(
                    local.ends_with("pull-me.json"),
                    "local path should end with filename: {local}"
                );
            }
            _ => panic!("expected Pull op"),
        }

        // Both locations should be Present
        let entry = service.get("pull-me.json").await.unwrap().unwrap();
        assert_eq!(entry.location_state(&cloud), LocationState::Present);
        assert_eq!(
            entry.location_state(&LocationId::local()),
            LocationState::Present
        );
    }

    #[cfg(feature = "sqlite")]
    #[tokio::test]
    async fn pull_file_no_route_returns_error() {
        let dir = tempfile::tempdir().unwrap();
        let (service, _) = test_service_with_dir(dir.path()).await;

        // test_service_with_dir only has local→cloud, no cloud→local
        let cloud = LocationId::new("cloud").unwrap();
        let result = service.pull_file(&cloud, "no-route.json").await;
        assert!(matches!(result, Err(SyncError::NoRouteAvailable { .. })));
    }

    /// Build a service with pod→cloud route (remote source).
    #[cfg(feature = "sqlite")]
    async fn test_service_with_remote_source(
        dir: &Path,
        mock_shell: Box<dyn crate::infra::shell::RemoteShell>,
    ) -> (SyncService, Arc<InMemoryBackend>) {
        let store = SqliteSyncStore::open_in_memory().await.unwrap();
        let cloud_backend = Arc::new(InMemoryBackend::default());

        // Register remotes
        for id in &["pod", "cloud"] {
            store
                .register_remote(&RemoteConfig {
                    location_id: LocationId::new(*id).unwrap(),
                    backend: "memory".into(),
                    config: serde_json::json!({}),
                    created_at: chrono::Utc::now(),
                })
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

        let service = SyncService::new(Box::new(store), routes);
        (service, cloud_backend)
    }

    #[cfg(feature = "sqlite")]
    #[tokio::test]
    async fn force_remote_source_success() {
        use crate::infra::shell::mock::MockShell;

        let dir = tempfile::tempdir().unwrap();
        // Mock: file exists on pod
        let shell = MockShell::new(vec!["/workspace/output/gen-001.png"]);
        let (service, backend) =
            test_service_with_remote_source(dir.path(), Box::new(shell)).await;

        // Manually register entry with pod=Present, cloud=Pending
        let mut locations = HashMap::new();
        locations.insert(LocationId::new("pod").unwrap(), LocationState::Present);
        locations.insert(LocationId::new("cloud").unwrap(), LocationState::Pending);

        let opts = RegisterOpts {
            file_hash: "hash_remote".into(),
            content_hash: None,
            file_size: Some(1024),
            gen_id: None,
            initial_locations: locations,
        };
        service
            .register("gen-001.png", FileType::Image, opts)
            .await
            .unwrap();

        // Force push to cloud — should use pod→cloud route
        let cloud = LocationId::new("cloud").unwrap();
        let batch = service.force(Some(&cloud)).await.unwrap();
        assert_eq!(batch.pushed, 1);
        assert_eq!(batch.failed, 0);

        // Verify backend received push
        let log = backend.log.lock().await;
        assert_eq!(log.len(), 1);
        match &log[0] {
            crate::infra::backend::memory::Op::Push { remote, .. } => {
                assert_eq!(remote, "vdsl/output/gen-001.png");
            }
            _ => panic!("expected Push op"),
        }

        // Verify state
        let entry = service.get("gen-001.png").await.unwrap().unwrap();
        assert_eq!(entry.location_state(&cloud), LocationState::Present);
    }

    #[cfg(feature = "sqlite")]
    #[tokio::test]
    async fn force_remote_source_file_not_found() {
        use crate::infra::shell::mock::MockShell;

        let dir = tempfile::tempdir().unwrap();
        // Mock: file does NOT exist on pod
        let shell = MockShell::new(Vec::<String>::new());
        let (service, _backend) =
            test_service_with_remote_source(dir.path(), Box::new(shell)).await;

        // Register entry with pod=Present, cloud=Pending
        let mut locations = HashMap::new();
        locations.insert(LocationId::new("pod").unwrap(), LocationState::Present);
        locations.insert(LocationId::new("cloud").unwrap(), LocationState::Pending);

        let opts = RegisterOpts {
            file_hash: "hash_missing".into(),
            content_hash: None,
            file_size: Some(512),
            gen_id: None,
            initial_locations: locations,
        };
        service
            .register("missing.png", FileType::Image, opts)
            .await
            .unwrap();

        // Force push — file not found on pod, should fail
        let cloud = LocationId::new("cloud").unwrap();
        let batch = service.force(Some(&cloud)).await.unwrap();
        assert_eq!(batch.pushed, 0);
        assert_eq!(batch.failed, 1);
        assert!(batch.errors[0].1.contains("source file not found"));

        // Pod should be marked Absent
        let entry = service.get("missing.png").await.unwrap().unwrap();
        assert_eq!(
            entry.location_state(&LocationId::new("pod").unwrap()),
            LocationState::Absent
        );
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
