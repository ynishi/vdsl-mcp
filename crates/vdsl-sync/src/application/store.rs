//! Store — application-layer facade for distributed file storage.
//!
//! Provides a Firestore-like API over the sync topology:
//!
//! - **Topology** — location/route configuration (fixed at build time via [`StoreBuilder`])
//! - **File CRUD** — document operations ([`Store::put`], [`Store::get`], [`Store::list`])
//! - **Sync** — replication across topology ([`Store::sync`], [`Store::sync_route`])
//! - **Status** — monitoring ([`Store::status`], [`Store::pending`], [`Store::errors`])
//!
//! The [`TransferEngine`] is an internal implementation detail — callers
//! interact only with `Store` and never see routing mechanics.
//!
//! # Sync vs sync_route vs force_full_rewrite
//!
//! - [`Store::sync()`] — full cycle: scan → register → retry failed → execute all.
//!   Potentially long-running (scales with file count × location count).
//!   Use [`sync_spawn()`](Store::sync_spawn) for non-blocking execution.
//! - [`Store::sync_route()`] — single route: reconcile missing → execute route.
//!   Use [`sync_route_spawn()`](Store::sync_route_spawn) for non-blocking execution.
//! - [`Store::force_full_rewrite()`] — maintenance operation: scan + requeue ALL
//!   files to ALL destinations regardless of state + execute all.
//!   Non-blocking only ([`force_full_rewrite_spawn()`](Store::force_full_rewrite_spawn)).
//!   **MCP-only — not exposed to Lua scripts.**
//!
//! All sync operations exposed to Lua and MCP use the `_spawn` variants
//! (returning [`TaskId`] immediately) to avoid blocking the caller.

use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::sync::Arc;

use super::observer::{DeltaSummary, HashProgress, NullObserver, SyncObserver};
use super::route::TransferRoute;
use super::scanner::{compute_deltas, ScannedEntry};
use super::task::ProgressFn;
use super::transfer_engine::TransferEngine;
use crate::application::error::SyncError;
use crate::domain::config::SyncConfig;
use crate::domain::delta::{AddedFile, FileDelta, ModifiedFile};
use crate::domain::error::DomainError;
use crate::domain::file_type::FileType;
use crate::domain::fingerprint::FileFingerprint;
use crate::domain::location::{LocationId, LocationSummary, SyncSummary};
use crate::domain::plan::{plan_transfers_for, PlannedTransfer};
use crate::domain::retry::RetryPolicy;
use crate::domain::scan::{ScanOutcome, ScanReport};
use crate::domain::tracked_file::TrackedFile;
use crate::domain::transfer::{Transfer, TransferKind, TransferState};
use crate::domain::view::{FileView, PresenceState, PresenceView};
use crate::infra::error::InfraError;
use crate::infra::file_store::FileStore;
use crate::infra::hasher::{ContentHasher, Djb2Hasher, HashResult};
use crate::infra::remote_store::RemoteStore;
use crate::infra::store::RemoteConfig;
use crate::infra::topology_file_store::TopologyFileStore;
use crate::infra::transfer_store::TransferStore;

// =============================================================================
// Result types — BatchResult/BatchError は transfer_engine.rs に移動済み
// =============================================================================

pub use super::transfer_engine::{BatchError, BatchResult};

/// Options for [`Store::put`].
#[deprecated(note = "use SyncFacade::put")]
#[derive(Debug, Default)]
pub struct PutOptions {
    /// Source location. `None` = local (auto-detect from filesystem).
    pub source: Option<LocationId>,
    /// Embedded generation ID (e.g., ComfyUI prompt_id).
    pub embedded_id: Option<String>,
    /// Pre-computed file hash (required for remote sources).
    pub file_hash: Option<String>,
    /// Pre-computed content hash (optional, for semantic dedup).
    pub content_hash: Option<String>,
    /// Pre-computed file size (required for remote sources).
    pub file_size: Option<u64>,
}

/// Result of a [`Store::put`] operation.
#[deprecated(note = "use SyncFacade::put")]
#[derive(Debug, serde::Serialize)]
pub struct PutResult {
    pub file: TrackedFile,
    pub is_duplicate: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub duplicate_of: Option<String>,
    pub transfers_created: usize,
}

impl PutResult {
    /// Serialize to [`serde_json::Value`] for cross-boundary transport.
    pub fn to_value(&self) -> Result<serde_json::Value, SyncError> {
        serde_json::to_value(self).map_err(|e| -> SyncError {
            InfraError::Serialization(format!("PutResult: {e}")).into()
        })
    }
}

/// Per-file error during directory scan (non-fatal).
///
/// Scans never abort on a single file failure — errors are accumulated
/// and returned alongside successful results.
#[deprecated(note = "use SyncFacade pipeline")]
#[derive(Debug, Clone, serde::Serialize)]
pub struct ScanError {
    pub path: String,
    pub error: String,
}

/// Result of a [`Store::sync`] operation.
///
/// Separates scan-phase and transfer-phase results for debuggability.
#[deprecated(note = "use SyncFacade::sync")]
#[derive(Debug, Clone, Default, serde::Serialize)]
pub struct SyncResult {
    /// Files newly registered during directory scan.
    pub scanned: usize,
    /// Per-file errors from the scan phase (always present, empty if none).
    pub scan_errors: Vec<ScanError>,
    /// Per-location scan outcome (success/failure/unreachable).
    pub scan_report: ScanReport,
    /// Transfer execution results (transferred/failed/errors).
    #[serde(flatten)]
    pub batch: BatchResult,
}

impl SyncResult {
    /// Serialize to [`serde_json::Value`] for cross-boundary transport.
    pub fn to_value(&self) -> Result<serde_json::Value, SyncError> {
        serde_json::to_value(self).map_err(|e| -> SyncError {
            InfraError::Serialization(format!("SyncResult: {e}")).into()
        })
    }
}

// =============================================================================
// Internal helper types
// =============================================================================

/// Bundled parameters for the internal `upsert_and_transfer` method.
struct UpsertParams<'a> {
    file_hash: String,
    content_hash: Option<String>,
    file_size: u64,
    embedded_id: Option<&'a str>,
    origin: &'a LocationId,
}

// =============================================================================
// Store
// =============================================================================

/// Distributed file storage database.
///
/// Facade over persistence stores, transfer engine, and content hasher.
/// All routing/transfer mechanics are internal — callers use the
/// 4-category API: Topology, File CRUD, Sync, Status.
///
/// Transfer execution is delegated to [`TransferEngine`], which provides
/// `execute_all()`, `execute_route()`, and `execute_file()`.
/// Store wraps these with scan, reconciliation, and retry logic.
#[deprecated(note = "use SyncFacade — Store has cross-location hash mismatch (DJB2 vs SHA256)")]
pub struct Store {
    file_store: Arc<dyn FileStore>,
    /// FileStore→TopologyFileStore bridge（TransferEngine互換用）。
    topology_files: Arc<dyn TopologyFileStore>,
    transfer_store: Arc<dyn TransferStore>,
    remote_store: Arc<dyn RemoteStore>,
    engine: TransferEngine,
    hasher: Arc<dyn ContentHasher>,
    config: SyncConfig,
    scan_excludes: Vec<glob::Pattern>,
}

impl Store {
    // =========================================================================
    // Topology (read-only — topology is fixed at build time via StoreBuilder)
    // =========================================================================

    /// List all registered locations.
    pub async fn locations(&self) -> Result<Vec<RemoteConfig>, SyncError> {
        self.remote_store.list_remotes().await
    }

    /// All edges in the topology as `(src, dest)` pairs.
    pub fn all_edges(&self) -> Vec<(LocationId, LocationId)> {
        self.engine.all_edges()
    }

    // =========================================================================
    // File CRUD
    // =========================================================================

    /// Upsert a file into the storage topology.
    ///
    /// Accepts either a relative path (from sync root) or an absolute
    /// local path (automatically resolved to relative).
    ///
    /// Registers a new file or updates an existing one, then creates
    /// Transfer objects for all directly reachable destinations.
    ///
    /// # Local files (`opts.source` is `None`)
    ///
    /// The file must exist on the local filesystem at
    /// `local_root / path`. Hash and size are computed automatically.
    ///
    /// # Remote files (`opts.source` is `Some(location)`)
    ///
    /// The file exists on a remote host. Caller must supply
    /// `opts.file_hash` and `opts.file_size` (local hashing is
    /// impossible for remote files).
    pub async fn put(
        &self,
        path: &str,
        file_type: FileType,
        opts: PutOptions,
    ) -> Result<PutResult, SyncError> {
        let relative_path = self.resolve_to_relative(path);
        let source = opts.source.clone().unwrap_or_else(LocationId::local);

        if source.is_local() {
            self.put_local(&relative_path, file_type, &opts).await
        } else {
            self.put_remote(&relative_path, file_type, &source, &opts)
                .await
        }
    }

    /// Get a single file's sync state.
    ///
    /// Accepts either a relative path (from sync root) or an absolute
    /// local path (automatically resolved to relative).
    ///
    /// Returns a [`FileView`] combining TrackedFile metadata with
    /// PresenceView per location (derived from latest Transfers).
    pub async fn get(&self, path: &str) -> Result<Option<FileView>, SyncError> {
        let relative = self.resolve_to_relative(path);
        let file = match self.file_store.get_file_by_path(&relative).await? {
            Some(f) => f,
            None => return Ok(None),
        };
        let view = self.build_file_view(file).await?;
        Ok(Some(view))
    }

    /// List tracked files with their presence state.
    ///
    /// Returns [`FileView`] (TrackedFile + per-location presence) for each file.
    /// Optionally filtered by [`FileType`] and limited to `limit` entries.
    pub async fn list(
        &self,
        filter: Option<FileType>,
        limit: Option<usize>,
    ) -> Result<Vec<FileView>, SyncError> {
        let files = self.file_store.list_files(filter, limit).await?;
        let mut views = Vec::with_capacity(files.len());
        for file in files {
            views.push(self.build_file_view(file).await?);
        }
        Ok(views)
    }

    // =========================================================================
    // Sync
    // =========================================================================

    /// Full sync cycle across the entire topology.
    ///
    /// 1. Scans all src locations for new/modified files (local + remote)
    /// 2. Detects local deletions and propagates Delete transfers
    /// 3. Retries failed transfers (transient errors within retry limit)
    /// 4. Executes all queued transfers in BFS order (nearest destinations first)
    ///
    /// Potentially long-running — scales with file count × location count.
    /// For single-route operations, use [`sync_route()`](Self::sync_route).
    pub async fn sync(&self, progress: Option<&ProgressFn>) -> Result<SyncResult, SyncError> {
        let null_obs = NullObserver;
        let bridge;
        let obs: &dyn SyncObserver = match progress {
            Some(p) => {
                bridge = super::observer::ProgressFnBridge::new(Arc::clone(p));
                &bridge
            }
            None => &null_obs,
        };
        self.sync_with_observer(obs).await
    }

    /// Observer-based sync: full pipeline with structured progress reporting.
    pub async fn sync_with_observer(
        &self,
        observer: &dyn SyncObserver,
    ) -> Result<SyncResult, SyncError> {
        // Cancel orphaned InFlight transfers from previous process crash.
        // Cancelled is a terminal state — these transfers will NOT be re-executed.
        let cancelled = self.transfer_store.cancel_orphaned_inflight().await?;
        if cancelled > 0 {
            tracing::info!(
                cancelled_count = cancelled,
                "sync: cancelled orphaned InFlight transfers"
            );
        }
        observer.on_sync_start(cancelled);

        let (scanned, scan_errors, scan_report) = self.scan_and_register(observer).await?;

        if scan_report.has_failures() {
            tracing::warn!(
                failed = ?scan_report.failed_locations(),
                "scan completed with location failures"
            );
        }

        // Transfer execution
        let queued = self.transfer_store.count_queued().await.unwrap_or(0);
        let targets = self.engine.destination_count();
        tracing::info!(
            queued_count = queued,
            destination_count = targets,
            "sync: transfer phase start"
        );
        observer.on_transfer_start(queued, targets);

        let batch = self
            .engine
            .execute_all_with_observer(
                self.topology_files.as_ref(),
                self.transfer_store.as_ref(),
                observer,
            )
            .await?;

        tracing::info!(
            transferred = batch.transferred,
            failed = batch.failed,
            scanned = scanned,
            scan_error_count = scan_errors.len(),
            "sync: pipeline complete"
        );
        observer.on_transfer_done(batch.transferred, batch.failed);
        observer.on_sync_done();

        Ok(SyncResult {
            scanned,
            scan_errors,
            scan_report,
            batch,
        })
    }

    /// Force full rewrite: scan + requeue ALL files → ALL destinations → execute.
    ///
    /// **LAST-RESORT maintenance operation.** Do NOT use for normal sync — use [`sync()`] instead.
    ///
    /// Unlike `sync()` which only processes changed/failed files, this
    /// re-queues **every** tracked file to **every** reachable destination
    /// regardless of current transfer state. Re-transfers all files even if
    /// already present at destination.
    ///
    /// Use ONLY when:
    /// - Remote storage lost/corrupted and needs full reconstruction
    /// - DB state inconsistent and cannot be repaired by normal sync
    /// - Explicit operator decision with understanding of full re-transfer cost
    ///
    /// **MCP-only** — not exposed to Lua scripts. Always long-running.
    #[deprecated(note = "use sync after clearing target — force_rewrite will be removed")]
    pub async fn force_full_rewrite(
        &self,
        progress: Option<&ProgressFn>,
    ) -> Result<SyncResult, SyncError> {
        let null_obs = NullObserver;
        let bridge;
        let obs: &dyn SyncObserver = match progress {
            Some(p) => {
                bridge = super::observer::ProgressFnBridge::new(Arc::clone(p));
                &bridge
            }
            None => &null_obs,
        };
        self.force_full_rewrite_with_observer(obs).await
    }

    /// Observer-based force full rewrite.
    #[deprecated(note = "use sync after clearing target — force_rewrite will be removed")]
    pub async fn force_full_rewrite_with_observer(
        &self,
        observer: &dyn SyncObserver,
    ) -> Result<SyncResult, SyncError> {
        // 1. Scan
        observer.on_sync_start(0);
        let (scanned, scan_errors, scan_report) = self.scan_and_register(observer).await?;

        // 2. Purge all non-completed transfers (failed/queued/blocked/in_flight).
        //    This ensures a clean slate — no duplicate requeue, no stale failures.
        let purged = self.transfer_store.purge_non_completed().await?;
        if purged > 0 {
            tracing::info!(purged, "force: purged non-completed transfers");
        }

        // 3. Compute optimal forward-only routes ONCE, then batch requeue.
        //    optimal_tree(local, reachable) produces outbound-only edges:
        //    e.g., local→pod→cloud. Pull-direction routes (cloud→local) are
        //    NOT generated because local is the origin.
        //    requeue_all handles per-file completed-check inside a single TX.
        let topology: &dyn crate::domain::plan::Topology = &self.engine;
        let origin = LocationId::local();
        let all_reachable = topology.reachable_from(&origin);
        tracing::info!(
            origin = %origin,
            reachable = ?all_reachable.iter().map(|l| l.to_string()).collect::<Vec<_>>(),
            "force: topology computed"
        );
        let routes = if all_reachable.is_empty() {
            Vec::new()
        } else {
            topology.optimal_tree(&origin, &all_reachable)
        };
        for (src, dest) in &routes {
            tracing::info!(src = %src, dest = %dest, "force: optimal route edge");
        }

        let requeued = if routes.is_empty() {
            0
        } else {
            let file_ids = self.file_store.list_all_ids().await?;
            tracing::info!(
                file_count = file_ids.len(),
                route_count = routes.len(),
                "force: requeue_all start"
            );
            self.transfer_store.requeue_all(&file_ids, &routes).await?
        };
        tracing::info!(
            requeued_count = requeued,
            route_count = routes.len(),
            origin = %origin,
            "force: batch requeued via optimal topology"
        );

        // 4. Execute all queued transfers
        let queued = self.transfer_store.count_queued().await.unwrap_or(0);
        let targets = self.engine.destination_count();
        observer.on_transfer_start(queued, targets);

        let batch = self
            .engine
            .execute_all_with_observer(
                self.topology_files.as_ref(),
                self.transfer_store.as_ref(),
                observer,
            )
            .await?;

        observer.on_transfer_done(batch.transferred, batch.failed);
        observer.on_sync_done();

        Ok(SyncResult {
            scanned,
            scan_errors,
            scan_report,
            batch,
        })
    }

    /// Sync queued transfers for a specific route (src -> dest).
    ///
    /// Before executing, reconciles missing files: if a Completed transfer
    /// exists but the dest file is absent, re-queues a new transfer.
    /// This enables restore scenarios (e.g., local file deleted → cloud→local pull).
    pub async fn sync_route(
        &self,
        src: &LocationId,
        dest: &LocationId,
    ) -> Result<BatchResult, SyncError> {
        self.engine
            .execute_route(
                self.topology_files.as_ref(),
                self.transfer_store.as_ref(),
                src,
                dest,
            )
            .await
    }

    /// Delete a file from the storage topology.
    ///
    /// Marks the TrackedFile as soft-deleted and creates Delete transfers
    /// for all reachable destinations from the origin location.
    /// The file record is kept in DB until all Delete transfers complete.
    pub async fn delete(
        &self,
        relative_path: &str,
        origin: &LocationId,
    ) -> Result<usize, SyncError> {
        let mut file = self
            .file_store
            .get_file_by_path(relative_path)
            .await?
            .ok_or_else(|| SyncError::NotRegistered(relative_path.to_string()))?;

        file.mark_deleted();
        self.file_store.upsert_file(&file).await?;

        self.plan_and_apply_delete(&file, origin).await
    }

    // =========================================================================
    // Status
    // =========================================================================

    /// Get aggregated sync status across all locations.
    ///
    /// For each file, determines the presence state at each location.
    /// A location appears at most once per file — if it appears as both
    /// src (implying Present) and dest, the dest-derived state takes
    /// priority only when it is more specific (e.g., Failed overrides Present).
    pub async fn status(&self) -> Result<SyncSummary, SyncError> {
        use crate::domain::view::{ErrorEntry, PendingEntry};

        let retry_policy = self.config.retry_policy();

        // DB集約クエリで完結（N+1問題の解消）
        let total_files = self.file_store.count_files().await?;
        let stats = self.transfer_store.transfer_stats().await?;
        let present_counts = self.transfer_store.present_counts_by_location().await?;
        let failed = self.transfer_store.failed_transfers().await?;
        let pending = self.transfer_store.all_pending_transfers().await?;

        let mut locations: HashMap<LocationId, LocationSummary> = HashMap::new();
        let mut total_errors = 0usize;

        // present_counts_by_location: src(送出元)とcompleted destのUNION DISTINCT
        for (loc, count) in &present_counts {
            let summary = locations.entry(loc.clone()).or_default();
            summary.present = *count;
        }

        // transfer_stats: dest側のpending/syncing/failed/absentを集計
        // (completedはpresent_counts_by_locationで処理済みなのでスキップ)
        for row in &stats {
            if row.state == TransferState::Completed || row.state == TransferState::Cancelled {
                continue;
            }
            let dest_state = match row.state {
                TransferState::Blocked | TransferState::Queued => PresenceState::Pending,
                TransferState::InFlight => PresenceState::Syncing,
                TransferState::Failed => {
                    let exhausted = match row.error_kind.as_deref() {
                        Some("permanent") => true,
                        Some("transient") => row.attempt >= retry_policy.max_attempts(),
                        _ => row.attempt >= retry_policy.max_attempts(),
                    };
                    if exhausted {
                        PresenceState::Failed
                    } else {
                        PresenceState::Pending
                    }
                }
                TransferState::Completed | TransferState::Cancelled => PresenceState::Absent,
            };

            let dest_summary = locations.entry(row.dest.clone()).or_default();
            match dest_state {
                PresenceState::Pending => {
                    dest_summary.pending = dest_summary.pending.saturating_add(row.file_count);
                }
                PresenceState::Syncing => {
                    dest_summary.syncing = dest_summary.syncing.saturating_add(row.file_count);
                }
                PresenceState::Failed => {
                    dest_summary.failed = dest_summary.failed.saturating_add(row.file_count);
                    total_errors = total_errors.saturating_add(row.file_count);
                }
                PresenceState::Absent => {
                    dest_summary.absent = dest_summary.absent.saturating_add(row.file_count);
                }
                PresenceState::Present => {}
            }
        }

        // error_entries: failed_transfers → View変換
        let error_entries: Vec<ErrorEntry> = failed
            .iter()
            .filter(|t| {
                let state = PresenceState::from_transfer(t, &retry_policy);
                state == PresenceState::Failed
            })
            .map(ErrorEntry::from_transfer)
            .collect();

        // pending_entries: queued/blocked transfers + retryable failed transfers
        let mut pending_entries: Vec<PendingEntry> =
            pending.iter().map(PendingEntry::from_transfer).collect();
        // Retryable failed transfers also count as pending
        for t in &failed {
            let state = PresenceState::from_transfer(t, &retry_policy);
            if state == PresenceState::Pending {
                pending_entries.push(PendingEntry::from_transfer(t));
            }
        }

        Ok(SyncSummary {
            locations,
            total_entries: total_files,
            total_errors,
            error_entries,
            pending_entries,
        })
    }

    // =========================================================================
    // Config accessors
    // =========================================================================

    /// The local file root (derived from routes).
    ///
    /// Returns `None` if no local-source route is registered.
    pub fn local_root(&self) -> Option<&Path> {
        self.engine.local_root()
    }

    /// The current sync configuration.
    pub fn config(&self) -> &SyncConfig {
        &self.config
    }

    /// The current retry policy (derived from config).
    pub fn retry_policy(&self) -> RetryPolicy {
        self.config.retry_policy()
    }

    // =========================================================================
    // Internal: put helpers
    // =========================================================================

    /// Put a local file (hash from filesystem).
    async fn put_local(
        &self,
        relative_path: &str,
        file_type: FileType,
        opts: &PutOptions,
    ) -> Result<PutResult, SyncError> {
        let local_root = self
            .local_root()
            .ok_or_else(|| SyncError::OutsideSyncRoot {
                path: relative_path.to_string(),
            })?;
        let abs_path = local_root.join(relative_path);
        self.assert_file_exists(&abs_path).await?;

        let (hash_result, file_size) = self.inspect_file(&abs_path).await?;

        // Check for duplicate by hash (same content at a different path)
        let duplicate_of = self
            .file_store
            .find_duplicate_file(
                &hash_result.file_hash,
                hash_result.content_hash.as_deref(),
                relative_path,
            )
            .await?;

        let is_duplicate = duplicate_of.is_some();
        let dup_path = duplicate_of.as_ref().map(|f| f.relative_path().to_string());

        // Always register the file under its own path (even if duplicate).
        // Duplicate files share the same content but live at different paths.
        // Transfer creation is skipped for duplicates — the content is
        // already at dest via the original file's transfer.
        let file = TrackedFile::from_scan(
            relative_path.to_string(),
            file_type,
            hash_result.file_hash,
            hash_result.content_hash,
            file_size.unwrap_or(0),
            opts.embedded_id.clone(),
        )?;

        // Check if this path already exists in DB (update case)
        if let Some(mut existing) = self.file_store.get_file_by_path(relative_path).await? {
            let hash_changed = existing.update_from_scan(
                file_type,
                file.file_hash().to_string(),
                file.content_hash().map(|s| s.to_string()),
                file.file_size(),
                opts.embedded_id.clone(),
            );
            self.file_store.upsert_file(&existing).await?;

            let transfers_created = if hash_changed && !is_duplicate {
                self.plan_and_apply_sync(&existing, &LocationId::local(), true)
                    .await?
            } else {
                0
            };

            return Ok(PutResult {
                file: existing,
                is_duplicate,
                duplicate_of: dup_path,
                transfers_created,
            });
        }

        self.file_store.upsert_file(&file).await?;

        // Create transfers only for non-duplicate files
        let transfers_created = if !is_duplicate {
            self.plan_and_apply_sync(&file, &LocationId::local(), false)
                .await?
        } else {
            0
        };

        Ok(PutResult {
            file,
            is_duplicate,
            duplicate_of: dup_path,
            transfers_created,
        })
    }

    /// Put a remote file (pre-computed hash).
    async fn put_remote(
        &self,
        relative_path: &str,
        file_type: FileType,
        source: &LocationId,
        opts: &PutOptions,
    ) -> Result<PutResult, SyncError> {
        let file_hash = opts.file_hash.clone().ok_or_else(|| -> SyncError {
            DomainError::Validation {
                field: "file_hash".into(),
                reason: "required for remote source".into(),
            }
            .into()
        })?;

        self.upsert_and_transfer(
            relative_path,
            file_type,
            UpsertParams {
                file_hash,
                content_hash: opts.content_hash.clone(),
                file_size: opts.file_size.unwrap_or(0),
                embedded_id: opts.embedded_id.as_deref(),
                origin: source,
            },
        )
        .await
    }

    /// Core upsert logic shared by local and remote put.
    async fn upsert_and_transfer(
        &self,
        relative_path: &str,
        file_type: FileType,
        params: UpsertParams<'_>,
    ) -> Result<PutResult, SyncError> {
        // Update existing or create new
        if let Some(mut existing) = self.file_store.get_file_by_path(relative_path).await? {
            let hash_changed = existing.update_from_scan(
                file_type,
                params.file_hash,
                params.content_hash,
                params.file_size,
                params.embedded_id.map(|s| s.to_string()),
            );
            self.file_store.upsert_file(&existing).await?;

            let transfers_created = if hash_changed {
                self.plan_and_apply_sync(&existing, params.origin, true)
                    .await?
            } else {
                0
            };

            return Ok(PutResult {
                file: existing,
                is_duplicate: false,
                duplicate_of: None,
                transfers_created,
            });
        }

        let file = TrackedFile::from_scan(
            relative_path.to_string(),
            file_type,
            params.file_hash,
            params.content_hash,
            params.file_size,
            params.embedded_id.map(|s| s.to_string()),
        )?;

        self.file_store.upsert_file(&file).await?;
        let transfers_created = self
            .plan_and_apply_sync(&file, params.origin, false)
            .await?;

        Ok(PutResult {
            file,
            is_duplicate: false,
            duplicate_of: None,
            transfers_created,
        })
    }

    // =========================================================================
    // Internal: scan
    // =========================================================================

    /// Scan all locations in the topology for new/modified files.
    ///
    /// Iterates every unique src location in the route graph:
    /// - local: filesystem recursive scan
    /// - remote (pod): `find` via src_shell
    /// - cloud (B2): `backend.list()` via rclone
    ///
    /// Detected files are registered with their origin location,
    /// generating Transfers to all reachable destinations.
    /// Full scan→diff→apply cycle.
    ///
    /// # Pipeline
    ///
    /// 1. **Scan**: collect ScannedEntry from each src location (list + hash)
    /// 2. **Diff**: compute_deltas(scanned, db_state) → FileDelta[]
    /// 3. **Apply**: upsert TrackedFile + create Transfers for each delta
    /// 4. **Delete**: filesystem-based local deletion detection
    async fn scan_and_register(
        &self,
        observer: &dyn SyncObserver,
    ) -> Result<(usize, Vec<ScanError>, ScanReport), SyncError> {
        let mut total_registered = 0usize;
        let mut all_errors = Vec::new();
        let mut all_entries = Vec::new();
        let mut scan_report = ScanReport::new();

        // Phase 1: Scan — collect ScannedEntries from all src locations
        let src_locations = self.engine.src_locations();
        let src_loc_names: Vec<String> =
            src_locations.iter().map(|(id, _)| id.to_string()).collect();
        tracing::info!(
            location_count = src_locations.len(),
            locations = ?src_loc_names,
            "scan: phase1 start — scanning locations"
        );
        observer.on_scan_start(src_locations.len());

        // Pre-fetch: DB state (needed for both incremental scan and diff)
        let db_files_list = self.file_store.list_files(None, None).await?;
        let db_files: HashMap<String, &TrackedFile> = db_files_list
            .iter()
            .map(|f| (f.relative_path().to_string(), f))
            .collect();
        tracing::info!(db_file_count = db_files.len(), "scan: DB state pre-fetched");
        observer.on_db_state_fetched(db_files.len());

        let location_total = src_locations.len();
        for (idx, (src_id, route)) in src_locations.iter().enumerate() {
            let loc_id = (*src_id).clone();
            observer.on_location_scan_start(src_id, idx, location_total);

            let scan_result = if src_id.is_local() {
                self.scan_local_entries(route, &db_files, observer).await
            } else if route.is_cloud_source() {
                self.scan_cloud_entries(src_id, route, observer).await
            } else {
                self.scan_remote_ssh_entries(src_id, route, observer).await
            };

            match scan_result {
                Ok((entries, errors)) => {
                    let entry_count = entries.len();
                    let error_count = errors.len();
                    tracing::info!(
                        location = %src_id,
                        entries = entry_count,
                        errors = error_count,
                        "scan: location done"
                    );
                    observer.on_location_scan_done(src_id, entry_count, error_count);
                    scan_report.record(
                        loc_id,
                        ScanOutcome::Scanned {
                            entries: entry_count,
                            errors: error_count,
                        },
                    );
                    all_entries.extend(entries);
                    all_errors.extend(errors);
                }
                Err(e) => {
                    tracing::error!(location = %src_id, error = %e, "scan failed for location");
                    observer.on_location_scan_failed(src_id, &e.to_string());
                    scan_report.record(
                        loc_id,
                        ScanOutcome::Failed {
                            error: e.to_string(),
                        },
                    );
                }
            }
        }

        // Phase 2: Diff — compare scanned entries against DB state
        let deltas = compute_deltas(&all_entries, &db_files);
        let added_count = deltas
            .iter()
            .filter(|d| matches!(d, FileDelta::Added(_)))
            .count();
        let modified_count = deltas
            .iter()
            .filter(|d| matches!(d, FileDelta::Modified(_)))
            .count();
        let removed_count = deltas
            .iter()
            .filter(|d| matches!(d, FileDelta::Removed(_)))
            .count();
        tracing::info!(
            added = added_count,
            modified = modified_count,
            removed = removed_count,
            total_scanned = all_entries.len(),
            "scan: phase2 diff computed"
        );
        // Per-delta debug: origin, path, hash for each Added/Modified
        for delta in &deltas {
            match delta {
                FileDelta::Added(a) => {
                    tracing::debug!(
                        delta = "added",
                        origin = %a.origin,
                        path = %a.relative_path,
                        hash = a.fingerprint.file_hash.as_deref().unwrap_or("none"),
                        size = a.fingerprint.size,
                        "scan: delta detail"
                    );
                }
                FileDelta::Modified(m) => {
                    tracing::debug!(
                        delta = "modified",
                        origin = %m.origin,
                        path = %m.relative_path,
                        old_hash = m.old_fingerprint.file_hash.as_deref().unwrap_or("none"),
                        new_hash = m.new_fingerprint.file_hash.as_deref().unwrap_or("none"),
                        old_size = m.old_fingerprint.size,
                        new_size = m.new_fingerprint.size,
                        "scan: delta detail"
                    );
                }
                FileDelta::Removed(r) => {
                    tracing::debug!(
                        delta = "removed",
                        origin = %r.origin,
                        path = %r.relative_path,
                        "scan: delta detail"
                    );
                }
            }
        }
        observer.on_diff_computed(&DeltaSummary {
            added: added_count,
            modified: modified_count,
            removed: removed_count,
        });

        // Phase 2.5: mtime backfill
        {
            let delta_paths: HashSet<&str> = deltas
                .iter()
                .map(|d| match d {
                    FileDelta::Added(a) => a.relative_path.as_str(),
                    FileDelta::Modified(m) => m.relative_path.as_str(),
                    FileDelta::Removed(r) => r.relative_path.as_str(),
                })
                .collect();
            let mut backfilled = 0usize;
            for entry in &all_entries {
                if delta_paths.contains(entry.relative_path.as_str()) {
                    continue;
                }
                if let Some(scan_mtime) = entry.fingerprint.modified_at {
                    if let Some(db_file) = db_files.get(&entry.relative_path) {
                        if db_file.modified_at().is_none() {
                            let mut updated = (*db_file).clone();
                            updated.set_modified_at(Some(scan_mtime));
                            self.file_store.upsert_file(&updated).await?;
                            backfilled += 1;
                        }
                    }
                }
            }
            if backfilled > 0 {
                tracing::info!(backfilled, "mtime backfill: populated missing modified_at");
            }
            observer.on_mtime_backfill(backfilled);
        }

        // Phase 3: Apply deltas + Plan transfers + Create transfers
        observer.on_apply_start(deltas.len());
        let topology: &dyn crate::domain::plan::Topology = &self.engine;
        for delta in &deltas {
            match delta {
                FileDelta::Added(added) => match self.apply_added(added).await {
                    Ok((file, is_duplicate)) => {
                        if !is_duplicate {
                            let planned = plan_transfers_for(
                                delta,
                                topology,
                                &HashMap::new(),
                                &HashSet::new(),
                            );
                            for pt in &planned {
                                tracing::info!(
                                    phase = "apply",
                                    delta = "added",
                                    path = %added.relative_path,
                                    origin = %added.origin,
                                    hash = added.fingerprint.file_hash.as_deref().unwrap_or("none"),
                                    transfer_src = %pt.src,
                                    transfer_dest = %pt.dest,
                                    kind = ?pt.kind,
                                    depends_on = ?pt.depends_on_index,
                                    "plan: transfer created"
                                );
                            }
                            if planned.is_empty() {
                                tracing::debug!(
                                    path = %added.relative_path,
                                    origin = %added.origin,
                                    "plan: no transfers needed (added)"
                                );
                            }
                            self.apply_planned_transfers(&planned, file.id()).await?;
                            total_registered += 1;
                        } else {
                            tracing::debug!(
                                path = %added.relative_path,
                                origin = %added.origin,
                                "plan: skipped (duplicate)"
                            );
                        }
                    }
                    Err(e) => {
                        all_errors.push(ScanError {
                            path: added.relative_path.clone(),
                            error: format!("apply added failed: {e}"),
                        });
                    }
                },
                FileDelta::Modified(modified) => match self.apply_modified(modified).await {
                    Ok(file) => {
                        let presence = self.presence_for(file.id()).await?;
                        let pending = self.pending_dests_for(file.id(), &modified.origin).await?;
                        tracing::debug!(
                            path = %modified.relative_path,
                            origin = %modified.origin,
                            presence = ?presence.iter().map(|(k,v)| (k.to_string(), format!("{:?}", v))).collect::<Vec<_>>(),
                            pending_dests = ?pending.iter().map(|l| l.to_string()).collect::<Vec<_>>(),
                            "plan: modified file presence/pending state"
                        );
                        let planned = plan_transfers_for(delta, topology, &presence, &pending);
                        for pt in &planned {
                            tracing::info!(
                                phase = "apply",
                                delta = "modified",
                                path = %modified.relative_path,
                                origin = %modified.origin,
                                old_hash = modified.old_fingerprint.file_hash.as_deref().unwrap_or("none"),
                                new_hash = modified.new_fingerprint.file_hash.as_deref().unwrap_or("none"),
                                transfer_src = %pt.src,
                                transfer_dest = %pt.dest,
                                kind = ?pt.kind,
                                "plan: transfer created"
                            );
                        }
                        self.apply_planned_transfers(&planned, file.id()).await?;
                        total_registered += 1;
                    }
                    Err(e) => {
                        all_errors.push(ScanError {
                            path: modified.relative_path.clone(),
                            error: format!("apply modified failed: {e}"),
                        });
                    }
                },
                FileDelta::Removed(_) => {
                    // Firestore型設計: スキャンによる自動削除は行わない。
                    // 削除は Store::delete() の明示呼び出しのみ。
                }
            }
        }
        observer.on_apply_done(total_registered, all_errors.len());

        // Note: 自動削除検出は行わない（Firestore型設計）。
        // FS上でファイルが消えてもTopologyからは削除しない。
        // 削除は Store::delete() の明示呼び出しのみ。

        Ok((total_registered, all_errors, scan_report))
    }

    // =========================================================================
    // Internal: scan entry collectors (Phase 1 — list + hash, no DB writes)
    // =========================================================================

    /// Scan local filesystem → ScannedEntry (hash once, no DB interaction).
    ///
    /// Incremental scan: files whose `(size, mtime)` match the DB record
    /// reuse cached hashes, skipping expensive `inspect_file()` I/O.
    /// Hash進捗報告の間隔（ファイル数）。
    const HASH_PROGRESS_INTERVAL: usize = 50;

    async fn scan_local_entries(
        &self,
        route: &TransferRoute,
        db_files: &HashMap<String, &TrackedFile>,
        observer: &dyn SyncObserver,
    ) -> Result<(Vec<ScannedEntry>, Vec<ScanError>), SyncError> {
        let local_root = route.src_file_root().to_path_buf();
        if !local_root.is_dir() {
            return Ok((Vec::new(), Vec::new()));
        }

        let mut entries = Vec::new();
        let mut errors = Vec::new();
        let mut skipped = 0usize;
        let mut hashed = 0usize;

        let files = match route.list_src_files().await {
            Ok(f) => f,
            Err(e) => {
                tracing::error!(
                    root = %local_root.display(),
                    error = %e,
                    "scan_local: list_src_files failed"
                );
                errors.push(ScanError {
                    path: local_root.display().to_string(),
                    error: format!("local scan failed: {e}"),
                });
                return Ok((Vec::new(), errors));
            }
        };

        let total = files.len();
        observer.on_location_listed(&LocationId::local(), total);

        for src_file in &files {
            if self
                .scan_excludes
                .iter()
                .any(|p| p.matches(&src_file.relative_path))
            {
                continue;
            }

            let file_type = std::path::Path::new(&src_file.relative_path)
                .extension()
                .and_then(|e| e.to_str())
                .map(FileType::from_extension)
                .unwrap_or(FileType::Asset);

            // Incremental scan: skip hashing if (size, mtime) unchanged
            if let (Some(scan_size), Some(scan_mtime)) = (src_file.size, src_file.modified_at) {
                if let Some(db_file) = db_files.get(&src_file.relative_path) {
                    if db_file.file_size() == scan_size
                        && db_file.modified_at() == Some(scan_mtime)
                        && db_file.has_real_file_hash()
                    {
                        let fp = db_file.fingerprint();
                        entries.push(ScannedEntry {
                            relative_path: src_file.relative_path.clone(),
                            file_type,
                            fingerprint: fp,
                            origin: LocationId::local(),
                            embedded_id: db_file.embedded_id().map(|s| s.to_string()),
                        });
                        skipped += 1;
                        continue;
                    }
                }
            }

            // Full hash required
            let abs_path = local_root.join(&src_file.relative_path);
            let path_display = abs_path.display().to_string();

            let (hash_result, file_size) = match self.inspect_file(&abs_path).await {
                Ok(v) => v,
                Err(e) => {
                    errors.push(ScanError {
                        path: path_display,
                        error: format!("hash failed: {e}"),
                    });
                    continue;
                }
            };

            hashed += 1;
            if hashed.is_multiple_of(Self::HASH_PROGRESS_INTERVAL) || hashed + skipped == total {
                observer.on_hash_progress(
                    &LocationId::local(),
                    &HashProgress {
                        hashed,
                        cached: skipped,
                        total,
                    },
                );
            }

            entries.push(ScannedEntry {
                relative_path: src_file.relative_path.clone(),
                file_type,
                fingerprint: FileFingerprint {
                    file_hash: Some(hash_result.file_hash),
                    content_hash: hash_result.content_hash,
                    meta_hash: None, // TODO: meta_hash抽出をスキャナに追加
                    size: file_size.unwrap_or(0),
                    modified_at: src_file.modified_at,
                },
                origin: LocationId::local(),
                embedded_id: None,
            });
        }

        if skipped > 0 {
            tracing::info!(
                total,
                skipped,
                hashed,
                "incremental scan: reused cached hashes"
            );
        }

        Ok((entries, errors))
    }

    /// Scan remote SSH host → ScannedEntry via batch_inspect (single task execution).
    ///
    /// 1. list_src_files → file list
    /// 2. exclude filter
    /// 3. batch_inspect (1 exec or 1 task_run for ALL files) → sha256 + size
    /// 4. Convert to ScannedEntry
    async fn scan_remote_ssh_entries(
        &self,
        src_id: &LocationId,
        route: &TransferRoute,
        observer: &dyn SyncObserver,
    ) -> Result<(Vec<ScannedEntry>, Vec<ScanError>), SyncError> {
        let mut errors = Vec::new();

        let files = match route.list_src_files().await {
            Ok(f) => f,
            Err(e) => {
                let msg = format!("remote scan failed: {e}");
                tracing::error!(
                    src = %src_id,
                    root = %route.src_file_root().display(),
                    error = %e,
                    "scan_remote_ssh: list_src_files failed"
                );
                errors.push(ScanError {
                    path: format!("{}:{}", src_id, route.src_file_root().display()),
                    error: msg,
                });
                return Ok((Vec::new(), errors));
            }
        };

        observer.on_location_listed(src_id, files.len());

        // Filter out excluded paths, collect relative paths for batch inspect.
        let relative_paths: Vec<String> = files
            .into_iter()
            .filter(|f| {
                !self
                    .scan_excludes
                    .iter()
                    .any(|p| p.matches(&f.relative_path))
            })
            .map(|f| f.relative_path)
            .collect();

        let total = relative_paths.len();

        // Get the shell from route — required for remote scan.
        let shell = route.src_shell().ok_or_else(|| -> SyncError {
            crate::infra::error::InfraError::Transfer {
                reason: format!("scan_remote_ssh: route has no src_shell (src={})", src_id),
            }
            .into()
        })?;

        let root_str = route
            .src_file_root()
            .to_str()
            .unwrap_or_default()
            .to_string();

        // Single batch call: 1 exec (or 1 task_run on RunPod) for ALL files.
        let inspections = match shell.batch_inspect(&root_str, &relative_paths).await {
            Ok(v) => v,
            Err(e) => {
                tracing::error!(
                    src = %src_id,
                    file_count = total,
                    error = %e,
                    "scan_remote_ssh: batch_inspect failed"
                );
                errors.push(ScanError {
                    path: format!("{}:{}", src_id, root_str),
                    error: format!("batch_inspect failed: {e}"),
                });
                return Ok((Vec::new(), errors));
            }
        };

        observer.on_hash_progress(
            src_id,
            &HashProgress {
                hashed: inspections.len(),
                cached: 0,
                total,
            },
        );

        let entries: Vec<ScannedEntry> = inspections
            .into_iter()
            .map(|fi| {
                let file_type = std::path::Path::new(&fi.relative_path)
                    .extension()
                    .and_then(|e| e.to_str())
                    .map(FileType::from_extension)
                    .unwrap_or(FileType::Asset);

                ScannedEntry {
                    relative_path: fi.relative_path,
                    file_type,
                    fingerprint: FileFingerprint {
                        file_hash: Some(fi.sha256),
                        content_hash: None, // Not available remotely
                        meta_hash: None,    // Not available remotely
                        size: fi.size,
                        modified_at: None,
                    },
                    origin: src_id.clone(),
                    embedded_id: None,
                }
            })
            .collect();

        Ok((entries, errors))
    }

    /// Scan Cloud storage → ScannedEntry (metadata only, no DB interaction).
    ///
    /// Cloud files have no file_hash (would require downloading each file).
    /// Uses size + modified_at for change detection via fingerprint comparison.
    async fn scan_cloud_entries(
        &self,
        src_id: &LocationId,
        route: &TransferRoute,
        observer: &dyn SyncObserver,
    ) -> Result<(Vec<ScannedEntry>, Vec<ScanError>), SyncError> {
        let mut entries = Vec::new();
        let mut errors = Vec::new();

        let files = match route.list_src_files().await {
            Ok(f) => f,
            Err(e) => {
                errors.push(ScanError {
                    path: format!("{}:{}", src_id, route.src_file_root().display()),
                    error: format!("cloud scan failed: {e}"),
                });
                return Ok((Vec::new(), errors));
            }
        };

        observer.on_location_listed(src_id, files.len());

        for src_file in files {
            if self
                .scan_excludes
                .iter()
                .any(|p| p.matches(&src_file.relative_path))
            {
                continue;
            }

            let file_type = std::path::Path::new(&src_file.relative_path)
                .extension()
                .and_then(|e| e.to_str())
                .map(FileType::from_extension)
                .unwrap_or(FileType::Asset);

            entries.push(ScannedEntry {
                relative_path: src_file.relative_path,
                file_type,
                fingerprint: FileFingerprint {
                    file_hash: None,
                    content_hash: None,
                    meta_hash: None,
                    size: src_file.size.unwrap_or(0),
                    modified_at: src_file.modified_at,
                },
                origin: src_id.clone(),
                embedded_id: None,
            });
        }

        Ok((entries, errors))
    }

    // =========================================================================
    // Internal: apply helpers (Phase 3 — DB upsert from FileDelta)
    // =========================================================================

    /// Apply an Added delta: create or re-register TrackedFile.
    ///
    /// Returns (TrackedFile, is_duplicate). Duplicate = same hash exists at
    /// another path, so transfers are unnecessary (content already synced).
    async fn apply_added(&self, added: &AddedFile) -> Result<(TrackedFile, bool), SyncError> {
        // Re-registration of a deleted file
        if let Some(mut existing) = self
            .file_store
            .get_file_by_path(&added.relative_path)
            .await?
        {
            if existing.is_deleted() {
                existing.unmark_deleted();
            }
            if let Some(ref hash) = added.fingerprint.file_hash {
                existing.update_from_scan(
                    added.file_type,
                    hash.clone(),
                    added.fingerprint.content_hash.clone(),
                    added.fingerprint.size,
                    added.embedded_id.clone(),
                );
                existing.set_modified_at(added.fingerprint.modified_at);
            } else {
                existing
                    .update_from_cloud_scan(added.fingerprint.size, added.fingerprint.modified_at);
            }
            self.file_store.upsert_file(&existing).await?;
            return Ok((existing, false));
        }

        // New file
        if let Some(ref hash) = added.fingerprint.file_hash {
            // Local/SSH: check duplicate by hash
            let duplicate = self
                .file_store
                .find_duplicate_file(
                    hash,
                    added.fingerprint.content_hash.as_deref(),
                    &added.relative_path,
                )
                .await?;
            let is_duplicate = duplicate.is_some();

            let mut file = TrackedFile::from_scan(
                added.relative_path.clone(),
                added.file_type,
                hash.clone(),
                added.fingerprint.content_hash.clone(),
                added.fingerprint.size,
                added.embedded_id.clone(),
            )?;
            file.set_modified_at(added.fingerprint.modified_at);
            self.file_store.upsert_file(&file).await?;
            Ok((file, is_duplicate))
        } else {
            // Cloud: no hash, use from_cloud_scan
            let file = TrackedFile::from_cloud_scan(
                added.relative_path.clone(),
                added.file_type,
                added.fingerprint.size,
                added.fingerprint.modified_at,
            )?;
            self.file_store.upsert_file(&file).await?;
            Ok((file, false))
        }
    }

    /// Apply a Modified delta: update existing TrackedFile with new fingerprint.
    async fn apply_modified(&self, modified: &ModifiedFile) -> Result<TrackedFile, SyncError> {
        let mut existing = self
            .file_store
            .get_file_by_path(&modified.relative_path)
            .await?
            .ok_or_else(|| SyncError::NotRegistered(modified.relative_path.clone()))?;

        if let Some(ref hash) = modified.new_fingerprint.file_hash {
            existing.update_from_scan(
                modified.file_type,
                hash.clone(),
                modified.new_fingerprint.content_hash.clone(),
                modified.new_fingerprint.size,
                modified.embedded_id.clone(),
            );
            existing.set_modified_at(modified.new_fingerprint.modified_at);
        } else {
            existing.update_from_cloud_scan(
                modified.new_fingerprint.size,
                modified.new_fingerprint.modified_at,
            );
        }
        self.file_store.upsert_file(&existing).await?;

        Ok(existing)
    }

    // =========================================================================
    // Internal: plan helpers (PlannedTransfer → Transfer)
    // =========================================================================

    /// PlannedTransfer[] から Transfer を作成し DB に挿入する。
    ///
    /// `actual_file_id` で PlannedTransfer.file_id を上書きする。
    /// Added の場合、delta の仮UUID と TrackedFile の実UUID が異なるため。
    async fn apply_planned_transfers(
        &self,
        planned: &[PlannedTransfer],
        actual_file_id: &str,
    ) -> Result<usize, SyncError> {
        let mut created = 0usize;
        // Transfer IDsを保持（depends_on_indexから実IDへの解決用）
        let mut transfer_ids: Vec<String> = Vec::with_capacity(planned.len());

        for (i, pt) in planned.iter().enumerate() {
            let depends_on_id = pt
                .depends_on_index
                .and_then(|idx| transfer_ids.get(idx))
                .cloned();

            let transfer = if let Some(dep_id) = depends_on_id {
                Transfer::with_dependency(
                    actual_file_id.to_string(),
                    pt.src.clone(),
                    pt.dest.clone(),
                    pt.kind,
                    dep_id,
                )?
            } else {
                match pt.kind {
                    TransferKind::Sync => {
                        Transfer::new(actual_file_id.to_string(), pt.src.clone(), pt.dest.clone())?
                    }
                    TransferKind::Delete => Transfer::new_delete(
                        actual_file_id.to_string(),
                        pt.src.clone(),
                        pt.dest.clone(),
                    )?,
                }
            };

            transfer_ids.push(transfer.id().to_string());
            self.transfer_store.insert_transfer(&transfer).await?;
            created += 1;

            // depends_on_index が i より前のインデックスを指さない場合は論理エラーだが
            // ここではサイレントに無視（plan側で保証されるべき不変条件）
            let _ = i;
        }
        Ok(created)
    }

    /// 指定ファイルの未完了Transfer先を取得（重複Transfer抑止用）。
    async fn pending_dests_for(
        &self,
        file_id: &str,
        origin: &LocationId,
    ) -> Result<HashSet<LocationId>, SyncError> {
        let transfers = self
            .transfer_store
            .latest_transfers_by_file(file_id)
            .await?;
        Ok(transfers
            .iter()
            .filter(|t| t.src() == origin && !matches!(t.state(), TransferState::Completed))
            .map(|t| t.dest().clone())
            .collect())
    }

    /// 指定ファイルの各locationでのPresenceStateを取得。
    ///
    /// plan_transfers_for に渡すための presence map を構築する。
    async fn presence_for(
        &self,
        file_id: &str,
    ) -> Result<HashMap<LocationId, PresenceState>, SyncError> {
        let transfers = self
            .transfer_store
            .latest_transfers_by_file(file_id)
            .await?;
        let mut map = HashMap::new();
        for t in &transfers {
            // src は Present（ファイルの出元）
            map.entry(t.src().clone()).or_insert(PresenceState::Present);
            // dest は Transfer 状態から導出
            let dest_state = PresenceState::from_transfer(t, &self.config.retry_policy());
            map.entry(t.dest().clone())
                .and_modify(|existing| {
                    if dest_state.priority() > existing.priority() {
                        *existing = dest_state;
                    }
                })
                .or_insert(dest_state);
        }
        Ok(map)
    }

    // =========================================================================
    // Internal: infrastructure helpers
    // =========================================================================

    fn to_relative(&self, absolute_path: &Path) -> Result<String, SyncError> {
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
        relative
            .to_str()
            .map(|s| s.to_string())
            .ok_or_else(|| -> SyncError {
                InfraError::Transfer {
                    reason: format!(
                        "relative path is not valid UTF-8: {}",
                        relative.to_string_lossy()
                    ),
                }
                .into()
            })
    }

    /// Resolve a path string to relative form (absolute → strip prefix, relative → as-is).
    fn resolve_to_relative(&self, path: &str) -> String {
        let p = Path::new(path);
        if p.is_absolute() {
            self.to_relative(p).unwrap_or_else(|_| path.to_string())
        } else {
            path.to_string()
        }
    }

    async fn assert_file_exists(&self, path: &Path) -> Result<(), SyncError> {
        match tokio::fs::try_exists(path).await {
            Ok(true) => Ok(()),
            Ok(false) => Err(InfraError::FileNotFound(path.to_path_buf()).into()),
            Err(e) => Err(SyncError::from(e)),
        }
    }

    async fn inspect_file(&self, path: &Path) -> Result<(HashResult, Option<u64>), SyncError> {
        let hasher = Arc::clone(&self.hasher);
        let hash_path = path.to_path_buf();
        let hash_result = tokio::task::spawn_blocking(move || hasher.hash_file(&hash_path))
            .await
            .map_err(|e| -> SyncError {
                InfraError::Hash {
                    op: "hasher",
                    reason: format!("spawn_blocking join failed: {e}"),
                }
                .into()
            })??;
        let file_size = Some(tokio::fs::metadata(path).await?.len());
        Ok((hash_result, file_size))
    }

    /// Create transfers for ALL route edges in the graph.
    ///
    /// Every edge (src→dest) that has a registered route gets a Transfer,
    /// unless a non-completed transfer already exists for this file on that edge.
    /// This ensures N-location mesh: file registered anywhere → transfers
    /// created for every route pair.
    /// origin から直接到達可能な destination にのみ Sync Transfer を作成。
    ///
    /// チェーン転送（例: local→cloud→pod）は `TransferEngine::create_next_hop_transfers()`
    /// が transfer 完了時に動的作成するため、ここでは 1-hop のみ計画する。
    ///
    /// 重複防止: 同一 (origin, dest) に未完了 Transfer が既にある場合はスキップ。
    /// Domain層 plan_sync に委譲してSync Transferを計画・適用する。
    ///
    /// `stale_presence`:
    /// - `false` (Added): Presentなdestはスキップ
    /// - `true` (Modified): 全destに再送
    async fn plan_and_apply_sync(
        &self,
        file: &TrackedFile,
        origin: &LocationId,
        stale_presence: bool,
    ) -> Result<usize, SyncError> {
        let topology: &dyn crate::domain::plan::Topology = &self.engine;

        // 既存Transferからpresence/pending_destsを構築
        let existing = self
            .transfer_store
            .latest_transfers_by_file(file.id())
            .await?;
        let retry_policy = self.config.retry_policy();

        let mut presence: HashMap<LocationId, PresenceState> = HashMap::new();
        let mut pending_dests: HashSet<LocationId> = HashSet::new();

        // origin is Present
        presence.insert(origin.clone(), PresenceState::Present);

        for t in &existing {
            let dest_state = PresenceState::from_transfer(t, &retry_policy);
            presence.insert(t.dest().clone(), dest_state);
            if matches!(dest_state, PresenceState::Pending | PresenceState::Syncing)
                && t.src() == origin
            {
                pending_dests.insert(t.dest().clone());
            }
        }

        let planned = crate::domain::plan::plan_sync(
            file.id(),
            origin,
            topology,
            &presence,
            &pending_dests,
            stale_presence,
        );
        self.apply_planned_transfers(&planned, file.id()).await
    }

    /// Domain層 plan_delete に委譲してDelete Transferを計画・適用する。
    async fn plan_and_apply_delete(
        &self,
        file: &TrackedFile,
        origin: &LocationId,
    ) -> Result<usize, SyncError> {
        let topology: &dyn crate::domain::plan::Topology = &self.engine;

        // 既存Transferからpending_destsを構築
        let existing = self
            .transfer_store
            .latest_transfers_by_file(file.id())
            .await?;
        let pending_dests: HashSet<LocationId> = existing
            .iter()
            .filter(|t| t.src() == origin && !matches!(t.state(), TransferState::Completed))
            .map(|t| t.dest().clone())
            .collect();

        let planned = crate::domain::plan::plan_delete(file.id(), origin, topology, &pending_dests);
        self.apply_planned_transfers(&planned, file.id()).await
    }

    async fn build_file_view(&self, file: TrackedFile) -> Result<FileView, SyncError> {
        let transfers = self
            .transfer_store
            .latest_transfers_by_file(file.id())
            .await?;

        // Deduplicate per location, keeping the most informative state.
        // Same logic as status() — HashMap ensures one entry per location.
        let mut location_map: HashMap<LocationId, PresenceView> = HashMap::new();

        for t in &transfers {
            // src locations are Present
            location_map
                .entry(t.src().clone())
                .or_insert_with(|| PresenceView {
                    location: t.src().clone(),
                    state: PresenceState::Present,
                    error: None,
                    synced_at: None,
                    attempt: 0,
                });

            // dest locations derive state from transfer
            let dest_state = PresenceState::from_transfer(t, &self.config.retry_policy());
            location_map
                .entry(t.dest().clone())
                .and_modify(|existing| {
                    // Keep the more informative state (higher priority wins)
                    if dest_state.priority() > existing.state.priority() {
                        existing.state = dest_state;
                        existing.error = t.error().map(|s| s.to_string());
                        existing.synced_at = t
                            .finished_at()
                            .filter(|_| t.state() == TransferState::Completed);
                        existing.attempt = t.attempt();
                    }
                })
                .or_insert_with(|| PresenceView {
                    location: t.dest().clone(),
                    state: dest_state,
                    error: t.error().map(|s| s.to_string()),
                    synced_at: t
                        .finished_at()
                        .filter(|_| t.state() == TransferState::Completed),
                    attempt: t.attempt(),
                });
        }

        let presences = location_map.into_values().collect();
        Ok(FileView { file, presences })
    }
}

// =============================================================================
// StoreBuilder
// =============================================================================

/// Builder for [`Store`] with automatic remote registration.
///
/// Collects routes and remote configs, then builds the database in one step.
/// Remote configs are persisted to the store during [`build()`](Self::build).
#[deprecated(note = "use SyncFacadeBuilder")]
pub struct StoreBuilder {
    file_store: Arc<dyn FileStore>,
    transfer_store: Arc<dyn TransferStore>,
    remote_store: Arc<dyn RemoteStore>,
    routes: Vec<TransferRoute>,
    remotes: Vec<RemoteConfig>,
    hasher: Option<Arc<dyn ContentHasher>>,
    config: Option<SyncConfig>,
    scan_excludes: Vec<glob::Pattern>,
}

impl StoreBuilder {
    /// Start building a Store with the given stores.
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
            config: None,
            scan_excludes: Vec::new(),
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
    ///
    /// Convenience method — sets `max_attempts` on the underlying `SyncConfig`.
    pub fn retry_policy(mut self, policy: RetryPolicy) -> Self {
        let mut cfg = self.config.unwrap_or_default();
        cfg.max_attempts = policy.max_attempts();
        self.config = Some(cfg);
        self
    }

    /// Set the full sync configuration.
    ///
    /// Overrides any previously set retry_policy.
    pub fn sync_config(mut self, config: SyncConfig) -> Self {
        self.config = Some(config);
        self
    }

    /// Add a glob pattern to exclude from scan.
    ///
    /// Patterns are matched against relative paths (from `local_root`).
    /// Invalid patterns are silently ignored.
    pub fn exclude(mut self, pattern: &str) -> Self {
        if let Ok(p) = glob::Pattern::new(pattern) {
            self.scan_excludes.push(p);
        }
        self
    }

    /// Build the Store, registering all remotes in the store.
    pub async fn build(self) -> Result<Store, SyncError> {
        for remote in &self.remotes {
            self.remote_store.register_remote(remote).await?;
        }

        let config = self.config.unwrap_or_default();
        let engine = TransferEngine::new(self.routes, config.concurrency);

        let topology_files: Arc<dyn TopologyFileStore> =
            Arc::new(FileStoreAdapter(self.file_store.clone()));

        Ok(Store {
            file_store: self.file_store,
            topology_files,
            transfer_store: self.transfer_store,
            remote_store: self.remote_store,
            engine,
            hasher: self.hasher.unwrap_or_else(|| Arc::new(Djb2Hasher)),
            config,
            scan_excludes: self.scan_excludes,
        })
    }
}

// =============================================================================
// FileStore → TopologyFileStore adapter (deprecated Store互換用)
// =============================================================================

/// FileStore → TopologyFileStore bridge。
///
/// 旧Store内でTransferEngineを呼ぶため、TrackedFile→TopologyFileの最小変換を行う。
/// 旧Store自体がdeprecated — SyncFacade移行完了後に除去。
struct FileStoreAdapter(Arc<dyn FileStore>);

impl FileStoreAdapter {
    fn convert(tf: &TrackedFile) -> crate::domain::topology_file::TopologyFile {
        crate::domain::topology_file::TopologyFile::reconstitute(
            tf.id().to_string(),
            tf.relative_path().to_string(),
            tf.content_hash().map(|s| s.to_string()),
            tf.file_type(),
            tf.registered_at(),
            tf.deleted_at(),
        )
    }
}

#[async_trait::async_trait]
impl TopologyFileStore for FileStoreAdapter {
    async fn upsert(
        &self,
        _file: &crate::domain::topology_file::TopologyFile,
    ) -> Result<(), SyncError> {
        Err(SyncError::Domain(DomainError::Validation {
            field: "upsert".into(),
            reason: "FileStoreAdapter: read-only bridge".into(),
        }))
    }

    async fn get_by_id(
        &self,
        id: &str,
    ) -> Result<Option<crate::domain::topology_file::TopologyFile>, SyncError> {
        let tf = self.0.get_file_by_id(id).await?;
        Ok(tf.as_ref().map(Self::convert))
    }

    async fn get_by_path(
        &self,
        relative_path: &str,
    ) -> Result<Option<crate::domain::topology_file::TopologyFile>, SyncError> {
        let tf = self.0.get_file_by_path(relative_path).await?;
        Ok(tf.as_ref().map(Self::convert))
    }

    async fn find_by_canonical_hash(
        &self,
        _hash: &str,
    ) -> Result<Option<crate::domain::topology_file::TopologyFile>, SyncError> {
        Ok(None)
    }

    async fn list_active(
        &self,
        file_type: Option<FileType>,
        limit: Option<usize>,
    ) -> Result<Vec<crate::domain::topology_file::TopologyFile>, SyncError> {
        let tfs = self.0.list_files(file_type, limit).await?;
        Ok(tfs.iter().map(Self::convert).collect())
    }

    async fn list_deleted(
        &self,
    ) -> Result<Vec<crate::domain::topology_file::TopologyFile>, SyncError> {
        Ok(Vec::new())
    }

    async fn count_active(&self) -> Result<usize, SyncError> {
        self.0.count_files().await
    }

    async fn list_active_paths(&self) -> Result<Vec<String>, SyncError> {
        self.0.list_all_paths().await
    }
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::infra::backend::memory::InMemoryBackend;
    use std::path::PathBuf;
    use std::sync::Arc;

    #[cfg(feature = "sqlite")]
    use crate::infra::sqlite::SqliteSyncStore;

    #[cfg(feature = "sqlite")]
    async fn test_db_with_dir(dir: &Path) -> (Store, Arc<InMemoryBackend>) {
        let store = SqliteSyncStore::open_in_memory().await.unwrap();
        let cloud_backend = Arc::new(InMemoryBackend::default());

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

        let routes = vec![TransferRoute::new(
            LocationId::local(),
            LocationId::new("cloud").unwrap(),
            dir.to_path_buf(),
            PathBuf::from("remote/output"),
            Box::new(Arc::clone(&cloud_backend)),
        )];

        let store = Arc::new(store);
        let db = StoreBuilder::new(
            store.clone() as Arc<dyn FileStore>,
            store.clone() as Arc<dyn TransferStore>,
            store.clone() as Arc<dyn RemoteStore>,
        )
        .routes(routes)
        .build()
        .await
        .unwrap();

        (db, cloud_backend)
    }

    // --- put: local file ---

    #[cfg(feature = "sqlite")]
    #[tokio::test]
    async fn put_local_new_file() {
        let dir = tempfile::tempdir().unwrap();
        let (db, _backend) = test_db_with_dir(dir.path()).await;

        let path = dir.path().join("test.json");
        std::fs::write(&path, b"{}").unwrap();

        let result = db
            .put(
                "test.json",
                FileType::Asset,
                PutOptions {
                    embedded_id: Some("gen-1".into()),
                    ..Default::default()
                },
            )
            .await
            .unwrap();

        assert!(!result.is_duplicate);
        assert_eq!(result.file.file_type(), FileType::Asset);
        assert_eq!(result.file.relative_path(), "test.json");
        assert_eq!(result.transfers_created, 1);

        // Verify via get()
        let view = db.get("test.json").await.unwrap().unwrap();
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
    async fn put_local_nonexistent_file() {
        let dir = tempfile::tempdir().unwrap();
        let (db, _) = test_db_with_dir(dir.path()).await;
        let result = db
            .put("no/such/file.png", FileType::Image, PutOptions::default())
            .await;
        assert!(matches!(
            result,
            Err(SyncError::Infra(InfraError::FileNotFound(_)))
        ));
    }

    // --- put: remote file ---

    #[cfg(feature = "sqlite")]
    async fn test_db_with_remote_source(
        mock_shell: Box<dyn crate::infra::shell::RemoteShell>,
    ) -> (Store, Arc<InMemoryBackend>) {
        let store = SqliteSyncStore::open_in_memory().await.unwrap();
        let cloud_backend = Arc::new(InMemoryBackend::default());

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

        let routes = vec![TransferRoute::new(
            LocationId::new("pod").unwrap(),
            LocationId::new("cloud").unwrap(),
            PathBuf::from("/workspace/output"),
            PathBuf::from("vdsl/output"),
            Box::new(Arc::clone(&cloud_backend)),
        )
        .with_src_shell(mock_shell)];

        let store = Arc::new(store);
        let db = StoreBuilder::new(
            store.clone() as Arc<dyn FileStore>,
            store.clone() as Arc<dyn TransferStore>,
            store.clone() as Arc<dyn RemoteStore>,
        )
        .routes(routes)
        .build()
        .await
        .unwrap();

        (db, cloud_backend)
    }

    #[cfg(feature = "sqlite")]
    #[tokio::test]
    async fn put_remote_and_sync() {
        use crate::infra::shell::mock::MockShell;

        let shell = MockShell::new(vec!["/workspace/output/gen-001.png"]);
        let (db, backend) = test_db_with_remote_source(Box::new(shell)).await;

        let result = db
            .put(
                "gen-001.png",
                FileType::Image,
                PutOptions {
                    source: Some(LocationId::new("pod").unwrap()),
                    file_hash: Some("hash_remote".into()),
                    file_size: Some(1024),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        assert_eq!(result.file.relative_path(), "gen-001.png");
        assert_eq!(result.transfers_created, 1);

        let sync_result = db.sync(None).await.unwrap();
        assert_eq!(sync_result.batch.transferred, 1);
        assert_eq!(sync_result.batch.failed, 0);

        let log = backend.log.lock().await;
        assert_eq!(log.len(), 1);
        match &log[0] {
            crate::infra::backend::memory::Op::Push { remote, .. } => {
                assert_eq!(remote, "vdsl/output/gen-001.png");
            }
            _ => panic!("expected Push op"),
        }

        let view = db.get("gen-001.png").await.unwrap().unwrap();
        assert_eq!(
            view.presence_state(&LocationId::new("cloud").unwrap()),
            Some(PresenceState::Present)
        );
    }

    #[cfg(feature = "sqlite")]
    #[tokio::test]
    async fn put_remote_requires_file_hash() {
        use crate::infra::shell::mock::MockShell;

        let shell = MockShell::new(Vec::<String>::new());
        let (db, _) = test_db_with_remote_source(Box::new(shell)).await;

        let result = db
            .put(
                "test.png",
                FileType::Image,
                PutOptions {
                    source: Some(LocationId::new("pod").unwrap()),
                    // file_hash is missing
                    ..Default::default()
                },
            )
            .await;
        assert!(matches!(
            result,
            Err(SyncError::Domain(DomainError::Validation { .. }))
        ));
    }

    // --- sync ---

    #[cfg(feature = "sqlite")]
    #[tokio::test]
    async fn sync_pushes_pending() {
        let dir = tempfile::tempdir().unwrap();
        let (db, backend) = test_db_with_dir(dir.path()).await;

        let path = dir.path().join("push.json");
        std::fs::write(&path, b"data").unwrap();

        db.put("push.json", FileType::Asset, PutOptions::default())
            .await
            .unwrap();

        let cloud = LocationId::new("cloud").unwrap();
        let result = db.sync(None).await.unwrap();
        assert_eq!(result.batch.transferred, 1);
        assert_eq!(result.batch.failed, 0);
        assert!(result.scan_errors.is_empty());

        let log = backend.log.lock().await;
        assert_eq!(log.len(), 1);
        match &log[0] {
            crate::infra::backend::memory::Op::Push { remote, .. } => {
                assert_eq!(remote, "remote/output/push.json");
            }
            _ => panic!("expected Push op"),
        }

        let view = db.get("push.json").await.unwrap().unwrap();
        assert_eq!(view.presence_state(&cloud), Some(PresenceState::Present));
    }

    #[cfg(feature = "sqlite")]
    #[tokio::test]
    async fn sync_failure_records_error() {
        let dir = tempfile::tempdir().unwrap();
        let (db, backend) = test_db_with_dir(dir.path()).await;

        let path = dir.path().join("fail.json");
        std::fs::write(&path, b"data").unwrap();

        db.put("fail.json", FileType::Asset, PutOptions::default())
            .await
            .unwrap();

        *backend.fail_next.lock().await = true;

        let result = db.sync(None).await.unwrap();
        assert_eq!(result.batch.failed, 1);
        assert_eq!(result.batch.transferred, 0);

        // First failure is retryable (within retry limit) → appears as pending, not error
        let summary = db.status().await.unwrap();
        assert!(
            !summary.pending_entries.is_empty(),
            "retryable failure should appear in pending_entries"
        );
        assert!(
            summary.error_entries.is_empty(),
            "not yet exhausted — should not be in error_entries"
        );
    }

    // --- status ---

    #[cfg(feature = "sqlite")]
    #[tokio::test]
    async fn status_summary() {
        let dir = tempfile::tempdir().unwrap();
        let (db, _) = test_db_with_dir(dir.path()).await;

        for (name, content) in &[("a.json", &b"data_a"[..]), ("b.json", &b"data_b"[..])] {
            let p = dir.path().join(name);
            std::fs::write(&p, content).unwrap();
            db.put(name, FileType::Asset, PutOptions::default())
                .await
                .unwrap();
        }

        let summary = db.status().await.unwrap();
        assert_eq!(summary.total_entries, 2);

        let local = summary.locations.get(&LocationId::local()).unwrap();
        assert_eq!(local.present, 2);

        let cloud = summary
            .locations
            .get(&LocationId::new("cloud").unwrap())
            .unwrap();
        assert_eq!(cloud.pending, 2);
    }

    // --- get: absolute path resolution ---

    #[cfg(feature = "sqlite")]
    #[tokio::test]
    async fn get_accepts_absolute_path() {
        let dir = tempfile::tempdir().unwrap();
        let (db, _) = test_db_with_dir(dir.path()).await;

        let path = dir.path().join("abs.json");
        std::fs::write(&path, b"{}").unwrap();

        db.put("abs.json", FileType::Asset, PutOptions::default())
            .await
            .unwrap();

        // Get by absolute path
        let view = db.get(path.to_str().unwrap()).await.unwrap();
        assert!(view.is_some());

        // Get by relative path
        let view2 = db.get("abs.json").await.unwrap();
        assert!(view2.is_some());
    }

    // --- put/get accept absolute paths ---

    #[cfg(feature = "sqlite")]
    #[tokio::test]
    async fn put_accepts_absolute_path() {
        let dir = tempfile::tempdir().unwrap();
        let (db, _) = test_db_with_dir(dir.path()).await;

        let img_path = dir.path().join("output/abs-test.png");
        std::fs::create_dir_all(img_path.parent().unwrap()).unwrap();
        std::fs::write(&img_path, b"test-data").unwrap();

        // put with absolute path
        let result = db
            .put(
                img_path.to_str().unwrap(),
                FileType::Image,
                PutOptions::default(),
            )
            .await
            .unwrap();

        // Stored as relative path
        assert_eq!(result.file.relative_path(), "output/abs-test.png");

        // get with relative path finds the same file
        let view = db.get("output/abs-test.png").await.unwrap();
        assert!(view.is_some());
    }

    // --- build_file_view: presences deduplication ---

    #[cfg(feature = "sqlite")]
    #[tokio::test]
    async fn file_view_presences_deduplicated() {
        use crate::infra::sqlite::SqliteSyncStore;

        let dir = tempfile::tempdir().unwrap();
        let store = SqliteSyncStore::open_in_memory().await.unwrap();
        let cloud_backend = Arc::new(InMemoryBackend::default());

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

        // Bidirectional routes: local↔cloud
        let routes = vec![
            TransferRoute::new(
                LocationId::local(),
                LocationId::new("cloud").unwrap(),
                dir.path().to_path_buf(),
                PathBuf::from("remote/output"),
                Box::new(Arc::clone(&cloud_backend)),
            ),
            TransferRoute::new(
                LocationId::new("cloud").unwrap(),
                LocationId::local(),
                PathBuf::from("remote/output"),
                dir.path().to_path_buf(),
                Box::new(Arc::clone(&cloud_backend)),
            )
            .direction(crate::TransferDirection::Pull),
        ];

        let store = Arc::new(store);
        let db = StoreBuilder::new(
            store.clone() as Arc<dyn FileStore>,
            store.clone() as Arc<dyn TransferStore>,
            store.clone() as Arc<dyn RemoteStore>,
        )
        .routes(routes)
        .build()
        .await
        .unwrap();

        // Create a file and put it (creates transfers for origin→direct_dests only)
        let path = dir.path().join("dedup.json");
        std::fs::write(&path, b"test-data").unwrap();
        db.put("dedup.json", FileType::Asset, PutOptions::default())
            .await
            .unwrap();

        // Execute sync to complete transfers
        let result = db.sync(None).await.unwrap();
        assert!(result.batch.transferred > 0 || result.batch.failed > 0);

        // get() should return deduplicated presences
        let view = db.get("dedup.json").await.unwrap().unwrap();
        let locations: Vec<&LocationId> = view.presences.iter().map(|p| &p.location).collect();

        // Each location should appear at most once
        let mut seen = std::collections::HashSet::new();
        for loc in &locations {
            assert!(seen.insert(*loc), "duplicate location in presences: {loc}");
        }

        // Should have exactly 2 locations: local + cloud
        assert_eq!(
            seen.len(),
            2,
            "expected 2 unique locations, got {}: {:?}",
            seen.len(),
            locations
        );
    }

    /// 3拠点チェーン(local→cloud→pod)でput時にTransferがorigin→directのみ作成され、
    /// next-hopはexecute時に動的作成されることを検証。
    ///
    /// E-09 regression: 以前は全辺にTransfer発行 → cloud.presentが2倍になっていた。
    #[cfg(feature = "sqlite")]
    #[tokio::test]
    async fn chain_transfer_origin_only() {
        use crate::infra::sqlite::SqliteSyncStore;

        let dir = tempfile::tempdir().unwrap();
        let store = SqliteSyncStore::open_in_memory().await.unwrap();
        let cloud_backend = Arc::new(InMemoryBackend::default());
        let pod_backend = Arc::new(InMemoryBackend::default());

        for id in &["cloud", "pod"] {
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

        // 3拠点チェーン: local→cloud→pod
        // cloud→pod はPull方向（cloudがremote source, podがrclone実行ホスト）
        let routes = vec![
            TransferRoute::new(
                LocationId::local(),
                LocationId::new("cloud").unwrap(),
                dir.path().to_path_buf(),
                PathBuf::from("remote/output"),
                Box::new(Arc::clone(&cloud_backend)),
            ),
            TransferRoute::new(
                LocationId::new("cloud").unwrap(),
                LocationId::new("pod").unwrap(),
                PathBuf::from("remote/output"),
                PathBuf::from("/workspace/output"),
                Box::new(Arc::clone(&pod_backend)),
            )
            .direction(crate::TransferDirection::Pull),
        ];

        let store = Arc::new(store);
        let db = StoreBuilder::new(
            store.clone() as Arc<dyn FileStore>,
            store.clone() as Arc<dyn TransferStore>,
            store.clone() as Arc<dyn RemoteStore>,
        )
        .routes(routes)
        .build()
        .await
        .unwrap();

        // put: origin=local → local→cloud のTransferのみ作成される
        let path = dir.path().join("chain.json");
        std::fs::write(&path, b"chain-data").unwrap();
        let put_result = db
            .put("chain.json", FileType::Asset, PutOptions::default())
            .await
            .unwrap();

        // origin=local, optimal_tree(local, {cloud, pod}) → 2件
        // local→cloud (Queued) + cloud→pod (Blocked, depends on local→cloud)
        assert_eq!(
            put_result.transfers_created, 2,
            "put should create 2 transfers (full chain via optimal_tree)"
        );

        // sync: local→cloud 実行 → next-hop cloud→pod が動的作成 → 実行
        let result = db.sync(None).await.unwrap();
        assert_eq!(
            result.batch.transferred, 2,
            "chain: local→cloud + cloud→pod = 2 transfers"
        );

        // status: 各locationに正確に1カウント
        let summary = db.status().await.unwrap();
        let local_count = summary
            .locations
            .get(&LocationId::local())
            .map(|s| s.present)
            .unwrap_or(0);
        let cloud_count = summary
            .locations
            .get(&LocationId::new("cloud").unwrap())
            .map(|s| s.present)
            .unwrap_or(0);
        let pod_count = summary
            .locations
            .get(&LocationId::new("pod").unwrap())
            .map(|s| s.present)
            .unwrap_or(0);

        assert_eq!(local_count, 1, "local should have exactly 1 present file");
        assert_eq!(cloud_count, 1, "cloud should have exactly 1 present file");
        assert_eq!(pod_count, 1, "pod should have exactly 1 present file");
    }
}
