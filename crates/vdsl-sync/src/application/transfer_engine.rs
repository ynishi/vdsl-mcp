//! TransferEngine — route-based transfer orchestrator.
//!
//! Owns the route map and executes concurrent transfers.
//! [`SdkImpl`](super::sdk_impl::SdkImpl) から Phase 3 (Execute) として呼び出される。
//!
//! Transfer execution operates on [`Transfer`] objects.
//! Each Transfer has explicit `src` and `dest` — no ambiguity about
//! which route to use. Chain transfers (local→cloud→pod) are handled
//! by creating next-hop Transfers on completion.

use std::collections::HashMap;

use futures::stream::{self, StreamExt};
use tracing::warn;

use super::observer::{SyncObserver, TransferProgress};
use super::route::TransferRoute;
use crate::application::error::SyncError;
use crate::domain::graph::{EdgeCost, RouteGraph};
use crate::domain::location::LocationId;
use crate::domain::plan::Topology;
use crate::domain::retry::TransferErrorKind;
use crate::domain::transfer::{Transfer, TransferKind};
use crate::infra::error::InfraError;
use crate::infra::topology_file_store::TopologyFileStore;
use crate::infra::transfer_store::TransferStore;

// =============================================================================
// Pure execution types (Engine ↔ SdkImpl boundary)
// =============================================================================

/// 実行準備済みTransfer。SdkImplがpath解決済みの状態でEngineに渡す。
///
/// Engineはこの型のみを入力として受け取り、DB/Observer一切不要で実行する。
pub struct PreparedTransfer {
    pub transfer: Transfer,
    pub relative_path: String,
}

/// 実行結果。Transfer状態はin-memoryで遷移済み（Completed or Failed）。
///
/// SdkImplがこの結果を受け取り、DB永続化 + unblock_dependentsを行う。
pub struct TransferOutcome {
    pub transfer: Transfer,
    pub relative_path: String,
}

/// Route map key: `(src, dest)` LocationId pair.
type RouteKey = (LocationId, LocationId);

// =============================================================================
// Batch result types (TransferEngine専用)
// =============================================================================

/// バッチ転送中の個別エラー。
#[derive(Debug, Clone, serde::Serialize)]
pub struct BatchError {
    pub path: String,
    pub error: String,
}

/// バッチ転送の結果。
#[derive(Debug, Clone, Default, serde::Serialize)]
pub struct BatchResult {
    pub transferred: usize,
    pub failed: usize,
    pub errors: Vec<BatchError>,
}

impl BatchResult {
    /// Serialize to [`serde_json::Value`] for cross-boundary transport.
    pub fn to_value(&self) -> Result<serde_json::Value, SyncError> {
        serde_json::to_value(self).map_err(|e| -> SyncError {
            InfraError::Serialization(format!("BatchResult: {e}")).into()
        })
    }
}

/// Route-based transfer engine.
///
/// Manages directed transfer routes and executes concurrent file transfers.
/// Does NOT own the stores — stores are passed by reference to execution methods.
///
/// # API surface
///
/// **Topology queries** (read-only):
/// - [`graph()`](Self::graph), [`find_route()`](Self::find_route),
///   [`local_root()`](Self::local_root), [`src_locations()`](Self::src_locations)
///
/// **Transfer execution** (side-effects, consumes queued Transfers):
/// - [`execute_all_with_observer()`](Self::execute_all_with_observer) — all routes, BFS order
/// - [`execute_route()`](Self::execute_route) — single (src, dest) route
/// - [`execute_file()`](Self::execute_file) — single file, optionally scoped to dest
///
/// The engine decides *how* to execute transfers (concurrency, ordering,
/// next-hop creation). Higher layers ([`SdkImpl`](super::sdk_impl::SdkImpl)) decide *when*.
/// Outcome of a single transfer execution: success (Transfer, file_id, src) or failure (file_id, error, src).
type ExecOutcome = Result<(Transfer, String, LocationId), (String, String, LocationId)>;

pub struct TransferEngine {
    graph: RouteGraph,
    routes: HashMap<RouteKey, TransferRoute>,
    concurrency: usize,
}

impl TransferEngine {
    /// Default maximum number of concurrent transfer operations per target.
    const DEFAULT_CONCURRENCY: usize = 8;

    /// Build the route map from a Vec of routes.
    fn build_route_map(routes: Vec<TransferRoute>) -> HashMap<RouteKey, TransferRoute> {
        routes
            .into_iter()
            .map(|r| ((r.src().clone(), r.dest().clone()), r))
            .collect()
    }

    /// Create a new TransferEngine from a list of routes.
    ///
    /// Builds the internal topology from route cost properties.
    /// `concurrency`: max concurrent transfers per target. 0 falls back to default.
    pub fn new(routes: Vec<TransferRoute>, concurrency: usize) -> Self {
        let mut graph = RouteGraph::new();
        for r in &routes {
            graph.add_with_cost(
                r.src().clone(),
                r.dest().clone(),
                EdgeCost::new(r.time_per_gb(), r.priority()),
            );
        }
        let concurrency = if concurrency == 0 {
            Self::DEFAULT_CONCURRENCY
        } else {
            concurrency
        };
        Self {
            graph,
            routes: Self::build_route_map(routes),
            concurrency,
        }
    }

    /// All edges as `(src, dest)` pairs.
    pub fn all_edges(&self) -> Vec<(LocationId, LocationId)> {
        self.graph.all_edges()
    }

    /// Find a route from src to dest. O(1) HashMap lookup.
    pub fn find_route(&self, src: &LocationId, dest: &LocationId) -> Option<&TransferRoute> {
        self.routes.get(&(src.clone(), dest.clone()))
    }

    /// Number of unique destination locations in the topology.
    pub fn destination_count(&self) -> usize {
        self.graph.all_destinations().len()
    }

    /// Destinations ordered by BFS distance from local.
    ///
    /// Used by `execute_all()` to process chain transfers in dependency order:
    /// e.g., `cloud` before `pod` when the graph is `local→cloud→pod`.
    fn destinations_ordered(&self) -> Vec<LocationId> {
        self.graph.destinations_ordered_from(&LocationId::local())
    }

    /// 全destinationをBFS順で返す（chain dependency order）。
    ///
    /// BFS順に並べた後、BFS到達不能なdestinationを末尾に追加する。
    /// SdkImplがPhase 3のBFSループで使用する。
    pub fn all_targets_ordered(&self) -> Vec<LocationId> {
        let mut targets = self.destinations_ordered();
        for dest in self.graph.all_destinations() {
            if !targets.contains(&dest) {
                targets.push(dest);
            }
        }
        targets
    }

    /// Resolve the local file root from routes.
    ///
    /// Finds the first route whose src is `local` and returns its `src_file_root`.
    /// Returns `None` if no local-source route is registered.
    pub fn local_root(&self) -> Option<&std::path::Path> {
        self.routes
            .values()
            .find(|r| r.src().is_local())
            .map(|r| r.src_file_root())
    }

    /// Unique source locations with one representative route per src.
    ///
    /// Used by `Store::scan_and_register` to scan each src location once.
    /// Returns `(src_location_id, &TransferRoute)` pairs, picking one route
    /// per unique src (deterministic but arbitrary which route if multiple exist).
    pub fn src_locations(&self) -> Vec<(&LocationId, &TransferRoute)> {
        let mut seen = std::collections::HashSet::new();
        let mut result = Vec::new();
        for ((src, _dest), route) in &self.routes {
            if seen.insert(src) {
                result.push((src, route));
            }
        }
        result
    }

    // =========================================================================
    // Transfer execution — v2 (Transfer object based)
    // =========================================================================

    /// Execute all queued transfers across the entire topology.
    ///
    /// Processes destinations in BFS order from local first (e.g., cloud before pod
    /// for chain transfers). On completion, creates next-hop Transfers for
    /// downstream destinations.
    #[deprecated(note = "use execute_prepared — execute_all accesses DB directly")]
    #[allow(dead_code)]
    pub async fn execute_all(
        &self,
        topology_files: &dyn TopologyFileStore,
        transfer_store: &dyn TransferStore,
    ) -> Result<BatchResult, SyncError> {
        let mut result = BatchResult::default();

        // BFS from local first (chain dependency order)
        let mut targets = self.destinations_ordered();

        // Append any destinations not reachable from local
        for dest in self.graph.all_destinations() {
            if !targets.contains(&dest) {
                targets.push(dest);
            }
        }

        for target in &targets {
            let batch = self
                .execute_target(topology_files, transfer_store, target)
                .await?;
            result.transferred += batch.transferred;
            result.failed += batch.failed;
            result.errors.extend(batch.errors);
        }

        Ok(result)
    }

    /// Execute all queued transfers with observer notifications.
    ///
    /// Uses batch execution when the route supports it (rclone backends).
    /// Sync transfers are batched per-route; Delete transfers run individually.
    #[deprecated(note = "use execute_prepared — execute_all_with_observer accesses DB directly")]
    pub async fn execute_all_with_observer(
        &self,
        topology_files: &dyn TopologyFileStore,
        transfer_store: &dyn TransferStore,
        observer: &dyn SyncObserver,
    ) -> Result<BatchResult, SyncError> {
        let mut result = BatchResult::default();

        let mut targets = self.destinations_ordered();
        for dest in self.graph.all_destinations() {
            if !targets.contains(&dest) {
                targets.push(dest);
            }
        }

        for target in &targets {
            let queued = transfer_store.queued_transfers(target).await?;
            let queued_count = queued.len();

            // Collect unique srcs for this dest.
            let srcs: Vec<LocationId> = {
                let mut s: Vec<LocationId> = queued.iter().map(|t| t.src().clone()).collect();
                s.sort();
                s.dedup();
                s
            };

            observer.on_target_start(&srcs, target, queued_count);

            // Use batch execution when any route to this target supports it
            let has_batch_route = queued.iter().any(|t| {
                self.find_route(t.src(), t.dest())
                    .is_some_and(|r| r.supports_batch())
            });

            let batch = if has_batch_route {
                self.execute_target_batch_with_observer(
                    topology_files,
                    transfer_store,
                    target,
                    queued,
                    observer,
                )
                .await?
            } else {
                self.execute_target_with_observer(
                    topology_files,
                    transfer_store,
                    target,
                    queued,
                    observer,
                )
                .await?
            };

            observer.on_target_done(&srcs, target, queued_count, batch.transferred, batch.failed);
            result.transferred += batch.transferred;
            result.failed += batch.failed;
            result.errors.extend(batch.errors);
        }

        Ok(result)
    }

    /// Execute queued transfers for a specific route (src → dest).
    ///
    /// Explicit source and destination. No next-hop creation.
    /// Returns error if no route is registered for (src, dest).
    #[deprecated(note = "use execute_prepared — execute_route accesses DB directly")]
    pub async fn execute_route(
        &self,
        topology_files: &dyn TopologyFileStore,
        transfer_store: &dyn TransferStore,
        src: &LocationId,
        dest: &LocationId,
    ) -> Result<BatchResult, SyncError> {
        let route = self
            .find_route(src, dest)
            .ok_or_else(|| SyncError::NoRouteAvailable {
                src: src.to_string(),
                dest: dest.to_string(),
                path: String::new(),
            })?;

        let queued = transfer_store.queued_transfers(dest).await?;
        // Filter: only transfers with matching src
        let eligible: Vec<_> = queued.into_iter().filter(|t| t.src() == src).collect();

        let mut result = BatchResult::default();

        let outcomes: Vec<_> = stream::iter(eligible.into_iter().map(|mut transfer| async move {
            let file = topology_files
                .get_by_id(transfer.file_id())
                .await
                .map_err(|e| (transfer.file_id().to_string(), e.to_string()))?
                .ok_or_else(|| {
                    (
                        transfer.file_id().to_string(),
                        format!("file {} not found in store", transfer.file_id()),
                    )
                })?;

            // Source file existence check (skip for pull-direction routes
            // and Delete transfers — deleted files won't exist on src)
            if !route.is_pull() && !transfer.is_delete() {
                match route.src_file_exists(file.relative_path()).await {
                    Ok(true) => {}
                    Ok(false) => {
                        // Mark transfer as failed with file-not-found
                        let _ = transfer.start();
                        let _ = transfer.fail(
                            format!("source file not found on {}", transfer.src()),
                            TransferErrorKind::Permanent,
                        );
                        let _ = transfer_store.update_transfer(&transfer).await;
                        return Err((
                            file.relative_path().to_string(),
                            format!("source file not found on {src}"),
                        ));
                    }
                    Err(e) => {
                        return Err((file.relative_path().to_string(), e.to_string()));
                    }
                }
            }

            Self::execute_one(transfer_store, &mut transfer, route, file.relative_path())
                .await
                .map_err(|e| (file.relative_path().to_string(), e.to_string()))
        }))
        .buffer_unordered(self.concurrency)
        .collect()
        .await;

        for outcome in outcomes {
            match outcome {
                Ok(()) => result.transferred += 1,
                Err((path, msg)) => {
                    result.failed += 1;
                    result.errors.push(BatchError { path, error: msg });
                }
            }
        }

        Ok(result)
    }

    /// Execute transfers for a single file.
    ///
    /// If `dest` is Some, executes to that destination only.
    /// If `dest` is None, executes all queued transfers for this file.
    #[allow(dead_code)] // API reserved for future per-file sync
    pub async fn execute_file(
        &self,
        topology_files: &dyn TopologyFileStore,
        transfer_store: &dyn TransferStore,
        relative_path: &str,
        dest: Option<&LocationId>,
    ) -> Result<BatchResult, SyncError> {
        let file = topology_files
            .get_by_path(relative_path)
            .await?
            .ok_or_else(|| SyncError::NotRegistered(relative_path.to_string()))?;

        let latest = transfer_store.latest_transfers_by_file(file.id()).await?;

        let targets: Vec<Transfer> = match dest {
            Some(d) => latest
                .into_iter()
                .filter(|t| t.dest() == d && t.state().is_actionable())
                .collect(),
            None => latest
                .into_iter()
                .filter(|t| t.state().is_actionable())
                .collect(),
        };

        let mut result = BatchResult::default();

        for mut transfer in targets {
            let route = match self.find_route(transfer.src(), transfer.dest()) {
                Some(r) => r,
                None => {
                    result.failed += 1;
                    result.errors.push(BatchError {
                        path: relative_path.to_string(),
                        error: format!("no route: {} → {}", transfer.src(), transfer.dest()),
                    });
                    continue;
                }
            };

            // Skip src_file_exists for Delete transfers — the file is already gone
            if !route.is_pull() && !transfer.is_delete() {
                match route.src_file_exists(relative_path).await {
                    Ok(true) => {}
                    Ok(false) => {
                        let _ = transfer.start();
                        let _ = transfer.fail(
                            format!("source file not found on {}", transfer.src()),
                            TransferErrorKind::Permanent,
                        );
                        let _ = transfer_store.update_transfer(&transfer).await;
                        result.failed += 1;
                        result.errors.push(BatchError {
                            path: relative_path.to_string(),
                            error: format!("source file not found on {}", transfer.src()),
                        });
                        continue;
                    }
                    Err(e) => {
                        result.failed += 1;
                        result.errors.push(BatchError {
                            path: relative_path.to_string(),
                            error: e.to_string(),
                        });
                        continue;
                    }
                }
            }

            match Self::execute_one(transfer_store, &mut transfer, route, relative_path).await {
                Ok(()) => result.transferred += 1,
                Err(e) => {
                    result.failed += 1;
                    result.errors.push(BatchError {
                        path: relative_path.to_string(),
                        error: e.to_string(),
                    });
                }
            }
        }

        Ok(result)
    }

    /// Execute queued transfers for a single target destination.
    ///
    /// Creates next-hop Transfers on completion for chain transfers.
    #[allow(dead_code)] // Used by execute_all (non-observer path)
    async fn execute_target(
        &self,
        topology_files: &dyn TopologyFileStore,
        transfer_store: &dyn TransferStore,
        target: &LocationId,
    ) -> Result<BatchResult, SyncError> {
        let queued = transfer_store.queued_transfers(target).await?;
        let mut result = BatchResult::default();

        let outcomes: Vec<_> = stream::iter(queued.into_iter().map(|mut transfer| async move {
            let file = topology_files
                .get_by_id(transfer.file_id())
                .await
                .map_err(|e| (transfer.file_id().to_string(), e.to_string()))?
                .ok_or_else(|| {
                    (
                        transfer.file_id().to_string(),
                        format!("file {} not found in store", transfer.file_id()),
                    )
                })?;

            let route = self
                .find_route(transfer.src(), transfer.dest())
                .ok_or_else(|| {
                    (
                        file.relative_path().to_string(),
                        format!("no route: {} → {}", transfer.src(), transfer.dest()),
                    )
                })?;

            // Source file existence check (skip for pull-direction routes
            // and Delete transfers — deleted files won't exist on src)
            if !route.is_pull() && !transfer.is_delete() {
                match route.src_file_exists(file.relative_path()).await {
                    Ok(true) => {}
                    Ok(false) => {
                        let _ = transfer.start();
                        let _ = transfer.fail(
                            format!("source file not found on {}", transfer.src()),
                            TransferErrorKind::Permanent,
                        );
                        let _ = transfer_store.update_transfer(&transfer).await;
                        return Err((
                            file.relative_path().to_string(),
                            format!("source file not found on {}", transfer.src()),
                        ));
                    }
                    Err(e) => {
                        return Err((file.relative_path().to_string(), e.to_string()));
                    }
                }
            }

            Self::execute_one(transfer_store, &mut transfer, route, file.relative_path())
                .await
                .map(|()| transfer) // return completed transfer for next-hop
                .map_err(|e| (file.relative_path().to_string(), e.to_string()))
        }))
        .buffer_unordered(self.concurrency)
        .collect()
        .await;

        for outcome in outcomes {
            match outcome {
                Ok(completed) => {
                    result.transferred += 1;
                    // Create next-hop transfers for chain routing
                    transfer_store.unblock_dependents(completed.id()).await?;
                }
                Err((path, msg)) => {
                    result.failed += 1;
                    result.errors.push(BatchError { path, error: msg });
                }
            }
        }

        Ok(result)
    }

    /// Execute queued transfers for a single target with observer.
    ///
    /// Pre-fetched `queued` transfers are passed in (already counted by caller).
    async fn execute_target_with_observer(
        &self,
        topology_files: &dyn TopologyFileStore,
        transfer_store: &dyn TransferStore,
        target: &LocationId,
        queued: Vec<Transfer>,
        observer: &dyn SyncObserver,
    ) -> Result<BatchResult, SyncError> {
        let total = queued.len();
        let mut result = BatchResult::default();

        let outcomes: Vec<ExecOutcome> =
            stream::iter(queued.into_iter().map(|mut transfer| async move {
                let src = transfer.src().clone();

                let file = topology_files
                    .get_by_id(transfer.file_id())
                    .await
                    .map_err(|e| (transfer.file_id().to_string(), e.to_string(), src.clone()))?
                    .ok_or_else(|| {
                        (
                            transfer.file_id().to_string(),
                            format!("file {} not found in store", transfer.file_id()),
                            src.clone(),
                        )
                    })?;

                let route = self
                    .find_route(transfer.src(), transfer.dest())
                    .ok_or_else(|| {
                        (
                            file.relative_path().to_string(),
                            format!("no route: {} → {}", transfer.src(), transfer.dest()),
                            src.clone(),
                        )
                    })?;

                if !route.is_pull() && !transfer.is_delete() {
                    match route.src_file_exists(file.relative_path()).await {
                        Ok(true) => {}
                        Ok(false) => {
                            let _ = transfer.start();
                            let _ = transfer.fail(
                                format!("source file not found on {}", transfer.src()),
                                TransferErrorKind::Permanent,
                            );
                            let _ = transfer_store.update_transfer(&transfer).await;
                            return Err((
                                file.relative_path().to_string(),
                                format!("source file not found on {}", transfer.src()),
                                src,
                            ));
                        }
                        Err(e) => {
                            return Err((file.relative_path().to_string(), e.to_string(), src));
                        }
                    }
                }

                Self::execute_one(transfer_store, &mut transfer, route, file.relative_path())
                    .await
                    .map(|()| (transfer, file.relative_path().to_string(), src.clone()))
                    .map_err(|e| (file.relative_path().to_string(), e.to_string(), src))
            }))
            .buffer_unordered(self.concurrency)
            .collect()
            .await;

        for outcome in outcomes {
            match outcome {
                Ok((completed, path, src)) => {
                    result.transferred += 1;
                    transfer_store.unblock_dependents(completed.id()).await?;

                    let completed_count = result.transferred + result.failed;
                    if completed_count % 20 == 0 || completed_count == total {
                        observer.on_transfer_progress(&TransferProgress {
                            src,
                            dest: target.clone(),
                            completed: completed_count,
                            total,
                            last_path: Some(path),
                        });
                    }
                }
                Err((path, msg, src)) => {
                    result.failed += 1;
                    result.errors.push(BatchError { path, error: msg });

                    let completed_count = result.transferred + result.failed;
                    if completed_count % 20 == 0 || completed_count == total {
                        observer.on_transfer_progress(&TransferProgress {
                            src,
                            dest: target.clone(),
                            completed: completed_count,
                            total,
                            last_path: None,
                        });
                    }
                }
            }
        }

        Ok(result)
    }

    /// Execute queued Sync transfers in batch, Delete transfers individually.
    ///
    /// Batch execution uses `rclone copy --files-from` for a single rclone
    /// process per route, dramatically reducing auth overhead and leveraging
    /// rclone's internal parallelism.
    ///
    /// Pre-fetched `queued` transfers are passed in (already counted by caller).
    async fn execute_target_batch_with_observer(
        &self,
        topology_files: &dyn TopologyFileStore,
        transfer_store: &dyn TransferStore,
        target: &LocationId,
        queued: Vec<Transfer>,
        observer: &dyn SyncObserver,
    ) -> Result<BatchResult, SyncError> {
        let total = queued.len();
        let mut result = BatchResult::default();

        // Partition: Sync transfers (batchable) vs Delete transfers (individual)
        let mut sync_transfers: Vec<Transfer> = Vec::new();
        let mut delete_transfers: Vec<Transfer> = Vec::new();
        for t in queued {
            match t.kind() {
                TransferKind::Sync => sync_transfers.push(t),
                TransferKind::Delete => delete_transfers.push(t),
            }
        }

        // --- Batch Sync transfers (grouped by src) ---
        if !sync_transfers.is_empty() {
            // Group by src — each group shares one route
            let mut by_src: HashMap<LocationId, Vec<Transfer>> = HashMap::new();
            for t in sync_transfers {
                by_src.entry(t.src().clone()).or_default().push(t);
            }

            for (group_src, group) in by_src {
                // Resolve file paths and find the route for this src group
                let mut path_map: HashMap<String, (Transfer, String)> = HashMap::new();
                let mut route_ref: Option<&TransferRoute> = None;

                for transfer in group.iter() {
                    let file = match topology_files.get_by_id(transfer.file_id()).await {
                        Ok(Some(f)) => f,
                        Ok(None) => {
                            result.failed += 1;
                            result.errors.push(BatchError {
                                path: transfer.file_id().to_string(),
                                error: format!("file {} not found in store", transfer.file_id()),
                            });
                            continue;
                        }
                        Err(e) => {
                            result.failed += 1;
                            result.errors.push(BatchError {
                                path: transfer.file_id().to_string(),
                                error: e.to_string(),
                            });
                            continue;
                        }
                    };

                    if route_ref.is_none() {
                        route_ref = self.find_route(transfer.src(), transfer.dest());
                    }

                    path_map.insert(
                        file.relative_path().to_string(),
                        (transfer.clone(), file.relative_path().to_string()),
                    );
                }

                let Some(route) = route_ref else {
                    continue;
                };

                if route.supports_batch() && path_map.len() > 1 {
                    // Batch path: single rclone process (or compressed batch)
                    let relative_paths: Vec<String> = path_map.keys().cloned().collect();

                    // Mark all as InFlight
                    for (_, (transfer, _)) in path_map.iter_mut() {
                        if let Err(e) = transfer.start() {
                            warn!(transfer_id = %transfer.id(), error = %e, "failed to start transfer");
                        }
                        let _ = transfer_store.update_transfer(transfer).await;
                    }

                    let batch_results = route.transfer_batch(&relative_paths).await;

                    for (rel_path, outcome) in batch_results {
                        if let Some((mut transfer, _)) = path_map.remove(&rel_path) {
                            match outcome {
                                Ok(()) => {
                                    if let Err(e) = transfer.complete() {
                                        warn!(transfer_id = %transfer.id(), error = %e, "failed to complete transfer");
                                    }
                                    let _ = transfer_store.update_transfer(&transfer).await;
                                    result.transferred += 1;

                                    transfer_store.unblock_dependents(transfer.id()).await?;
                                }
                                Err(e) => {
                                    let err_msg = e.to_string();
                                    let kind = classify_transfer_error(&e);
                                    let _ = transfer.fail(err_msg.clone(), kind);
                                    let _ = transfer_store.update_transfer(&transfer).await;
                                    result.failed += 1;
                                    result.errors.push(BatchError {
                                        path: rel_path,
                                        error: err_msg,
                                    });
                                }
                            }

                            let completed_count = result.transferred + result.failed;
                            if completed_count % 20 == 0 || completed_count == total {
                                observer.on_transfer_progress(&TransferProgress {
                                    src: group_src.clone(),
                                    dest: target.clone(),
                                    completed: completed_count,
                                    total,
                                    last_path: None,
                                });
                            }
                        }
                    }
                } else {
                    // Fallback: individual transfers (1 file or non-batch backend)
                    for (rel_path, (mut transfer, _)) in path_map {
                        match Self::execute_one(transfer_store, &mut transfer, route, &rel_path)
                            .await
                        {
                            Ok(()) => {
                                result.transferred += 1;
                                transfer_store.unblock_dependents(transfer.id()).await?;
                            }
                            Err(e) => {
                                result.failed += 1;
                                result.errors.push(BatchError {
                                    path: rel_path,
                                    error: e.to_string(),
                                });
                            }
                        }

                        let completed_count = result.transferred + result.failed;
                        if completed_count % 20 == 0 || completed_count == total {
                            observer.on_transfer_progress(&TransferProgress {
                                src: group_src.clone(),
                                dest: target.clone(),
                                completed: completed_count,
                                total,
                                last_path: None,
                            });
                        }
                    }
                }
            }
        }

        // --- Individual Delete transfers ---
        for mut transfer in delete_transfers {
            let file = match topology_files.get_by_id(transfer.file_id()).await {
                Ok(Some(f)) => f,
                Ok(None) | Err(_) => {
                    result.failed += 1;
                    continue;
                }
            };

            let route = match self.find_route(transfer.src(), transfer.dest()) {
                Some(r) => r,
                None => {
                    result.failed += 1;
                    result.errors.push(BatchError {
                        path: file.relative_path().to_string(),
                        error: format!("no route: {} → {}", transfer.src(), transfer.dest()),
                    });
                    continue;
                }
            };

            match Self::execute_one(transfer_store, &mut transfer, route, file.relative_path())
                .await
            {
                Ok(()) => {
                    result.transferred += 1;
                    transfer_store.unblock_dependents(transfer.id()).await?;
                }
                Err(e) => {
                    result.failed += 1;
                    result.errors.push(BatchError {
                        path: file.relative_path().to_string(),
                        error: e.to_string(),
                    });
                }
            }
        }

        Ok(result)
    }

    // =========================================================================
    // Pure execution — DB/Observer不要 (SdkImpl boundary)
    // =========================================================================

    /// PreparedTransfer群を実行し、TransferOutcome群を返す。
    ///
    /// **純粋なroute I/O実行のみ**。DB永続化・Observer通知は一切行わない。
    /// SdkImplがpath解決済みのPreparedTransferを渡し、
    /// 結果のTransferOutcomeをDB永続化する責務を負う。
    ///
    /// # 実行戦略
    ///
    /// - (src, dest)ルート毎にグループ化
    /// - Sync転送: batchサポート時はtransfer_batch、それ以外は個別並行実行
    /// - Delete転送: 常に個別並行実行
    pub async fn execute_prepared(&self, prepared: Vec<PreparedTransfer>) -> Vec<TransferOutcome> {
        let mut outcomes = Vec::with_capacity(prepared.len());

        // Group by (src, dest) route
        let mut by_route: HashMap<RouteKey, Vec<PreparedTransfer>> = HashMap::new();
        for p in prepared {
            let key = (p.transfer.src().clone(), p.transfer.dest().clone());
            by_route.entry(key).or_default().push(p);
        }

        for ((src, dest), group) in by_route {
            let route = match self.find_route(&src, &dest) {
                Some(r) => r,
                None => {
                    // No route: fail all transfers in this group
                    for mut p in group {
                        let _ = p.transfer.start();
                        let _ = p.transfer.fail(
                            format!("no route: {src} → {dest}"),
                            TransferErrorKind::Permanent,
                        );
                        outcomes.push(TransferOutcome {
                            transfer: p.transfer,
                            relative_path: p.relative_path,
                        });
                    }
                    continue;
                }
            };

            // Partition Sync vs Delete
            let (delete_group, sync_group): (Vec<_>, Vec<_>) =
                group.into_iter().partition(|p| p.transfer.is_delete());

            // Sync transfers
            if route.supports_batch() && sync_group.len() > 1 {
                outcomes.extend(Self::execute_batch_pure(route, sync_group).await);
            } else {
                let sync_outcomes: Vec<TransferOutcome> = stream::iter(
                    sync_group
                        .into_iter()
                        .map(|p| async { Self::execute_single_pure(route, p).await }),
                )
                .buffer_unordered(self.concurrency)
                .collect()
                .await;
                outcomes.extend(sync_outcomes);
            }

            // Delete transfers: always individual
            if !delete_group.is_empty() {
                let delete_outcomes: Vec<TransferOutcome> = stream::iter(
                    delete_group
                        .into_iter()
                        .map(|p| async { Self::execute_single_pure(route, p).await }),
                )
                .buffer_unordered(self.concurrency)
                .collect()
                .await;
                outcomes.extend(delete_outcomes);
            }
        }

        outcomes
    }

    /// 単一PreparedTransferを実行する純粋関数。DB/Observer不使用。
    ///
    /// state遷移: Queued → InFlight → Completed/Failed (in-memory)。
    async fn execute_single_pure(
        route: &TransferRoute,
        mut prepared: PreparedTransfer,
    ) -> TransferOutcome {
        // Source file existence check (push, non-delete only)
        if !route.is_pull() && !prepared.transfer.is_delete() {
            match route.src_file_exists(&prepared.relative_path).await {
                Ok(true) => {}
                Ok(false) => {
                    let _ = prepared.transfer.start();
                    let _ = prepared.transfer.fail(
                        format!("source file not found on {}", prepared.transfer.src()),
                        TransferErrorKind::Permanent,
                    );
                    return TransferOutcome {
                        transfer: prepared.transfer,
                        relative_path: prepared.relative_path,
                    };
                }
                Err(e) => {
                    let _ = prepared.transfer.start();
                    let _ = prepared
                        .transfer
                        .fail(e.to_string(), classify_transfer_error(&e));
                    return TransferOutcome {
                        transfer: prepared.transfer,
                        relative_path: prepared.relative_path,
                    };
                }
            }
        }

        // Start: Queued → InFlight
        if let Err(e) = prepared.transfer.start() {
            warn!(
                transfer_id = %prepared.transfer.id(),
                error = %e,
                "execute_single_pure: failed to start transfer"
            );
            return TransferOutcome {
                transfer: prepared.transfer,
                relative_path: prepared.relative_path,
            };
        }

        // Execute route operation
        let op_result = match prepared.transfer.kind() {
            TransferKind::Sync => route.transfer(&prepared.relative_path).await,
            TransferKind::Delete => route.delete(&prepared.relative_path).await,
        };

        match op_result {
            Ok(()) => {
                if let Err(e) = prepared.transfer.complete() {
                    warn!(
                        transfer_id = %prepared.transfer.id(),
                        error = %e,
                        "execute_single_pure: failed to complete transfer"
                    );
                }
            }
            Err(e) => {
                let kind = classify_transfer_error(&e);
                if let Err(state_err) = prepared.transfer.fail(e.to_string(), kind) {
                    warn!(
                        transfer_id = %prepared.transfer.id(),
                        error = %state_err,
                        "execute_single_pure: failed to mark transfer as failed"
                    );
                }
            }
        }

        TransferOutcome {
            transfer: prepared.transfer,
            relative_path: prepared.relative_path,
        }
    }

    /// Batch実行（rclone --files-from等）。DB/Observer不使用。
    ///
    /// 全transferをInFlightに遷移後、route.transfer_batch()を一括実行し、
    /// 結果をTransferOutcomeに変換する。
    async fn execute_batch_pure(
        route: &TransferRoute,
        mut prepared: Vec<PreparedTransfer>,
    ) -> Vec<TransferOutcome> {
        let relative_paths: Vec<String> =
            prepared.iter().map(|p| p.relative_path.clone()).collect();

        // Mark all as InFlight
        for p in &mut prepared {
            if let Err(e) = p.transfer.start() {
                warn!(
                    transfer_id = %p.transfer.id(),
                    error = %e,
                    "execute_batch_pure: failed to start transfer"
                );
            }
        }

        let batch_results = route.transfer_batch(&relative_paths).await;

        let mut outcomes = Vec::with_capacity(prepared.len());
        let mut path_map: HashMap<String, PreparedTransfer> = prepared
            .into_iter()
            .map(|p| (p.relative_path.clone(), p))
            .collect();

        for (rel_path, result) in batch_results {
            if let Some(mut p) = path_map.remove(&rel_path) {
                match result {
                    Ok(()) => {
                        if let Err(e) = p.transfer.complete() {
                            warn!(
                                transfer_id = %p.transfer.id(),
                                error = %e,
                                "execute_batch_pure: failed to complete transfer"
                            );
                        }
                    }
                    Err(e) => {
                        let kind = classify_transfer_error(&e);
                        let _ = p.transfer.fail(e.to_string(), kind);
                    }
                }
                outcomes.push(TransferOutcome {
                    transfer: p.transfer,
                    relative_path: p.relative_path,
                });
            }
        }

        // Transfers not in batch result — mark as failed
        for (_, mut p) in path_map {
            let _ = p.transfer.fail(
                "not included in batch result".to_string(),
                TransferErrorKind::Transient,
            );
            outcomes.push(TransferOutcome {
                transfer: p.transfer,
                relative_path: p.relative_path,
            });
        }

        outcomes
    }

    // =========================================================================
    // Legacy execution — DB/Observer使用 (旧Store/SyncFacade互換)
    // =========================================================================

    /// Execute a single transfer: start → route.transfer → complete/fail.
    ///
    /// Manages the Queued→InFlight→Completed/Failed state transitions
    /// on the Transfer object, persisting each transition.
    async fn execute_one(
        transfer_store: &dyn TransferStore,
        transfer: &mut Transfer,
        route: &TransferRoute,
        relative_path: &str,
    ) -> Result<(), SyncError> {
        transfer.start()?;
        transfer_store.update_transfer(transfer).await?;

        let op_result = match transfer.kind() {
            TransferKind::Sync => route.transfer(relative_path).await,
            TransferKind::Delete => route.delete(relative_path).await,
        };

        match op_result {
            Ok(()) => {
                transfer.complete()?;
                transfer_store.update_transfer(transfer).await?;
                Ok(())
            }
            Err(e) => {
                let err_msg = e.to_string();
                let kind = classify_transfer_error(&e);
                if let Err(state_err) = transfer.fail(err_msg, kind) {
                    warn!(
                        transfer_id = %transfer.id(),
                        error = %state_err,
                        "failed to transition transfer to Failed state"
                    );
                }
                if let Err(store_err) = transfer_store.update_transfer(transfer).await {
                    warn!(
                        transfer_id = %transfer.id(),
                        error = %store_err,
                        "failed to persist transfer failure"
                    );
                }
                Err(e)
            }
        }
    }
}

impl Topology for TransferEngine {
    fn reachable_from(&self, origin: &LocationId) -> std::collections::HashSet<LocationId> {
        self.graph.reachable_from(origin)
    }

    fn optimal_tree(
        &self,
        origin: &LocationId,
        required_dests: &std::collections::HashSet<LocationId>,
    ) -> Vec<(LocationId, LocationId)> {
        self.graph.optimal_tree(origin, required_dests)
    }
}

/// Classify a transfer error as Transient or Permanent.
///
/// Domain rule: errors that cannot be resolved by retrying are Permanent.
/// All others default to Transient (network issues, timeouts, rate limits).
///
/// Permanent indicators:
/// - File not found at source (source deleted between scan and transfer)
/// - Path validation failures (traversal, invalid UTF-8)
/// - Backend not configured / not supported
fn classify_transfer_error(e: &SyncError) -> TransferErrorKind {
    match e {
        // Domain errors (validation, state machine) — bugs, not retry-worthy
        SyncError::Domain(_) => TransferErrorKind::Permanent,
        // Structural routing / registration errors
        SyncError::OutsideSyncRoot { .. }
        | SyncError::NotRegistered(_)
        | SyncError::NoBackend(_)
        | SyncError::NoRouteAvailable { .. } => TransferErrorKind::Permanent,
        // Infra errors: inspect inner type
        SyncError::Infra(infra) => classify_infra_error(infra),
        // Duplicate is not a transfer error, but if it somehow gets here, permanent
        SyncError::Duplicate { .. } => TransferErrorKind::Permanent,
    }
}

fn classify_infra_error(e: &crate::infra::error::InfraError) -> TransferErrorKind {
    use crate::infra::error::InfraError;
    match e {
        // File not found — source disappeared, retry won't help
        InfraError::FileNotFound(_) => TransferErrorKind::Permanent,
        // Transfer errors: check for permanent patterns in the message
        InfraError::Transfer { reason } => {
            let r = reason.to_lowercase();
            if r.contains("not valid utf-8")
                || r.contains("traversal")
                || r.contains("not supported")
                || r.contains("starts with '-'")
            {
                TransferErrorKind::Permanent
            } else {
                // Network failures, timeouts, rate limits — retryable
                TransferErrorKind::Transient
            }
        }
        // IO errors: most are transient (disk full, permission denied could be permanent
        // but conservatively treat as transient for retry)
        InfraError::Io(_) => TransferErrorKind::Transient,
        // Store/Hash/Serialization — likely bugs, permanent
        InfraError::Store { .. } | InfraError::Hash { .. } | InfraError::Serialization(_) => {
            TransferErrorKind::Permanent
        }
    }
}
