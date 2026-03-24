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
use tracing::{debug, info, trace, warn};

use super::route::TransferRoute;
use crate::application::error::SyncError;
use crate::domain::graph::RouteGraph;
use crate::domain::location::LocationId;
use crate::domain::plan::Topology;
use crate::domain::retry::TransferErrorKind;
use crate::domain::transfer::{Transfer, TransferKind, TransferState};

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

/// Batch操作の抽象化。transfer_batch / delete_batch を統一的に扱う。
trait AsyncBatchFn {
    fn call(
        &self,
        route: &TransferRoute,
        paths: &[String],
    ) -> impl std::future::Future<Output = HashMap<String, Result<(), SyncError>>> + Send;
}

/// Sync用batch操作。`route.transfer_batch()` を呼び出す。
struct BatchSync;
impl AsyncBatchFn for BatchSync {
    async fn call(
        &self,
        route: &TransferRoute,
        paths: &[String],
    ) -> HashMap<String, Result<(), SyncError>> {
        route.transfer_batch(paths).await
    }
}

/// Delete用batch操作。`route.delete_batch()` を呼び出す。
struct BatchDelete;
impl AsyncBatchFn for BatchDelete {
    async fn call(
        &self,
        route: &TransferRoute,
        paths: &[String],
    ) -> HashMap<String, Result<(), SyncError>> {
        route.delete_batch(paths).await
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
/// - [`find_route()`](Self::find_route), [`local_root()`](Self::local_root),
///   [`destinations_ordered()`](Self::destinations_ordered)
///
/// **Transfer execution** (pure I/O, no DB access):
/// - [`execute_prepared()`](Self::execute_prepared) — batch execute with prepared transfers
///
/// The engine decides *how* to execute transfers (concurrency, ordering).
/// Higher layers ([`SdkImpl`](super::sdk_impl::SdkImpl)) decide *when*.
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

    /// Create a new TransferEngine from a pre-built graph and routes.
    ///
    /// The caller is responsible for building the `RouteGraph` from route costs.
    /// `concurrency`: max concurrent transfers per target. 0 falls back to default.
    pub fn new(graph: RouteGraph, routes: Vec<TransferRoute>, concurrency: usize) -> Self {
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
            let group_len = group.len();
            info!(src = %src, dest = %dest, count = group_len, "execute_prepared: route group start");

            let route = match self.find_route(&src, &dest) {
                Some(r) => r,
                None => {
                    warn!(src = %src, dest = %dest, count = group_len, "execute_prepared: no route found");
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

            debug!(
                src = %src, dest = %dest,
                sync = sync_group.len(), delete = delete_group.len(),
                batch = route.supports_batch(),
                "execute_prepared: partitioned"
            );

            // Sync transfers
            if route.supports_batch() && sync_group.len() > 1 {
                info!(src = %src, dest = %dest, count = sync_group.len(), "execute_prepared: batch transfer start");
                outcomes.extend(
                    Self::execute_batch_common(route, sync_group, BatchSync, "batch_sync").await,
                );
            } else {
                info!(src = %src, dest = %dest, count = sync_group.len(), concurrency = self.concurrency, "execute_prepared: individual transfer start");
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

            // Delete transfers: batch when supported, individual fallback
            if !delete_group.is_empty() {
                if route.supports_batch() && delete_group.len() > 1 {
                    info!(src = %src, dest = %dest, count = delete_group.len(), "execute_prepared: batch delete start");
                    outcomes.extend(
                        Self::execute_batch_common(
                            route,
                            delete_group,
                            BatchDelete,
                            "batch_delete",
                        )
                        .await,
                    );
                } else {
                    info!(src = %src, dest = %dest, count = delete_group.len(), concurrency = self.concurrency, "execute_prepared: individual delete start");
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

            let completed = outcomes
                .iter()
                .filter(|o| o.transfer.state() == TransferState::Completed)
                .count();
            let failed = outcomes
                .iter()
                .filter(|o| o.transfer.state() == TransferState::Failed)
                .count();
            info!(
                src = %src, dest = %dest,
                completed = completed, failed = failed,
                "execute_prepared: route group done"
            );
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
        trace!(
            transfer_id = %prepared.transfer.id(),
            path = %prepared.relative_path,
            src = %prepared.transfer.src(),
            dest = %prepared.transfer.dest(),
            kind = ?prepared.transfer.kind(),
            "execute_single_pure: start"
        );
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
                debug!(
                    path = %prepared.relative_path,
                    src = %prepared.transfer.src(),
                    dest = %prepared.transfer.dest(),
                    "execute_single_pure: completed"
                );
            }
            Err(e) => {
                let kind = classify_transfer_error(&e);
                debug!(
                    path = %prepared.relative_path,
                    err = %e,
                    kind = ?kind,
                    "execute_single_pure: failed"
                );
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

    /// Batch実行の共通ヘルパー。DB/Observer不使用。
    ///
    /// 全transferをInFlightに遷移後、`batch_fn`で一括実行し、
    /// 結果をTransferOutcomeに変換する。
    /// Sync（transfer_batch）/ Delete（delete_batch）の両方で使用。
    async fn execute_batch_common(
        route: &TransferRoute,
        mut prepared: Vec<PreparedTransfer>,
        batch_fn: impl AsyncBatchFn,
        label: &str,
    ) -> Vec<TransferOutcome> {
        let relative_paths: Vec<String> =
            prepared.iter().map(|p| p.relative_path.clone()).collect();

        // Mark all as InFlight
        for p in &mut prepared {
            if let Err(e) = p.transfer.start() {
                warn!(
                    transfer_id = %p.transfer.id(),
                    error = %e,
                    "{label}: failed to start transfer"
                );
            }
        }

        let batch_start = std::time::Instant::now();
        info!(
            count = relative_paths.len(),
            src = %route.src(),
            dest = %route.dest(),
            "{label}: calling batch"
        );
        let batch_results = batch_fn.call(route, &relative_paths).await;
        let elapsed = batch_start.elapsed();
        info!(
            results = batch_results.len(),
            elapsed_secs = elapsed.as_secs(),
            src = %route.src(),
            dest = %route.dest(),
            "{label}: batch returned"
        );

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
                                "{label}: failed to complete transfer"
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
                format!("not included in {label} result"),
                TransferErrorKind::Transient,
            );
            outcomes.push(TransferOutcome {
                transfer: p.transfer,
                relative_path: p.relative_path,
            });
        }

        outcomes
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
        // Structural routing / registration / initialization errors
        SyncError::OutsideSyncRoot { .. }
        | SyncError::NotRegistered(_)
        | SyncError::NoBackend(_)
        | SyncError::NoRouteAvailable { .. }
        | SyncError::Init(_) => TransferErrorKind::Permanent,
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
