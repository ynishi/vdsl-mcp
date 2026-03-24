//! TransferEngine — route-based transfer orchestrator.
//!
//! Owns the route map and executes concurrent transfers.
//! Separated from [`SyncService`] to isolate routing/transfer concerns
//! from notify/register/store-delegation responsibilities.
//!
//! # v2 architecture
//!
//! Transfer execution operates on [`Transfer`] objects (not SyncEntry).
//! Each Transfer has explicit `src` and `dest` — no ambiguity about
//! which route to use. Chain transfers (local→cloud→pod) are handled
//! by creating next-hop Transfers on completion.

use std::collections::HashMap;

use futures::stream::{self, StreamExt};
use tracing::warn;

use super::route::TransferRoute;
use crate::domain::error::SyncError;
use crate::domain::graph::RouteGraph;
use crate::domain::location::LocationId;
use crate::domain::retry::TransferErrorKind;
use crate::domain::transfer::Transfer;
use crate::infra::file_store::FileStore;
use crate::infra::transfer_store::TransferStore;

/// Route map key: `(src, dest)` LocationId pair.
type RouteKey = (LocationId, LocationId);

/// A single error from a batch push operation.
#[derive(Debug, serde::Serialize)]
pub struct BatchError {
    pub path: String,
    pub error: String,
}

/// Result of a batch push operation.
#[derive(Debug, Default, serde::Serialize)]
pub struct BatchResult {
    pub pushed: usize,
    pub failed: usize,
    pub errors: Vec<BatchError>,
}

impl BatchResult {
    /// Serialize to [`serde_json::Value`] for cross-boundary transport.
    pub fn to_value(&self) -> Result<serde_json::Value, SyncError> {
        serde_json::to_value(self)
            .map_err(|e| SyncError::Serialization(format!("BatchResult: {e}")))
    }
}

/// Route-based transfer engine.
///
/// Manages directed transfer routes and executes concurrent file transfers.
/// Does NOT own the stores — stores are passed by reference to `force()`.
pub struct TransferEngine {
    graph: RouteGraph,
    routes: HashMap<RouteKey, TransferRoute>,
    force_concurrency: usize,
}

impl TransferEngine {
    /// Default maximum number of concurrent push operations per target.
    const DEFAULT_FORCE_CONCURRENCY: usize = 8;

    /// Build the route map from a Vec of routes.
    fn build_route_map(routes: Vec<TransferRoute>) -> HashMap<RouteKey, TransferRoute> {
        routes
            .into_iter()
            .map(|r| ((r.src().clone(), r.dest().clone()), r))
            .collect()
    }

    /// Create a new TransferEngine from a list of routes.
    ///
    /// Builds the RouteGraph automatically from the route (src, dest) pairs.
    pub fn new(routes: Vec<TransferRoute>) -> Self {
        let mut graph = RouteGraph::new();
        for r in &routes {
            graph.add(r.src().clone(), r.dest().clone());
        }
        Self {
            graph,
            routes: Self::build_route_map(routes),
            force_concurrency: Self::DEFAULT_FORCE_CONCURRENCY,
        }
    }

    /// Set the maximum number of concurrent push operations in `force()`.
    ///
    /// Clamped to minimum 1 — `buffer_unordered(0)` would deadlock the stream.
    pub fn set_force_concurrency(&mut self, n: usize) {
        self.force_concurrency = n.max(1);
    }

    /// The domain RouteGraph (DAG). Used by SyncService for reachability queries.
    pub fn graph(&self) -> &RouteGraph {
        &self.graph
    }

    /// Add a route at runtime. Also updates the RouteGraph.
    pub fn add_route(&mut self, route: TransferRoute) {
        self.graph.add(route.src().clone(), route.dest().clone());
        let key = (route.src().clone(), route.dest().clone());
        self.routes.insert(key, route);
    }

    /// Remove all routes targeting a specific destination. Also updates the graph.
    pub fn remove_routes_for(&mut self, dest: &LocationId) {
        let srcs_to_remove: Vec<LocationId> = self
            .routes
            .keys()
            .filter(|(_, d)| d == dest)
            .map(|(s, _)| s.clone())
            .collect();
        self.routes.retain(|(_src, d), _| d != dest);
        for src in srcs_to_remove {
            self.graph.remove(&src, dest);
        }
    }

    /// Find a route from src to dest. O(1) HashMap lookup.
    pub fn find_route(&self, src: &LocationId, dest: &LocationId) -> Option<&TransferRoute> {
        self.routes.get(&(src.clone(), dest.clone()))
    }

    /// Collect unique destination LocationIds from all registered routes.
    pub fn route_destinations(&self) -> Vec<LocationId> {
        self.graph.all_destinations().into_iter().collect()
    }

    /// Destinations ordered by BFS distance from local.
    ///
    /// Used by `force()` to process chain transfers in dependency order:
    /// e.g., `cloud` before `pod` when the graph is `local→cloud→pod`.
    pub fn route_destinations_ordered(&self) -> Vec<LocationId> {
        self.graph.destinations_ordered_from(&LocationId::local())
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

    /// Iterate over all routes (for external inspection).
    pub fn routes(&self) -> impl Iterator<Item = &TransferRoute> {
        self.routes.values()
    }

    // =========================================================================
    // Transfer execution — v2 (Transfer object based)
    // =========================================================================

    /// Force-sync all queued transfers across the entire topology.
    ///
    /// Processes destinations in BFS order from local first (e.g., cloud before pod
    /// for chain transfers). On completion, creates next-hop Transfers for
    /// downstream destinations.
    pub async fn force(
        &self,
        file_store: &dyn FileStore,
        transfer_store: &dyn TransferStore,
    ) -> Result<BatchResult, SyncError> {
        let mut result = BatchResult::default();

        // BFS from local first (chain dependency order)
        let mut targets = self.route_destinations_ordered();

        // Append any destinations not reachable from local
        for dest in self.graph.all_destinations() {
            if !targets.contains(&dest) {
                targets.push(dest);
            }
        }

        for target in &targets {
            let batch = self
                .force_target(file_store, transfer_store, target)
                .await?;
            result.pushed += batch.pushed;
            result.failed += batch.failed;
            result.errors.extend(batch.errors);
        }

        Ok(result)
    }

    /// Force-sync queued transfers for a specific route (src → dest).
    ///
    /// Explicit source and destination. No next-hop creation.
    /// Returns error if no route is registered for (src, dest).
    pub async fn force_route(
        &self,
        file_store: &dyn FileStore,
        transfer_store: &dyn TransferStore,
        src: &LocationId,
        dest: &LocationId,
    ) -> Result<BatchResult, SyncError> {
        let route = self.find_route(src, dest).ok_or_else(|| {
            SyncError::TransferFailed(format!("no route registered: {src} → {dest}"))
        })?;

        let queued = transfer_store.queued_transfers(dest).await?;
        // Filter: only transfers with matching src
        let eligible: Vec<_> = queued.into_iter().filter(|t| t.src() == src).collect();

        let mut result = BatchResult::default();

        let outcomes: Vec<_> = stream::iter(eligible.into_iter().map(|mut transfer| async move {
            let file = file_store
                .get_file_by_id(transfer.file_id())
                .await
                .map_err(|e| (transfer.file_id().to_string(), e.to_string()))?
                .ok_or_else(|| {
                    (
                        transfer.file_id().to_string(),
                        format!("file {} not found in store", transfer.file_id()),
                    )
                })?;

            // Source file existence check
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

            Self::execute_transfer(transfer_store, &mut transfer, route, file.relative_path())
                .await
                .map_err(|e| (file.relative_path().to_string(), e.to_string()))
        }))
        .buffer_unordered(self.force_concurrency)
        .collect()
        .await;

        for outcome in outcomes {
            match outcome {
                Ok(()) => result.pushed += 1,
                Err((path, msg)) => {
                    result.failed += 1;
                    result.errors.push(BatchError { path, error: msg });
                }
            }
        }

        Ok(result)
    }

    /// Push a single file to specific destination(s).
    ///
    /// If `dest` is Some, pushes to that destination only.
    /// If `dest` is None, pushes all queued transfers for this file.
    pub async fn push_file(
        &self,
        file_store: &dyn FileStore,
        transfer_store: &dyn TransferStore,
        relative_path: &str,
        dest: Option<&LocationId>,
    ) -> Result<BatchResult, SyncError> {
        let file = file_store
            .get_file_by_path(relative_path)
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

            match Self::execute_transfer(transfer_store, &mut transfer, route, relative_path).await
            {
                Ok(()) => result.pushed += 1,
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

    /// Force-sync queued transfers for a single target destination.
    ///
    /// Creates next-hop Transfers on completion for chain transfers.
    async fn force_target(
        &self,
        file_store: &dyn FileStore,
        transfer_store: &dyn TransferStore,
        target: &LocationId,
    ) -> Result<BatchResult, SyncError> {
        let queued = transfer_store.queued_transfers(target).await?;
        let mut result = BatchResult::default();

        let outcomes: Vec<_> = stream::iter(queued.into_iter().map(|mut transfer| async move {
            let file = file_store
                .get_file_by_id(transfer.file_id())
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

            // Source file existence check
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

            Self::execute_transfer(transfer_store, &mut transfer, route, file.relative_path())
                .await
                .map(|()| transfer) // return completed transfer for next-hop
                .map_err(|e| (file.relative_path().to_string(), e.to_string()))
        }))
        .buffer_unordered(self.force_concurrency)
        .collect()
        .await;

        for outcome in outcomes {
            match outcome {
                Ok(completed) => {
                    result.pushed += 1;
                    // Create next-hop transfers for chain routing
                    self.create_next_hop_transfers(transfer_store, &completed)
                        .await?;
                }
                Err((path, msg)) => {
                    result.failed += 1;
                    result.errors.push(BatchError { path, error: msg });
                }
            }
        }

        Ok(result)
    }

    /// Execute a single transfer: start → route.transfer → complete/fail.
    ///
    /// Manages the Queued→InFlight→Completed/Failed state transitions
    /// on the Transfer object, persisting each transition.
    async fn execute_transfer(
        transfer_store: &dyn TransferStore,
        transfer: &mut Transfer,
        route: &TransferRoute,
        relative_path: &str,
    ) -> Result<(), SyncError> {
        transfer.start()?;
        transfer_store.update_transfer(transfer).await?;

        match route.transfer(relative_path).await {
            Ok(()) => {
                transfer.complete()?;
                transfer_store.update_transfer(transfer).await?;
                Ok(())
            }
            Err(e) => {
                let err_msg = e.to_string();
                // Backend transfer errors are transient (network, timeout, etc.)
                if let Err(state_err) = transfer.fail(err_msg, TransferErrorKind::Transient) {
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

    /// Create next-hop Transfers after a successful transfer completes.
    ///
    /// For chain routing (local→cloud→pod): when a Transfer to cloud completes,
    /// creates a new Transfer(cloud, pod) for downstream delivery.
    async fn create_next_hop_transfers(
        &self,
        transfer_store: &dyn TransferStore,
        completed: &Transfer,
    ) -> Result<(), SyncError> {
        let completed_dest = completed.dest();
        let next_dests = self.graph.direct_from(completed_dest);

        for next_dest in next_dests {
            if self.find_route(completed_dest, next_dest).is_some() {
                let t = Transfer::new(
                    completed.file_id().to_string(),
                    completed_dest.clone(),
                    next_dest.clone(),
                )?;
                transfer_store.insert_transfer(&t).await?;
            }
        }

        Ok(())
    }
}
