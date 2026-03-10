//! TransferEngine — route-based transfer orchestrator.
//!
//! Owns the route map and executes concurrent transfers.
//! Separated from [`SyncService`] to isolate routing/transfer concerns
//! from notify/register/store-delegation responsibilities.

use std::collections::{HashMap, HashSet};

use futures::stream::{self, StreamExt};
use tracing::warn;

use crate::domain::entry::SyncEntry;
use crate::domain::error::SyncError;
use crate::domain::location::{LocationId, LocationState};
use crate::domain::route::TransferRoute;
use crate::infra::store::SyncStore;

/// Route map key: `(src, dest)` LocationId pair.
type RouteKey = (LocationId, LocationId);

/// Result of a batch push operation.
#[derive(Debug, Default)]
pub struct BatchResult {
    pub pushed: usize,
    pub failed: usize,
    pub errors: Vec<(String, String)>,
}

/// Route-based transfer engine.
///
/// Manages directed transfer routes and executes concurrent file transfers.
/// Does NOT own the store — store is passed by reference to `force()`.
pub struct TransferEngine {
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
    pub fn new(routes: Vec<TransferRoute>) -> Self {
        Self {
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

    /// Add a route at runtime.
    pub fn add_route(&mut self, route: TransferRoute) {
        let key = (route.src().clone(), route.dest().clone());
        self.routes.insert(key, route);
    }

    /// Remove all routes targeting a specific destination.
    pub fn remove_routes_for(&mut self, dest: &LocationId) {
        self.routes.retain(|(_src, d), _| d != dest);
    }

    /// Find a route from src to dest. O(1) HashMap lookup.
    pub fn find_route(&self, src: &LocationId, dest: &LocationId) -> Option<&TransferRoute> {
        self.routes.get(&(src.clone(), dest.clone()))
    }

    /// Find a route for transferring an entry to the given destination.
    ///
    /// Searches entry.locations for a src that is Present,
    /// then finds a matching route (src, dest) in self.routes.
    ///
    /// Source selection priority:
    /// 1. Local (lowest latency, most reliable file existence check)
    /// 2. Any other Present location with a matching route
    pub fn find_route_for_entry(
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
    pub fn route_destinations(&self) -> Vec<LocationId> {
        self.routes
            .keys()
            .map(|(_src, dest)| dest.clone())
            .collect::<HashSet<_>>()
            .into_iter()
            .collect()
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
    pub async fn force(
        &self,
        store: &dyn SyncStore,
        dest: Option<&LocationId>,
    ) -> Result<BatchResult, SyncError> {
        let mut result = BatchResult::default();

        let targets: Vec<LocationId> = if let Some(d) = dest {
            vec![d.clone()]
        } else {
            self.route_destinations()
        };

        for target in &targets {
            let pending = store.pending(target).await?;

            let outcomes: Vec<_> = stream::iter(pending.into_iter().map(|entry| async move {
                // --- Source selection ---
                let route = match self.find_route_for_entry(&entry, target) {
                    Some(r) => r,
                    None => {
                        return Err((
                            entry.relative_path,
                            format!("no route available to {target}"),
                        ));
                    }
                };

                // --- File existence check on src ---
                match route.src_file_exists(&entry.relative_path).await {
                    Ok(true) => {}
                    Ok(false) => {
                        if let Err(e) = store
                            .set_location_state(&entry.id, route.src(), LocationState::Absent)
                            .await
                        {
                            warn!(
                                path = %entry.relative_path,
                                src = %route.src(),
                                error = %e,
                                "failed to mark as Absent"
                            );
                        }
                        return Err((
                            entry.relative_path,
                            format!("source file not found on {}", route.src()),
                        ));
                    }
                    Err(e) => {
                        return Err((entry.relative_path, e.to_string()));
                    }
                }

                // --- Transfer via route ---
                Self::push_via_route(store, &entry, route, target)
                    .await
                    .map_err(|e| (entry.relative_path, e.to_string()))
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

    /// Push a single entry via a resolved route.
    ///
    /// Manages the Syncing→Present/Pending state transitions and error recording.
    /// Called from `force()` after source selection and file existence checks.
    async fn push_via_route(
        store: &dyn SyncStore,
        entry: &SyncEntry,
        route: &TransferRoute,
        dest: &LocationId,
    ) -> Result<(), SyncError> {
        store
            .set_location_state(&entry.id, dest, LocationState::Syncing)
            .await?;
        store.set_error(&entry.relative_path, None).await?;

        match route.transfer(&entry.relative_path).await {
            Ok(()) => {
                store
                    .set_location_state(&entry.id, dest, LocationState::Present)
                    .await?;
                store
                    .set_synced_at(&entry.relative_path, chrono::Utc::now())
                    .await?;
                Ok(())
            }
            Err(e) => {
                if let Err(store_err) = store
                    .set_location_state(&entry.id, dest, LocationState::Pending)
                    .await
                {
                    warn!(
                        path = %entry.relative_path,
                        dest = %dest,
                        error = %store_err,
                        "failed to revert to Pending"
                    );
                }
                if let Err(store_err) = store
                    .set_error(&entry.relative_path, Some(&e.to_string()))
                    .await
                {
                    warn!(
                        path = %entry.relative_path,
                        error = %store_err,
                        "failed to record sync error"
                    );
                }
                Err(e)
            }
        }
    }
}
