//! SdkImpl — SyncStoreSdk の本実装。
//!
//! scan→delta→plan→execute の全パイプラインを内部完結させる。
//! インターフェース層（MCP, Lua）は `Arc<dyn SyncStoreSdk>` 経由でのみ使用する。
//!
//! # 構成
//!
//! ```text
//! SdkImpl
//!   ├── scanner: TopologyScanner  — scan → TopologyDelta[]
//!   ├── topology: TopologyStore   — Apply → Distribute → Route → Transfer作成
//!   ├── engine: TransferEngine    — Transfer実行
//!   ├── transfer_store            — Transfer永続化（execute時に必要）
//!   ├── topology_files            — TopologyFile参照（execute時に必要）
//!   ├── config: SyncConfig        — リトライ/並行数
//!   └── scan_excludes             — globパターン
//! ```

use std::path::Path;
use std::sync::Arc;

use async_trait::async_trait;

use super::route::{TransferDirection, TransferRoute};
use super::sdk::{PutReport, SyncReport, SyncReportError, SyncStoreSdk};
use super::topology_scanner::TopologyScanner;
use super::topology_store::{TopologyFileView, TopologyStore};
use super::transfer_engine::{PreparedTransfer, TransferEngine};
use crate::application::error::SyncError;
use crate::domain::config::SyncConfig;
use crate::domain::file_type::FileType;
use crate::domain::fingerprint::FileFingerprint;
use crate::domain::graph::{EdgeCost, RouteGraph};
use crate::domain::location::{LocationId, SyncSummary};
use crate::domain::transfer::TransferState;
use crate::domain::view::{ErrorEntry, PendingEntry, PresenceState};
use crate::infra::backend::StorageBackend;
use crate::infra::location::{Location, LocationKind};
use crate::infra::location_file_store::LocationFileStore;
use crate::infra::location_scanner::LocationScanner;
use crate::infra::shell::RemoteShell;
use crate::infra::topology_file_store::TopologyFileStore;
use crate::infra::transfer_store::TransferStore;

/// SyncStoreSdkの本実装。
///
/// scan→delta→plan→execute を一貫して実行する。
/// インターフェース層は `Arc<dyn SyncStoreSdk>` として保持する。
pub struct SdkImpl {
    scanner: TopologyScanner,
    topology: TopologyStore,
    engine: TransferEngine,
    topology_files: Arc<dyn TopologyFileStore>,
    transfer_store: Arc<dyn TransferStore>,
    config: SyncConfig,
    scan_excludes: Vec<glob::Pattern>,
}

// =============================================================================
// SdkImplBuilder — 外部crateからの構築用
// =============================================================================

/// ルート接続の中間表現。
///
/// `connect()` で登録され、`build()` 時に Location の `file_root()` で
/// TransferRoute に変換される。コストは `LocationKind` の組み合わせから自動推定。
struct PendingRoute {
    src: LocationId,
    dest: LocationId,
    backend: Box<dyn StorageBackend>,
    src_shell: Option<Box<dyn RemoteShell>>,
    direction: TransferDirection,
}

/// SdkImplのビルダー。
///
/// Location（拠点）を `location()` で登録し、ルートを `connect()` で宣言する。
/// Location からスキャナーが自動導出され、ルートコストは `LocationKind` の
/// 組み合わせから自動推定される。
///
/// # 使用例
///
/// ```ignore
/// let sdk = SdkImplBuilder::new(topology_files, location_files, transfer_store)
///     .location(Arc::new(LocalLocation::new(root, hasher)))
///     .location(Arc::new(SshLocation::new(pod_id, pod_root, shell)))
///     .location(Arc::new(CloudLocation::new(cloud_id, cloud_root, backend)))
///     .connect(&local_id, &cloud_id, rclone_backend)
///     .connect_with_shell(&pod_id, &cloud_id, pod_rclone, pod_shell)
///     .connect_pull(&cloud_id, &local_id, rclone_pull)
///     .exclude(".git")
///     .build();
/// ```
pub struct SdkImplBuilder {
    topology_files: Arc<dyn TopologyFileStore>,
    location_files: Arc<dyn LocationFileStore>,
    transfer_store: Arc<dyn TransferStore>,
    locations: Vec<Arc<dyn Location>>,
    pending_routes: Vec<PendingRoute>,
    config: Option<SyncConfig>,
    scan_excludes: Vec<glob::Pattern>,
}

impl SdkImplBuilder {
    /// 必須3ストアでビルダーを作成。
    pub fn new(
        topology_files: Arc<dyn TopologyFileStore>,
        location_files: Arc<dyn LocationFileStore>,
        transfer_store: Arc<dyn TransferStore>,
    ) -> Self {
        Self {
            topology_files,
            location_files,
            transfer_store,
            locations: Vec::new(),
            pending_routes: Vec::new(),
            config: None,
            scan_excludes: Vec::new(),
        }
    }

    /// Location（拠点）追加。
    ///
    /// Location trait 実装からスキャナーが自動導出される。
    /// 同じLocationIdの二重登録は無視される。
    pub fn location(mut self, loc: Arc<dyn Location>) -> Self {
        if !self.locations.iter().any(|l| l.id() == loc.id()) {
            self.locations.push(loc);
        }
        self
    }

    /// Push方向のルート接続を宣言。
    ///
    /// `src` → `dest` への転送ルートを登録する。
    /// `file_root` は `build()` 時に Location から自動解決される。
    /// コストは `LocationKind` の組み合わせから自動推定される。
    pub fn connect(
        mut self,
        src: &LocationId,
        dest: &LocationId,
        backend: Box<dyn StorageBackend>,
    ) -> Self {
        self.pending_routes.push(PendingRoute {
            src: src.clone(),
            dest: dest.clone(),
            backend,
            src_shell: None,
            direction: TransferDirection::Push,
        });
        self
    }

    /// Push方向 + ソース側Shell付きのルート接続。
    ///
    /// リモートホスト（Pod等）がソースの場合、ソース側でのファイル操作
    /// （存在確認、ハッシュ計算）に `src_shell` を使用する。
    pub fn connect_with_shell(
        mut self,
        src: &LocationId,
        dest: &LocationId,
        backend: Box<dyn StorageBackend>,
        src_shell: Box<dyn RemoteShell>,
    ) -> Self {
        self.pending_routes.push(PendingRoute {
            src: src.clone(),
            dest: dest.clone(),
            backend,
            src_shell: Some(src_shell),
            direction: TransferDirection::Push,
        });
        self
    }

    /// Pull方向のルート接続。
    ///
    /// Cloud → Local, Cloud → Pod のように、リモートからローカル方向への
    /// 転送ルートを登録する。`backend.pull()` が使用される。
    pub fn connect_pull(
        mut self,
        src: &LocationId,
        dest: &LocationId,
        backend: Box<dyn StorageBackend>,
    ) -> Self {
        self.pending_routes.push(PendingRoute {
            src: src.clone(),
            dest: dest.clone(),
            backend,
            src_shell: None,
            direction: TransferDirection::Pull,
        });
        self
    }

    /// SyncConfig設定。
    pub fn config(mut self, config: SyncConfig) -> Self {
        self.config = Some(config);
        self
    }

    /// スキャン除外パターン追加。
    pub fn exclude(mut self, pattern: &str) -> Self {
        if let Ok(p) = glob::Pattern::new(pattern) {
            self.scan_excludes.push(p);
        }
        self
    }

    /// SdkImplを構築。
    ///
    /// 1. Location から Scanner を自動導出
    /// 2. PendingRoute → TransferRoute に変換（file_root 自動解決 + コスト自動推定）
    /// 3. TransferRoute から RouteGraph + TransferEngine を構築
    /// 4. TopologyStore + TopologyScanner を構築
    pub fn build(self) -> SdkImpl {
        use std::collections::HashMap;

        let config = self.config.unwrap_or_default();

        // Location map: LocationId → Arc<dyn Location>
        let loc_map: HashMap<LocationId, &Arc<dyn Location>> =
            self.locations.iter().map(|l| (l.id().clone(), l)).collect();

        // Scanner 導出
        let scanners: Vec<Arc<dyn LocationScanner>> =
            self.locations.iter().map(|loc| loc.scanner()).collect();

        // PendingRoute → TransferRoute 変換
        let routes: Vec<TransferRoute> = self
            .pending_routes
            .into_iter()
            .filter_map(|pr| {
                let src_loc = loc_map.get(&pr.src)?;
                let dest_loc = loc_map.get(&pr.dest)?;

                let cost = estimate_route_cost(src_loc.kind(), dest_loc.kind());

                let mut route = TransferRoute::new(
                    pr.src,
                    pr.dest,
                    src_loc.file_root().to_path_buf(),
                    dest_loc.file_root().to_path_buf(),
                    pr.backend,
                )
                .with_cost(cost.time_per_gb, cost.priority);

                if pr.direction == TransferDirection::Pull {
                    route = route.direction(TransferDirection::Pull);
                }
                if let Some(shell) = pr.src_shell {
                    route = route.with_src_shell(shell);
                }

                Some(route)
            })
            .collect();

        // Location一覧
        let location_ids: Vec<LocationId> =
            self.locations.iter().map(|loc| loc.id().clone()).collect();

        // RouteGraph（TopologyStore 用）
        let mut graph = RouteGraph::new();
        for r in &routes {
            graph.add_with_cost(
                r.src().clone(),
                r.dest().clone(),
                EdgeCost::new(r.time_per_gb(), r.priority()),
            );
        }

        let topology = TopologyStore::new(
            self.topology_files.clone(),
            self.location_files.clone(),
            self.transfer_store.clone(),
            graph,
            location_ids,
        );

        let engine = TransferEngine::new(routes, config.concurrency);

        let scanner = TopologyScanner::new(
            self.topology_files.clone(),
            self.location_files.clone(),
            scanners,
        );

        SdkImpl {
            scanner,
            topology,
            engine,
            topology_files: self.topology_files,
            transfer_store: self.transfer_store,
            config,
            scan_excludes: self.scan_excludes,
        }
    }
}

/// LocationKind の組み合わせからルートコストを自動推定する。
///
/// optimal_tree（Dijkstra）がこのコストで最適経路を計算する。
/// 例: Local→Pod(1.0) + Pod→Cloud(2.0) = 3.0 < Local→Cloud(5.0)
/// → Pod経由チェーンが自動的に選択される。MCP層での条件分岐は不要。
fn estimate_route_cost(src: LocationKind, dest: LocationKind) -> EdgeCost {
    let (time_per_gb, priority) = match (src, dest) {
        // LAN/SSH: 低コスト（ローカルネットワーク、低レイテンシ）
        (LocationKind::Local, LocationKind::Remote) => (1.0, 10),
        (LocationKind::Remote, LocationKind::Local) => (1.0, 10),

        // DC帯域: 中コスト（データセンター内 or DC→Cloud）
        (LocationKind::Remote, LocationKind::Cloud) => (2.0, 50),
        (LocationKind::Cloud, LocationKind::Remote) => (2.0, 50),

        // WAN: 高コスト（家庭回線アップロード/ダウンロード）
        (LocationKind::Local, LocationKind::Cloud) => (5.0, 100),
        (LocationKind::Cloud, LocationKind::Local) => (5.0, 100),

        // 同種間: 中立（通常は発生しないが安全なフォールバック）
        _ => (1.0, 100),
    };
    EdgeCost::new(time_per_gb, priority)
}

impl SdkImpl {
    /// BFS順でTransfer実行 + DB永続化。
    ///
    /// Engine.execute_prepared()で純粋なroute I/Oを実行し、
    /// 結果をtransfer_storeに永続化 + unblock_dependentsでチェーン転送を解放する。
    async fn execute_bfs(&self) -> Result<(usize, usize, Vec<SyncReportError>), SyncError> {
        let mut total_transferred = 0usize;
        let mut total_failed = 0usize;
        let mut all_errors: Vec<SyncReportError> = Vec::new();

        let targets = self.engine.all_targets_ordered();

        for target in &targets {
            let queued = self.transfer_store.queued_transfers(target).await?;
            if queued.is_empty() {
                continue;
            }

            // Prepare: resolve relative_path from topology_files
            let mut prepared = Vec::with_capacity(queued.len());
            for transfer in queued {
                match self.topology_files.get_by_id(transfer.file_id()).await {
                    Ok(Some(file)) => {
                        prepared.push(PreparedTransfer {
                            transfer,
                            relative_path: file.relative_path().to_string(),
                        });
                    }
                    Ok(None) => {
                        total_failed += 1;
                        all_errors.push(SyncReportError {
                            path: transfer.file_id().to_string(),
                            error: format!("file {} not found in store", transfer.file_id()),
                        });
                    }
                    Err(e) => {
                        total_failed += 1;
                        all_errors.push(SyncReportError {
                            path: transfer.file_id().to_string(),
                            error: e.to_string(),
                        });
                    }
                }
            }

            // Execute: pure route I/O (no DB, no observer)
            let outcomes = self.engine.execute_prepared(prepared).await;

            // Persist: DB永続化 + chain transfer unblock
            for outcome in outcomes {
                let is_completed = outcome.transfer.state() == TransferState::Completed;
                self.transfer_store
                    .update_transfer(&outcome.transfer)
                    .await?;

                if is_completed {
                    self.transfer_store
                        .unblock_dependents(outcome.transfer.id())
                        .await?;
                    total_transferred += 1;
                } else {
                    total_failed += 1;
                    if let Some(err) = outcome.transfer.error() {
                        all_errors.push(SyncReportError {
                            path: outcome.relative_path,
                            error: err.to_string(),
                        });
                    }
                }
            }
        }

        Ok((total_transferred, total_failed, all_errors))
    }
}

#[async_trait]
impl SyncStoreSdk for SdkImpl {
    // =========================================================================
    // UseCase — 同期操作
    // =========================================================================

    async fn sync(&self) -> Result<SyncReport, SyncError> {
        // Phase 0: InFlight孤児の終端化（プロセスクラッシュ復帰）
        let cancelled = self.transfer_store.cancel_orphaned_inflight().await?;
        if cancelled > 0 {
            tracing::info!(
                cancelled_count = cancelled,
                "sync: cancelled orphaned InFlight transfers"
            );
        }

        // Phase 1: Scan → TopologyDelta[]
        let scan_result = self.scanner.scan_all(&self.scan_excludes).await?;

        // Phase 2: Plan — Apply → Distribute → Route → Transfer作成
        let plan_result = self.topology.sync(&scan_result.deltas).await?;

        // Phase 3: Execute — BFS順でTransfer実行 + DB永続化
        let (transferred, failed, errors) = self.execute_bfs().await?;

        Ok(SyncReport {
            scanned: scan_result.scanned,
            scan_errors: scan_result
                .scan_errors
                .iter()
                .map(|e| SyncReportError {
                    path: e.path.clone(),
                    error: e.error.clone(),
                })
                .collect(),
            transfers_created: plan_result.transfers_created,
            transferred,
            failed,
            errors,
            conflicts: plan_result
                .conflicts
                .iter()
                .map(|c| super::sdk::SyncReportConflict {
                    file_id: c.topology_file_id().to_string(),
                    path: c.relative_path().to_string(),
                    locations: c
                        .variants()
                        .iter()
                        .map(|v| v.location_id().to_string())
                        .collect(),
                })
                .collect(),
        })
    }

    async fn sync_route(
        &self,
        src: &LocationId,
        dest: &LocationId,
    ) -> Result<SyncReport, SyncError> {
        // Phase 0: InFlight孤児の終端化
        let cancelled = self.transfer_store.cancel_orphaned_inflight().await?;
        if cancelled > 0 {
            tracing::info!(
                cancelled_count = cancelled,
                "sync_route: cancelled orphaned InFlight transfers"
            );
        }

        // Phase 1: Plan — sync_routeはdelta生成なし、Distribute + Route のみ
        let plan_result = self.topology.sync_route(src, dest).await?;

        // Phase 2: Execute — dest宛のQueued Transferをsrcでフィルタして実行
        let queued = self.transfer_store.queued_transfers(dest).await?;
        let eligible: Vec<_> = queued.into_iter().filter(|t| t.src() == src).collect();

        let mut prepared = Vec::with_capacity(eligible.len());
        let mut total_failed = 0usize;
        let mut all_errors: Vec<SyncReportError> = Vec::new();

        for transfer in eligible {
            match self.topology_files.get_by_id(transfer.file_id()).await {
                Ok(Some(file)) => {
                    prepared.push(PreparedTransfer {
                        transfer,
                        relative_path: file.relative_path().to_string(),
                    });
                }
                Ok(None) => {
                    total_failed += 1;
                    all_errors.push(SyncReportError {
                        path: transfer.file_id().to_string(),
                        error: format!("file {} not found in store", transfer.file_id()),
                    });
                }
                Err(e) => {
                    total_failed += 1;
                    all_errors.push(SyncReportError {
                        path: transfer.file_id().to_string(),
                        error: e.to_string(),
                    });
                }
            }
        }

        let outcomes = self.engine.execute_prepared(prepared).await;
        let mut total_transferred = 0usize;

        for outcome in outcomes {
            let is_completed = outcome.transfer.state() == TransferState::Completed;
            self.transfer_store
                .update_transfer(&outcome.transfer)
                .await?;

            if is_completed {
                // sync_routeは単一ルートなのでunblock不要だが、
                // chain先がある場合に備えて実行する
                self.transfer_store
                    .unblock_dependents(outcome.transfer.id())
                    .await?;
                total_transferred += 1;
            } else {
                total_failed += 1;
                if let Some(err) = outcome.transfer.error() {
                    all_errors.push(SyncReportError {
                        path: outcome.relative_path,
                        error: err.to_string(),
                    });
                }
            }
        }

        Ok(SyncReport {
            scanned: 0,
            scan_errors: Vec::new(),
            transfers_created: plan_result.transfers_created,
            transferred: total_transferred,
            failed: total_failed,
            errors: all_errors,
            conflicts: plan_result
                .conflicts
                .iter()
                .map(|c| super::sdk::SyncReportConflict {
                    file_id: c.topology_file_id().to_string(),
                    path: c.relative_path().to_string(),
                    locations: c
                        .variants()
                        .iter()
                        .map(|v| v.location_id().to_string())
                        .collect(),
                })
                .collect(),
        })
    }

    #[allow(deprecated)]
    async fn force_rewrite(&self) -> Result<SyncReport, SyncError> {
        // force_rewrite: 全TopologyFile → 全LocationのTransferを再作成
        // 空deltaでsyncし（既存TopologyFileベースでDistribute）、全Transfer実行
        let plan_result = self.topology.sync(&[]).await?;

        let (transferred, failed, errors) = self.execute_bfs().await?;

        Ok(SyncReport {
            scanned: 0,
            scan_errors: Vec::new(),
            transfers_created: plan_result.transfers_created,
            transferred,
            failed,
            errors,
            conflicts: plan_result
                .conflicts
                .iter()
                .map(|c| super::sdk::SyncReportConflict {
                    file_id: c.topology_file_id().to_string(),
                    path: c.relative_path().to_string(),
                    locations: c
                        .variants()
                        .iter()
                        .map(|v| v.location_id().to_string())
                        .collect(),
                })
                .collect(),
        })
    }

    // =========================================================================
    // Command — ファイル操作
    // =========================================================================

    async fn put(
        &self,
        path: &str,
        file_type: FileType,
        fingerprint: FileFingerprint,
        origin: &LocationId,
        embedded_id: Option<String>,
    ) -> Result<PutReport, SyncError> {
        let result = self
            .topology
            .put(path, file_type, fingerprint, origin, embedded_id)
            .await?;
        Ok(PutReport {
            file_id: result.topology_file_id,
            is_new: result.is_new,
            transfers_created: result.transfers_created,
        })
    }

    async fn delete(&self, path: &str) -> Result<usize, SyncError> {
        self.topology.delete(path).await
    }

    // =========================================================================
    // Query — 読み取り
    // =========================================================================

    async fn get(&self, path: &str) -> Result<Option<TopologyFileView>, SyncError> {
        self.topology.get(path).await
    }

    async fn list(
        &self,
        file_type: Option<FileType>,
        limit: Option<usize>,
    ) -> Result<Vec<TopologyFileView>, SyncError> {
        self.topology.list(file_type, limit).await
    }

    async fn status(&self) -> Result<SyncSummary, SyncError> {
        use crate::domain::location::LocationSummary;
        use crate::domain::transfer::TransferState;
        use std::collections::HashMap;

        let retry_policy = self.config.retry_policy();
        let total_files = self.topology.file_count().await?;
        let stats = self.transfer_store.transfer_stats().await?;
        let present_counts = self.transfer_store.present_counts_by_location().await?;
        let failed = self.transfer_store.failed_transfers().await?;
        let pending = self.transfer_store.all_pending_transfers().await?;

        let mut locations: HashMap<LocationId, LocationSummary> = HashMap::new();
        let mut total_errors = 0usize;

        for (loc, count) in &present_counts {
            let summary = locations.entry(loc.clone()).or_default();
            summary.present = *count;
        }

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

        let error_entries: Vec<ErrorEntry> = failed
            .iter()
            .filter(|t| {
                let state = PresenceState::from_transfer(t, &retry_policy);
                state == PresenceState::Failed
            })
            .map(ErrorEntry::from_transfer)
            .collect();

        let mut pending_entries: Vec<PendingEntry> =
            pending.iter().map(PendingEntry::from_transfer).collect();
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

    async fn errors(&self) -> Result<Vec<ErrorEntry>, SyncError> {
        let summary = self.status().await?;
        Ok(summary.error_entries)
    }

    async fn pending(&self, dest: &LocationId) -> Result<Vec<PendingEntry>, SyncError> {
        let summary = self.status().await?;
        Ok(summary
            .pending_entries
            .into_iter()
            .filter(|e| &e.dest == dest)
            .collect())
    }

    // =========================================================================
    // Topology — 読み取り専用
    // =========================================================================

    fn locations(&self) -> Vec<LocationId> {
        self.topology.locations().to_vec()
    }

    fn all_edges(&self) -> Vec<(LocationId, LocationId)> {
        self.engine.all_edges()
    }

    fn local_root(&self) -> Option<&Path> {
        self.engine.local_root()
    }
}
