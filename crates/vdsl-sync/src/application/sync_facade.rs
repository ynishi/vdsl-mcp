//! SyncFacade — TopologyStore（計画）+ TransferEngine（実行）を合成するファサード。
//!
//! 旧Store互換のAPIを提供しつつ、内部はTopology中心モデルを使用する。
//!
//! # 責務分離
//!
//! - **計画**: TopologyStore が Ingest→Distribute→Route→Transfer作成を担当
//! - **実行**: TransferEngine が Transfer実行（BFS順序・並行実行）を担当
//! - **SyncFacade**: 上記2つを統合し、sync/put/get/list/delete の外部APIを提供

use std::path::Path;
use std::sync::Arc;

use super::observer::{NullObserver, SyncObserver};
use super::route::TransferRoute;
use super::store::{BatchResult, ScanError};
use super::topology_store::{
    TopologyFileView, TopologyPutResult, TopologyStore, TopologySyncResult,
};
use super::transfer_engine::TransferEngine;
use crate::application::error::SyncError;
use crate::domain::config::SyncConfig;
use crate::domain::file_type::FileType;
use crate::domain::fingerprint::FileFingerprint;
use crate::domain::location::{LocationId, SyncSummary};
use crate::domain::scan::ScanReport;
use crate::domain::topology_delta::TopologyDelta;
use crate::infra::hasher::{ContentHasher, Djb2Hasher};
use crate::infra::location_file_store::LocationFileStore;
use crate::infra::topology_file_store::TopologyFileStore;
use crate::infra::transfer_store::TransferStore;

// =============================================================================
// SyncFacade Result (sync+execution合成)
// =============================================================================

/// SyncFacade::sync()の結果。計画+実行の両方を含む。
///
/// TODO: SyncReportに統合後、除去する。
#[deprecated(note = "use SyncReport — FacadeSyncResult is a transitional type")]
#[derive(Debug, Clone, Default, serde::Serialize)]
pub struct FacadeSyncResult {
    /// スキャンフェーズの結果。
    pub scanned: usize,
    /// スキャンエラー（非致命的）。
    pub scan_errors: Vec<ScanError>,
    /// ロケーション別スキャン結果。
    pub scan_report: ScanReport,
    /// 計画フェーズの結果。
    pub plan: TopologySyncResult,
    /// 実行フェーズの結果。
    #[serde(flatten)]
    pub batch: BatchResult,
}

impl FacadeSyncResult {
    /// JSON Value変換。
    pub fn to_value(&self) -> Result<serde_json::Value, SyncError> {
        serde_json::to_value(self).map_err(|e| -> SyncError {
            crate::infra::error::InfraError::Serialization(format!("FacadeSyncResult: {e}")).into()
        })
    }
}

// =============================================================================
// SyncFacade
// =============================================================================

/// TopologyStore（計画）+ TransferEngine（実行）を統合するファサード。
///
/// 旧Store互換のAPI（put/get/list/sync/delete/status）を提供しつつ、
/// 内部はTopology中心モデル（TopologyFile + LocationFile + RouteGraph）を使用する。
///
/// TODO: SyncStoreSdk実装体に統合後、除去する。
#[deprecated(note = "use SyncStoreSdk — SyncFacade is a transitional adapter")]
pub struct SyncFacade {
    /// 計画層: Ingest→Distribute→Route→Transfer作成。
    topology: TopologyStore,
    /// 実行層: Transfer実行（BFS順序・並行実行）。
    engine: TransferEngine,
    /// TopologyFile永続化（TransferEngineのファイルID→パス解決用）。
    topology_files: Arc<dyn TopologyFileStore>,
    /// Transfer永続化（実行時に必要）。
    transfer_store: Arc<dyn TransferStore>,
    /// コンテンツハッシャー（put時のローカルファイルハッシュ用）。
    hasher: Arc<dyn ContentHasher>,
    /// 同期設定。
    config: SyncConfig,
    /// スキャン除外パターン。
    scan_excludes: Vec<glob::Pattern>,
}

impl SyncFacade {
    // =========================================================================
    // Topology (read-only)
    // =========================================================================

    /// All edges in the topology as `(src, dest)` pairs.
    pub fn all_edges(&self) -> Vec<(LocationId, LocationId)> {
        self.engine.all_edges()
    }

    /// Local file root resolved from routes.
    pub fn local_root(&self) -> Option<&Path> {
        self.engine.local_root()
    }

    /// Location一覧。
    pub fn locations(&self) -> &[LocationId] {
        self.topology.locations()
    }

    /// RouteGraph参照。
    pub fn graph(&self) -> &crate::domain::graph::RouteGraph {
        self.topology.graph()
    }

    // =========================================================================
    // File CRUD
    // =========================================================================

    /// ファイル登録。
    ///
    /// ローカルファイルの場合、ハッシュ計算後にTopologyStoreに委譲。
    pub async fn put(
        &self,
        path: &str,
        file_type: FileType,
        fingerprint: FileFingerprint,
        origin: &LocationId,
        embedded_id: Option<String>,
    ) -> Result<TopologyPutResult, SyncError> {
        self.topology
            .put(path, file_type, fingerprint, origin, embedded_id)
            .await
    }

    /// ローカルファイルのput（パスからハッシュ自動計算）。
    pub async fn put_local(
        &self,
        relative_path: &str,
        file_type: FileType,
        embedded_id: Option<String>,
    ) -> Result<TopologyPutResult, SyncError> {
        let local_root = self
            .local_root()
            .ok_or_else(|| SyncError::NoRouteAvailable {
                src: "local".into(),
                dest: "*".into(),
                path: relative_path.into(),
            })?;

        let abs_path = local_root.join(relative_path);
        let hash_result = self.hasher.hash_file(&abs_path)?;
        let metadata = std::fs::metadata(&abs_path)
            .map_err(|e| -> SyncError { crate::infra::error::InfraError::Io(e).into() })?;

        let fingerprint = FileFingerprint {
            file_hash: Some(hash_result.file_hash),
            content_hash: hash_result.content_hash,
            meta_hash: None,
            size: metadata.len(),
            modified_at: None,
        };

        self.topology
            .put(
                relative_path,
                file_type,
                fingerprint,
                &LocationId::local(),
                embedded_id,
            )
            .await
    }

    /// ファイル取得。
    pub async fn get(&self, relative_path: &str) -> Result<Option<TopologyFileView>, SyncError> {
        self.topology.get(relative_path).await
    }

    /// ファイル一覧。
    pub async fn list(
        &self,
        file_type: Option<FileType>,
        limit: Option<usize>,
    ) -> Result<Vec<TopologyFileView>, SyncError> {
        self.topology.list(file_type, limit).await
    }

    /// ファイル削除。
    pub async fn delete(&self, relative_path: &str) -> Result<usize, SyncError> {
        self.topology.delete(relative_path).await
    }

    // =========================================================================
    // Sync
    // =========================================================================

    /// 全体同期: 計画（TopologyDelta→Transfer作成）+ 実行（Transfer実行）。
    ///
    /// `deltas` は呼び出し元がスキャンで生成する。
    /// SyncFacadeは計画→実行を一括で行う。
    pub async fn sync(&self, deltas: &[TopologyDelta]) -> Result<FacadeSyncResult, SyncError> {
        self.sync_with_observer(deltas, &NullObserver).await
    }

    /// Observer付き全体同期。
    pub async fn sync_with_observer(
        &self,
        deltas: &[TopologyDelta],
        observer: &dyn SyncObserver,
    ) -> Result<FacadeSyncResult, SyncError> {
        // Phase 1: 計画 — TopologyStore.sync()
        let plan_result = self.topology.sync(deltas).await?;

        // Phase 2: 実行 — TransferEngine.execute_all_with_observer()
        let batch = self
            .engine
            .execute_all_with_observer(
                self.topology_files.as_ref(),
                self.transfer_store.as_ref(),
                observer,
            )
            .await?;

        Ok(FacadeSyncResult {
            scanned: plan_result.scanned,
            scan_errors: Vec::new(),
            scan_report: ScanReport::new(),
            plan: plan_result,
            batch,
        })
    }

    /// 単一ルート同期: src→dest の計画 + 実行。
    pub async fn sync_route(
        &self,
        src: &LocationId,
        dest: &LocationId,
    ) -> Result<FacadeSyncResult, SyncError> {
        // Phase 1: 計画
        let plan_result = self.topology.sync_route(src, dest).await?;

        // Phase 2: 実行 — 該当ルートのみ
        let batch = self
            .engine
            .execute_route(
                self.topology_files.as_ref(),
                self.transfer_store.as_ref(),
                src,
                dest,
            )
            .await?;

        Ok(FacadeSyncResult {
            scanned: 0,
            scan_errors: Vec::new(),
            scan_report: ScanReport::new(),
            plan: plan_result,
            batch,
        })
    }

    // =========================================================================
    // Status
    // =========================================================================

    /// ファイル数。
    pub async fn file_count(&self) -> Result<usize, SyncError> {
        self.topology.file_count().await
    }

    /// Location別の同期状態サマリー。
    ///
    /// Store.status()と同じロジックをTransferStoreの集約クエリで構築する。
    pub async fn status(&self) -> Result<SyncSummary, SyncError> {
        use crate::domain::location::LocationSummary;
        use crate::domain::transfer::TransferState;
        use crate::domain::view::{ErrorEntry, PendingEntry, PresenceState};
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

    /// SyncConfig参照。
    pub fn config(&self) -> &SyncConfig {
        &self.config
    }

    /// スキャン除外パターン。
    pub fn scan_excludes(&self) -> &[glob::Pattern] {
        &self.scan_excludes
    }

    /// TransferEngine参照（テスト用）。
    #[allow(dead_code)]
    pub(crate) fn engine(&self) -> &TransferEngine {
        &self.engine
    }
}

// =============================================================================
// SyncStoreSdk transitional impl
// =============================================================================

/// SyncFacade → SyncStoreSdk ブリッジ。
///
/// scan統合が完了するまでの暫定実装。
/// sync_with_observer/force_rewrite_with_observer は空deltaで委譲（scan未統合）。
///
/// TODO: scan統合後、SyncFacade自体を除去し、SDK専用の実装体に移行する。
#[allow(deprecated)]
#[async_trait::async_trait]
impl super::sdk::SyncStoreSdk for SyncFacade {
    async fn sync(&self) -> Result<super::sdk::SyncReport, SyncError> {
        // TODO: scan統合 — 現在は空delta（スキャンなし、実行のみ）
        let result = SyncFacade::sync_with_observer(self, &[], &NullObserver).await?;
        Ok(facade_to_report(&result))
    }

    async fn sync_route(
        &self,
        src: &LocationId,
        dest: &LocationId,
    ) -> Result<super::sdk::SyncReport, SyncError> {
        let result = SyncFacade::sync_route(self, src, dest).await?;
        Ok(facade_to_report(&result))
    }

    async fn force_rewrite(&self) -> Result<super::sdk::SyncReport, SyncError> {
        // TODO: force_rewrite統合 — 現在は空deltaで通常sync
        let result = SyncFacade::sync(self, &[]).await?;
        Ok(facade_to_report(&result))
    }

    async fn put(
        &self,
        path: &str,
        file_type: FileType,
        fingerprint: FileFingerprint,
        origin: &LocationId,
        embedded_id: Option<String>,
    ) -> Result<super::sdk::PutReport, SyncError> {
        let result =
            SyncFacade::put(self, path, file_type, fingerprint, origin, embedded_id).await?;
        Ok(super::sdk::PutReport {
            file_id: result.topology_file_id,
            is_new: result.is_new,
            transfers_created: result.transfers_created,
        })
    }

    async fn delete(&self, path: &str) -> Result<usize, SyncError> {
        SyncFacade::delete(self, path).await
    }

    async fn get(&self, path: &str) -> Result<Option<TopologyFileView>, SyncError> {
        SyncFacade::get(self, path).await
    }

    async fn list(
        &self,
        file_type: Option<FileType>,
        limit: Option<usize>,
    ) -> Result<Vec<TopologyFileView>, SyncError> {
        SyncFacade::list(self, file_type, limit).await
    }

    async fn status(&self) -> Result<SyncSummary, SyncError> {
        SyncFacade::status(self).await
    }

    async fn errors(&self) -> Result<Vec<crate::domain::view::ErrorEntry>, SyncError> {
        let summary = SyncFacade::status(self).await?;
        Ok(summary.error_entries)
    }

    async fn pending(
        &self,
        dest: &LocationId,
    ) -> Result<Vec<crate::domain::view::PendingEntry>, SyncError> {
        let summary = SyncFacade::status(self).await?;
        Ok(summary
            .pending_entries
            .into_iter()
            .filter(|e| &e.dest == dest)
            .collect())
    }

    fn locations(&self) -> Vec<LocationId> {
        SyncFacade::locations(self).to_vec()
    }

    fn all_edges(&self) -> Vec<(LocationId, LocationId)> {
        SyncFacade::all_edges(self)
    }

    fn local_root(&self) -> Option<&Path> {
        SyncFacade::local_root(self)
    }
}

/// FacadeSyncResult → SyncReport 変換。
#[allow(deprecated)]
fn facade_to_report(result: &FacadeSyncResult) -> super::sdk::SyncReport {
    super::sdk::SyncReport {
        scanned: result.scanned,
        scan_errors: Vec::new(),
        transfers_created: result.plan.transfers_created,
        transferred: result.batch.transferred,
        failed: result.batch.failed,
        errors: result
            .batch
            .errors
            .iter()
            .map(|e| super::sdk::SyncReportError {
                path: e.path.clone(),
                error: e.error.clone(),
            })
            .collect(),
        conflicts: result
            .plan
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
    }
}

// =============================================================================
// SyncFacadeBuilder
// =============================================================================

/// SyncFacadeのビルダー。
///
/// TopologyFileStore + LocationFileStore + TransferStore + routes から構築する。
///
/// TODO: SyncStoreSdk実装体に統合後、除去する。
#[deprecated(note = "use SyncStoreSdk — SyncFacadeBuilder is a transitional adapter")]
pub struct SyncFacadeBuilder {
    topology_files: Arc<dyn TopologyFileStore>,
    location_files: Arc<dyn LocationFileStore>,
    transfer_store: Arc<dyn TransferStore>,
    routes: Vec<TransferRoute>,
    locations: Vec<LocationId>,
    hasher: Option<Arc<dyn ContentHasher>>,
    config: Option<SyncConfig>,
    scan_excludes: Vec<glob::Pattern>,
}

impl SyncFacadeBuilder {
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
            routes: Vec::new(),
            locations: Vec::new(),
            hasher: None,
            config: None,
            scan_excludes: Vec::new(),
        }
    }

    /// Transfer route追加。
    pub fn route(mut self, route: TransferRoute) -> Self {
        self.routes.push(route);
        self
    }

    /// 複数route追加。
    pub fn routes(mut self, routes: impl IntoIterator<Item = TransferRoute>) -> Self {
        self.routes.extend(routes);
        self
    }

    /// Location追加。
    pub fn location(mut self, id: LocationId) -> Self {
        if !self.locations.contains(&id) {
            self.locations.push(id);
        }
        self
    }

    /// Content hasher設定。
    pub fn hasher(mut self, hasher: Arc<dyn ContentHasher>) -> Self {
        self.hasher = Some(hasher);
        self
    }

    /// SyncConfig設定。
    pub fn sync_config(mut self, config: SyncConfig) -> Self {
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

    /// SyncFacadeを構築。
    ///
    /// routesからLocationId一覧とRouteGraphを自動構築する。
    pub fn build(self) -> SyncFacade {
        let config = self.config.unwrap_or_default();

        // routesからLocation一覧を導出（明示追加分 + route端点）
        let mut locations = self.locations;
        for r in &self.routes {
            if !locations.contains(r.src()) {
                locations.push(r.src().clone());
            }
            if !locations.contains(r.dest()) {
                locations.push(r.dest().clone());
            }
        }

        // RouteGraph構築
        let mut graph = crate::domain::graph::RouteGraph::new();
        for r in &self.routes {
            graph.add_with_cost(
                r.src().clone(),
                r.dest().clone(),
                crate::domain::graph::EdgeCost::new(r.time_per_gb(), r.priority()),
            );
        }

        let topology = TopologyStore::new(
            self.topology_files.clone(),
            self.location_files.clone(),
            self.transfer_store.clone(),
            graph,
            locations,
        );

        let engine = TransferEngine::new(self.routes, config.concurrency);

        SyncFacade {
            topology,
            engine,
            topology_files: self.topology_files,
            transfer_store: self.transfer_store,
            hasher: self.hasher.unwrap_or_else(|| Arc::new(Djb2Hasher)),
            config,
            scan_excludes: self.scan_excludes,
        }
    }
}
