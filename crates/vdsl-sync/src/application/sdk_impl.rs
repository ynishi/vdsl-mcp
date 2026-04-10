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

use std::collections::HashMap;
use std::path::Path;
use std::sync::{Arc, Mutex as StdMutex};

use async_trait::async_trait;
use tracing::{debug, error, info, trace, warn};

use super::route::{TransferDirection, TransferRoute};
use super::sdk::{PutReport, SyncReport, SyncReportError, SyncStoreSdk};
use super::topology_scanner::TopologyScanner;
use super::topology_store::{TopologyFileView, TopologyStore};
use super::transfer_engine::{PreparedTransfer, TransferEngine, TransferOutcome};
use crate::application::error::SyncError;
use crate::domain::config::SyncConfig;
use crate::domain::file_type::FileType;
use crate::domain::fingerprint::FileFingerprint;
use crate::domain::graph::{EdgeCost, RouteGraph};
use crate::domain::location::{LocationId, SyncSummary};
use crate::domain::transfer::TransferState;
use crate::domain::view::{ErrorEntry, PendingEntry, PresenceState};
use crate::infra::backend::{ProgressFn, StorageBackend};
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
    location_files: Arc<dyn LocationFileStore>,
    transfer_store: Arc<dyn TransferStore>,
    locations: Vec<Arc<dyn Location>>,
    config: SyncConfig,
    scan_excludes: Vec<glob::Pattern>,
    /// Progress callback for reporting phase/chunk progress.
    progress: StdMutex<Option<ProgressFn>>,
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
///     .build()?;
/// ```
pub struct SdkImplBuilder {
    topology_files: Arc<dyn TopologyFileStore>,
    location_files: Arc<dyn LocationFileStore>,
    transfer_store: Arc<dyn TransferStore>,
    locations: Vec<Arc<dyn Location>>,
    pending_routes: Vec<PendingRoute>,
    config: Option<SyncConfig>,
    scan_excludes: Vec<glob::Pattern>,
    /// Per-destination archive root: dest LocationId → archive path.
    /// When set, all routes whose dest matches get archive-on-delete.
    archive_roots: HashMap<LocationId, std::path::PathBuf>,
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
            archive_roots: HashMap::new(),
        }
    }

    /// Enable archive-on-delete for routes targeting `dest`.
    ///
    /// All routes whose destination is `dest` will move deleted files to
    /// `{archive_root}/{ISO8601_ts}/{relative_path}` instead of hard-deleting.
    /// The backend must implement `archive_move` (e.g. RcloneBackend via
    /// `rclone moveto`).
    pub fn archive_route_to(mut self, dest: &LocationId, archive_root: std::path::PathBuf) -> Self {
        self.archive_roots.insert(dest.clone(), archive_root);
        self
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
        match glob::Pattern::new(pattern) {
            Ok(p) => self.scan_excludes.push(p),
            Err(e) => {
                tracing::warn!(pattern = pattern, error = %e, "invalid exclude glob pattern, skipped");
            }
        }
        self
    }

    /// SdkImplを構築。
    ///
    /// 1. Location から Scanner を自動導出
    /// 2. PendingRoute → TransferRoute に変換（file_root 自動解決 + コスト自動推定）
    /// 3. TransferRoute から RouteGraph + TransferEngine を構築
    /// 4. TopologyStore + TopologyScanner を構築
    pub fn build(self) -> Result<SdkImpl, SyncError> {
        let config = self.config.unwrap_or_default();

        // Location map: LocationId → Arc<dyn Location>
        let loc_map: HashMap<LocationId, &Arc<dyn Location>> =
            self.locations.iter().map(|l| (l.id().clone(), l)).collect();

        // Scanner 導出
        let scanners: Vec<Arc<dyn LocationScanner>> =
            self.locations.iter().map(|loc| loc.scanner()).collect();

        let archive_roots = self.archive_roots;

        // PendingRoute → TransferRoute 変換
        let routes: Vec<TransferRoute> = self
            .pending_routes
            .into_iter()
            .filter_map(|pr| {
                let src_loc = loc_map.get(&pr.src)?;
                let dest_loc = loc_map.get(&pr.dest)?;

                let cost = match estimate_route_cost(src_loc.kind(), dest_loc.kind()) {
                    Ok(c) => c,
                    Err(e) => {
                        tracing::warn!(src = ?src_loc.kind(), dest = ?dest_loc.kind(), error = %e, "skipping route: invalid cost");
                        return None;
                    }
                };

                let archive_root_for_dest = archive_roots.get(&pr.dest).cloned();

                let mut route = TransferRoute::new(
                    pr.src,
                    pr.dest.clone(),
                    src_loc.file_root().to_path_buf(),
                    dest_loc.file_root().to_path_buf(),
                    pr.backend,
                )
                .with_cost(cost.time_per_gb, cost.priority);

                if let Some(archive_root) = archive_root_for_dest {
                    // Archive-on-delete is only meaningful for Push direction:
                    // Pull routes delete from local filesystem and can't archive
                    // to a remote. Silently ignore for Pull.
                    if pr.direction == TransferDirection::Push {
                        route = route.with_archive_root(archive_root);
                    }
                }

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

        // RouteGraph構築（1回のみ。TopologyStore / TransferEngine で共有）
        let mut graph = RouteGraph::new();
        for r in &routes {
            graph.add_with_cost(
                r.src().clone(),
                r.dest().clone(),
                EdgeCost::new(r.time_per_gb(), r.priority())?,
            );
        }

        let topology = TopologyStore::new(
            self.topology_files.clone(),
            self.location_files.clone(),
            self.transfer_store.clone(),
            graph.clone(),
            location_ids,
        );

        let engine = TransferEngine::new(graph, routes, config.concurrency);

        let scanner = TopologyScanner::new(
            self.topology_files.clone(),
            self.location_files.clone(),
            scanners,
        );

        Ok(SdkImpl {
            scanner,
            topology,
            engine,
            topology_files: self.topology_files,
            location_files: self.location_files,
            transfer_store: self.transfer_store,
            locations: self.locations,
            config,
            scan_excludes: self.scan_excludes,
            progress: StdMutex::new(None),
        })
    }
}

/// LocationKind の組み合わせからルートコストを自動推定する。
///
/// optimal_tree（Dijkstra）がこのコストで最適経路を計算する。
/// 例: Local→Pod(1.0) + Pod→Cloud(2.0) = 3.0 < Local→Cloud(5.0)
/// → Pod経由チェーンが自動的に選択される。MCP層での条件分岐は不要。
fn estimate_route_cost(
    src: LocationKind,
    dest: LocationKind,
) -> Result<EdgeCost, crate::domain::error::DomainError> {
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
    /// Report progress via the stored callback (if set).
    fn report_progress(&self, msg: &str) {
        if let Ok(guard) = self.progress.lock() {
            if let Some(cb) = guard.as_ref() {
                cb(msg);
            }
        }
    }

    /// BFS順でTransfer実行 + DB永続化。
    ///
    /// Engine.execute_prepared()で純粋なroute I/Oを実行し、
    /// 結果をtransfer_storeに永続化 + unblock_dependentsでチェーン転送を解放する。
    async fn execute_bfs(
        &self,
        skip_locations: &std::collections::HashSet<crate::domain::location::LocationId>,
    ) -> Result<(usize, usize, Vec<SyncReportError>), SyncError> {
        let mut total_transferred = 0usize;
        let mut total_failed = 0usize;
        let mut all_errors: Vec<SyncReportError> = Vec::new();

        let targets = self.engine.all_targets_ordered();
        debug!(targets = ?targets, "execute_bfs: BFS target order");

        // Re-iterate BFS targets until no progress: chain transfers (e.g. pod→cloud)
        // become Queued only after their parent (local→pod) completes via
        // `unblock_dependents`. A single pass would miss them when the dependent
        // target was visited before the parent.
        let max_passes = targets.len().saturating_add(1).max(2);
        for pass in 0..max_passes {
            let mut progress = false;
            for target in &targets {
                if skip_locations.contains(target) {
                    if pass == 0 {
                        warn!(
                            target = %target,
                            "execute_bfs: skipping target (ensure failed)"
                        );
                    }
                    continue;
                }
                let queued = self.transfer_store.queued_transfers(target).await?;
                if queued.is_empty() {
                    debug!(target = %target, pass, "execute_bfs: no queued transfers, skip");
                    continue;
                }
                progress = true;
                info!(target = %target, pass, queued = queued.len(), "execute_bfs: processing target");
                self.process_target_batch(
                    target,
                    queued,
                    &mut total_transferred,
                    &mut total_failed,
                    &mut all_errors,
                )
                .await?;
            }
            if !progress {
                debug!(pass, "execute_bfs: no progress, exiting");
                break;
            }
        }

        Ok((total_transferred, total_failed, all_errors))
    }

    /// 1ターゲット分のqueued転送をprepare→sync→delete→permitの順で実行する。
    async fn process_target_batch(
        &self,
        target: &crate::domain::location::LocationId,
        queued: Vec<crate::domain::transfer::Transfer>,
        total_transferred: &mut usize,
        total_failed: &mut usize,
        all_errors: &mut Vec<SyncReportError>,
    ) -> Result<(), SyncError> {
        {
            info!(target = %target, queued = queued.len(), "execute_bfs: processing target");
            self.report_progress(&format!("target {target}: {} queued", queued.len()));

            // Prepare: resolve relative_path from topology_files
            let mut prepared = Vec::with_capacity(queued.len());
            let mut resolve_miss = 0usize;
            for transfer in queued {
                match self.topology_files.get_by_id(transfer.file_id()).await {
                    Ok(Some(file)) => {
                        trace!(
                            file_id = %transfer.file_id(),
                            path = %file.relative_path(),
                            src = %transfer.src(),
                            dest = %transfer.dest(),
                            "execute_bfs: prepared"
                        );
                        prepared.push(PreparedTransfer {
                            transfer,
                            relative_path: file.relative_path().to_string(),
                        });
                    }
                    Ok(None) => {
                        resolve_miss += 1;
                        error!(
                            file_id = %transfer.file_id(),
                            src = %transfer.src(),
                            dest = %transfer.dest(),
                            "execute_bfs: topology_file not found — transfer skipped"
                        );
                        *total_failed += 1;
                        all_errors.push(SyncReportError {
                            path: transfer.file_id().to_string(),
                            error: format!("file {} not found in store", transfer.file_id()),
                        });
                    }
                    Err(e) => {
                        resolve_miss += 1;
                        error!(
                            file_id = %transfer.file_id(),
                            src = %transfer.src(),
                            dest = %transfer.dest(),
                            err = %e,
                            "execute_bfs: topology_file lookup error — transfer skipped"
                        );
                        *total_failed += 1;
                        all_errors.push(SyncReportError {
                            path: transfer.file_id().to_string(),
                            error: e.to_string(),
                        });
                    }
                }
            }
            // Partition: sync / delete を分離して段階実行
            // sync完了→DB永続化→delete実行→DB永続化 の2段階。
            // delete がハング/失敗しても sync 結果がDBに反映される。
            let (sync_prepared, delete_prepared): (Vec<_>, Vec<_>) =
                prepared.into_iter().partition(|p| !p.transfer.is_delete());

            debug!(
                target = %target,
                sync = sync_prepared.len(),
                delete = delete_prepared.len(),
                resolve_miss = resolve_miss,
                "execute_bfs: preparation done"
            );

            // Phase A: Sync transfers → execute → DB persist
            if !sync_prepared.is_empty() {
                self.report_progress(&format!(
                    "target {target}: syncing {} files",
                    sync_prepared.len()
                ));
                info!(
                    target = %target,
                    count = sync_prepared.len(),
                    "execute_bfs: executing sync transfers"
                );
                let sync_outcomes = self.engine.execute_prepared(sync_prepared).await;
                self.report_progress(&format!(
                    "target {target}: sync done, persisting {}",
                    sync_outcomes.len()
                ));
                info!(
                    target = %target,
                    outcomes = sync_outcomes.len(),
                    "execute_bfs: sync execution done, persisting"
                );
                self.persist_outcomes(&sync_outcomes, total_transferred, total_failed, all_errors)
                    .await?;
            }

            // Phase B: Delete transfers → execute → DB persist
            if !delete_prepared.is_empty() {
                self.report_progress(&format!(
                    "target {target}: deleting {} files",
                    delete_prepared.len()
                ));
                info!(
                    target = %target,
                    count = delete_prepared.len(),
                    "execute_bfs: executing delete transfers"
                );
                let delete_outcomes = self.engine.execute_prepared(delete_prepared).await;
                self.report_progress(&format!(
                    "target {target}: delete done, persisting {}",
                    delete_outcomes.len()
                ));
                info!(
                    target = %target,
                    outcomes = delete_outcomes.len(),
                    "execute_bfs: delete execution done, persisting"
                );
                self.persist_outcomes(
                    &delete_outcomes,
                    total_transferred,
                    total_failed,
                    all_errors,
                )
                .await?;
            }

            info!(
                target = %target,
                transferred = *total_transferred,
                failed = *total_failed,
                "execute_bfs: target batch done"
            );
        }

        Ok(())
    }

    /// TransferOutcome群をDB永続化する共通ヘルパー。
    ///
    /// sync/delete の2段階実行で共通化するために抽出。
    async fn persist_outcomes(
        &self,
        outcomes: &[TransferOutcome],
        total_transferred: &mut usize,
        total_failed: &mut usize,
        all_errors: &mut Vec<SyncReportError>,
    ) -> Result<(), SyncError> {
        for outcome in outcomes {
            let is_completed = outcome.transfer.state() == TransferState::Completed;
            self.transfer_store
                .update_transfer(&outcome.transfer)
                .await?;

            if is_completed {
                self.transfer_store
                    .unblock_dependents(outcome.transfer.id())
                    .await?;

                if outcome.transfer.is_delete() {
                    // Delete完了 = dest側にファイルが存在しない → LocationFile削除
                    let deleted = self
                        .location_files
                        .delete(outcome.transfer.file_id(), outcome.transfer.dest())
                        .await?;
                    trace!(
                        file_id = %outcome.transfer.file_id(),
                        dest = %outcome.transfer.dest(),
                        deleted = deleted,
                        "execute_bfs: delete transfer → LocationFile removed"
                    );
                    // 全LF削除済みならTFを物理削除（list_deleted肥大化防止）
                    let remaining = self
                        .location_files
                        .list_by_file(outcome.transfer.file_id())
                        .await?;
                    if remaining.is_empty() {
                        let purged = self
                            .topology_files
                            .hard_delete(outcome.transfer.file_id())
                            .await?;
                        if purged {
                            debug!(
                                file_id = %outcome.transfer.file_id(),
                                "execute_bfs: all LFs gone → TopologyFile hard-deleted"
                            );
                        }
                    }
                } else {
                    // Sync完了 = dest側にファイルが存在 → LocationFile作成
                    if let Ok(Some(tf)) = self
                        .topology_files
                        .get_by_id(outcome.transfer.file_id())
                        .await
                    {
                        let src_lf = self
                            .location_files
                            .get(outcome.transfer.file_id(), outcome.transfer.src())
                            .await?;
                        if let Some(src_lf) = src_lf {
                            trace!(
                                file_id = %outcome.transfer.file_id(),
                                src = %outcome.transfer.src(),
                                dest = %outcome.transfer.dest(),
                                path = %outcome.relative_path,
                                "persist_outcomes: creating dest LocationFile from src"
                            );
                            let dest_lf = tf
                                .materialize(
                                    outcome.transfer.dest().clone(),
                                    outcome.relative_path.clone(),
                                    src_lf.fingerprint().clone(),
                                    src_lf.embedded_id().map(|s| s.to_string()),
                                )
                                .map_err(SyncError::Domain)?;
                            self.location_files.upsert(&dest_lf).await?;
                        } else {
                            warn!(
                                file_id = %outcome.transfer.file_id(),
                                src = %outcome.transfer.src(),
                                "persist_outcomes: src LocationFile not found, cannot create dest LF"
                            );
                        }
                    } else {
                        warn!(
                            file_id = %outcome.transfer.file_id(),
                            "persist_outcomes: TopologyFile not found for completed transfer"
                        );
                    }
                }

                *total_transferred += 1;
                info!(
                    id = %outcome.transfer.id(),
                    src = %outcome.transfer.src(),
                    dest = %outcome.transfer.dest(),
                    path = %outcome.relative_path,
                    kind = ?outcome.transfer.kind(),
                    "execute_bfs: transfer completed"
                );
            } else {
                *total_failed += 1;
                let err_msg = outcome
                    .transfer
                    .error()
                    .map(|e| e.to_string())
                    .unwrap_or_else(|| "unknown error".to_string());
                error!(
                    id = %outcome.transfer.id(),
                    src = %outcome.transfer.src(),
                    dest = %outcome.transfer.dest(),
                    path = %outcome.relative_path,
                    err = %err_msg,
                    "execute_bfs: transfer FAILED"
                );
                all_errors.push(SyncReportError {
                    path: outcome.relative_path.clone(),
                    error: err_msg,
                });
            }
        }
        Ok(())
    }
}

#[async_trait]
impl SyncStoreSdk for SdkImpl {
    // =========================================================================
    // UseCase — 同期操作
    // =========================================================================

    async fn sync(&self) -> Result<SyncReport, SyncError> {
        info!("sdk_impl::sync: pipeline start");
        self.report_progress("ensure: checking locations");

        // Phase 0a: Ensure — 全拠点の到達確認 + 外部ツール確保
        // 失敗したLocationはスキャン/転送対象から除外し、syncは続行する。
        let location_ids: Vec<String> = self.locations.iter().map(|l| l.id().to_string()).collect();
        info!(
            location_count = self.locations.len(),
            locations = %location_ids.join(", "),
            "sdk_impl::sync: ensure start"
        );
        let mut failed_locations: std::collections::HashSet<LocationId> =
            std::collections::HashSet::new();
        for loc in &self.locations {
            info!(
                location = %loc.id(),
                kind = ?loc.kind(),
                "sdk_impl::sync: ensure checking"
            );
            match loc.ensure().await {
                Ok(()) => {
                    info!(location = %loc.id(), "sdk_impl::sync: ensure ok");
                }
                Err(e) => {
                    error!(
                        location = %loc.id(),
                        kind = ?loc.kind(),
                        error = %e,
                        "sdk_impl::sync: ensure FAILED — this location will be excluded from sync"
                    );
                    failed_locations.insert(loc.id().clone());
                }
            }
        }
        if failed_locations.is_empty() {
            info!("sdk_impl::sync: ensure done — all locations reachable");
        } else {
            let excluded: Vec<String> = failed_locations.iter().map(|l| l.to_string()).collect();
            warn!(
                excluded = %excluded.join(", "),
                "sdk_impl::sync: ensure done — {} location(s) excluded due to ensure failure",
                failed_locations.len()
            );
        }

        // Phase 0b: InFlight孤児の終端化（プロセスクラッシュ復帰）
        let cancelled = self.transfer_store.cancel_orphaned_inflight().await?;
        if cancelled > 0 {
            info!(
                cancelled_count = cancelled,
                "sdk_impl::sync: cancelled orphaned InFlight transfers"
            );
        }

        // Phase 1: Scan → TopologyDelta[]
        self.report_progress("scan: scanning locations");
        info!("sdk_impl::sync: phase1 scan start");
        let progress_cb = self.progress.lock().ok().and_then(|g| g.clone());
        let scan_result = self
            .scanner
            .scan_all(&self.scan_excludes, &failed_locations, progress_cb.as_ref())
            .await?;
        info!(
            scanned = scan_result.scanned,
            deltas = scan_result.deltas.len(),
            scan_errors = scan_result.scan_errors.len(),
            "sdk_impl::sync: phase1 scan done"
        );
        // delta詳細をtrace出力
        for delta in &scan_result.deltas {
            trace!(delta = ?delta, "sdk_impl::sync: delta");
        }

        // Phase 2: Plan — Apply → Distribute → Route → Transfer作成
        self.report_progress(&format!(
            "plan: {} files scanned, {} deltas",
            scan_result.scanned,
            scan_result.deltas.len()
        ));
        info!(
            delta_count = scan_result.deltas.len(),
            "sdk_impl::sync: phase2 plan start"
        );
        let plan_result = self.topology.sync(&scan_result.deltas).await?;
        info!(
            transfers_created = plan_result.transfers_created,
            conflicts = plan_result.conflicts.len(),
            "sdk_impl::sync: phase2 plan done"
        );

        // Phase 3: Execute — BFS順でTransfer実行 + DB永続化
        // Propagate progress callback to all route backends for chunk-level reporting.
        if let Ok(guard) = self.progress.lock() {
            self.engine.set_progress_callback(guard.clone());
        }
        self.report_progress(&format!(
            "execute: {} transfers queued",
            plan_result.transfers_created
        ));
        info!("sdk_impl::sync: phase3 execute start");
        let (transferred, failed, errors) = self.execute_bfs(&failed_locations).await?;
        // Clear backend callbacks after execution.
        self.engine.set_progress_callback(None);
        info!(
            transferred = transferred,
            failed = failed,
            error_count = errors.len(),
            "sdk_impl::sync: phase3 execute done"
        );

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
                .map(super::sdk::SyncReportConflict::from)
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
            info!(
                cancelled_count = cancelled,
                "sync_route: cancelled orphaned InFlight transfers"
            );
        }

        // Phase 1: Plan — sync_routeはdelta生成なし、Distribute + Route のみ
        self.report_progress(&format!("plan: route {src} → {dest}"));
        let plan_result = self.topology.sync_route(src, dest).await?;

        // Phase 2: Execute — dest宛のQueued Transferをsrcでフィルタして実行
        // Propagate progress callback to all route backends.
        if let Ok(guard) = self.progress.lock() {
            self.engine.set_progress_callback(guard.clone());
        }
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

        self.report_progress(&format!(
            "execute: {} transfers ({src} → {dest})",
            prepared.len()
        ));
        let outcomes = self.engine.execute_prepared(prepared).await;
        // Clear backend callbacks after execution.
        self.engine.set_progress_callback(None);
        let mut total_transferred = 0usize;

        self.persist_outcomes(
            &outcomes,
            &mut total_transferred,
            &mut total_failed,
            &mut all_errors,
        )
        .await?;

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
                .map(super::sdk::SyncReportConflict::from)
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

    async fn restore(&self, path: &str, revision: &str) -> Result<(), SyncError> {
        info!(path = %path, revision = %revision, "sdk_impl::restore: start");

        // 1. archive_root を持つルート（cloud宛）を engine から1件取得
        let route = self.engine.archive_route().ok_or_else(|| -> SyncError {
            crate::infra::error::InfraError::Transfer {
                reason: "restore: no route with archive_root configured".into(),
            }
            .into()
        })?;

        // 2. 物理復元: cloud archive → cloud original
        route.restore_from_archive(path, revision).await?;
        info!(path = %path, "sdk_impl::restore: physical restore done");

        // 3. 削除済みTopologyFileを取得して unmark
        //    delete transfers 完走後は TF が hard-delete されている (commit c8213ce)
        //    ため見つからないケースがある。物理 restore は既に完了しているので
        //    次回 full sync で cloud から再発見させれば整合が取れる。
        let deleted_tfs = self.topology_files.list_deleted().await?;
        match deleted_tfs.into_iter().find(|t| t.relative_path() == path) {
            Some(mut tf) => {
                tf.unmark_deleted();
                self.topology_files.upsert(&tf).await?;
                info!(path = %path, file_id = %tf.id(), "sdk_impl::restore: TopologyFile unmarked");
            }
            None => {
                warn!(
                    path = %path,
                    "sdk_impl::restore: TopologyFile not in deleted list (likely hard-deleted after delete transfers). Physical restore succeeded — next full sync will re-register."
                );
            }
        }

        Ok(())
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
        let retry_policy = self.config.retry_policy();
        let failed = self.transfer_store.failed_transfers().await?;
        Ok(failed
            .iter()
            .filter(|t| {
                let state = PresenceState::from_transfer(t, &retry_policy);
                state == PresenceState::Failed
            })
            .map(ErrorEntry::from_transfer)
            .collect())
    }

    async fn pending(&self, dest: &LocationId) -> Result<Vec<PendingEntry>, SyncError> {
        let retry_policy = self.config.retry_policy();

        // Queued/Blocked/InFlight transfers for the target dest
        let all_pending = self.transfer_store.all_pending_transfers().await?;
        let mut entries: Vec<PendingEntry> = all_pending
            .iter()
            .filter(|t| t.dest() == dest)
            .map(PendingEntry::from_transfer)
            .collect();

        // Failed but retryable transfers also count as pending
        let failed = self.transfer_store.failed_transfers().await?;
        for t in &failed {
            if t.dest() == dest {
                let state = PresenceState::from_transfer(t, &retry_policy);
                if state == PresenceState::Pending {
                    entries.push(PendingEntry::from_transfer(t));
                }
            }
        }

        Ok(entries)
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

    fn set_progress_callback(&self, callback: Option<ProgressFn>) {
        if let Ok(mut guard) = self.progress.lock() {
            *guard = callback;
        }
    }
}
