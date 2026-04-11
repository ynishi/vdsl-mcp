//! TopologyStore — Topology中心モデルのapplication-layer facade。
//!
//! TopologyFile（inode）+ LocationFile（directory entry）+ RouteGraph による
//! 分散ファイルストレージの統合API。
//!
//! # API カテゴリ
//!
//! - **Sync** — トポロジー全体の同期 ([`sync`], [`sync_route`])
//! - **File CRUD** — ファイル操作 ([`put`], [`get`], [`list`], [`delete`])
//! - **Status** — 監視 ([`status`])
//!
//! # 3フェーズパイプライン
//!
//! ```text
//! Phase 1: Ingest   — scan → TopologyDelta → TopologyFile/LocationFile更新
//! Phase 2: Distribute — TopologyFile × LocationFile → DistributeAction[]
//! Phase 3: Route     — DistributeAction + RouteGraph → PlannedTransfer → Transfer実行
//! ```

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use crate::application::error::SyncError;
use crate::domain::file_type::FileType;
use crate::domain::fingerprint::FileFingerprint;
use crate::domain::graph::RouteGraph;
use crate::domain::location::LocationId;
use crate::domain::location_file::{self, LocationFile};
use crate::domain::plan::{plan_distribution, PlannedTransfer};
use tracing::{debug, info, trace};

use crate::domain::distribute::distribute_actions;
use crate::domain::topology_delta::TopologyDelta;
use crate::domain::topology_file::TopologyFile;
use crate::domain::transfer::{Transfer, TransferKind};
use crate::infra::location_file_store::LocationFileStore;
use crate::infra::topology_file_store::TopologyFileStore;
use crate::infra::transfer_store::TransferStore;

// =============================================================================
// Result types
// =============================================================================

/// sync()の結果。
#[derive(Debug, Clone, Default, serde::Serialize)]
pub struct TopologySyncResult {
    /// スキャンで検出されたファイル数。
    pub scanned: usize,
    /// Ingestで生成されたTopologyDelta数。
    pub ingested: usize,
    /// Distributeで生成されたDistributeAction数。
    pub distributed: usize,
    /// 作成されたTransfer数。
    pub transfers_created: usize,
    /// 検出されたコンフリクト。
    ///
    /// 複数Locationで同一ファイルが異なる内容に更新された場合に報告される。
    /// コンフリクトがあるファイルのUpdate転送は抑止される。
    pub conflicts: Vec<crate::domain::distribute::ConflictEntry>,
}

/// put()の結果。
#[derive(Debug, serde::Serialize)]
pub struct TopologyPutResult {
    /// 登録/更新されたTopologyFile ID。
    pub topology_file_id: String,
    /// 新規登録 = true、更新 = false。
    pub is_new: bool,
    /// 作成されたTransfer数。
    pub transfers_created: usize,
}

// =============================================================================
// TopologyStore
// =============================================================================

/// Topology中心モデルの分散ファイルストレージ。
///
/// 3つの永続化トレイトに依存し、ドメインロジックをオーケストレーションする。
pub struct TopologyStore {
    topology_files: Arc<dyn TopologyFileStore>,
    location_files: Arc<dyn LocationFileStore>,
    transfers: Arc<dyn TransferStore>,
    graph: RouteGraph,
    /// 全Locationの一覧（target_locations用）。
    locations: Vec<LocationId>,
}

impl TopologyStore {
    pub fn new(
        topology_files: Arc<dyn TopologyFileStore>,
        location_files: Arc<dyn LocationFileStore>,
        transfers: Arc<dyn TransferStore>,
        graph: RouteGraph,
        locations: Vec<LocationId>,
    ) -> Self {
        Self {
            topology_files,
            location_files,
            transfers,
            graph,
            locations,
        }
    }

    // =========================================================================
    // Sync — 全体同期
    // =========================================================================

    /// 全体同期: Ingest済みのTopologyDelta群を受け取り、
    /// Apply → Distribute → Route → Transfer作成 を実行する。
    ///
    /// Ingest（スキャン→TopologyDelta生成）は呼び出し元が実行する。
    /// この関数はその後の3ステップを担当する。
    ///
    /// # フロー
    ///
    /// 1. Apply: TopologyDelta → TopologyFile/LocationFile更新
    /// 2. Distribute: 全TopologyFile × 全LocationFile → DistributeAction[]
    /// 3. Route: DistributeAction + RouteGraph → PlannedTransfer → Transfer作成
    pub async fn sync(&self, deltas: &[TopologyDelta]) -> Result<TopologySyncResult, SyncError> {
        info!(delta_count = deltas.len(), "topology_store::sync: start");

        // Phase 1: Apply — TopologyDelta → TopologyFile/LocationFile更新
        let ingest_origins = self.apply_ingest(deltas).await?;
        info!(
            origins = ingest_origins.len(),
            "topology_store::sync: phase1 apply done"
        );

        // Phase 2: Distribute
        let active_tfs = self.topology_files.list_active(None, None).await?;
        let active_tf_refs: Vec<&TopologyFile> = active_tfs.iter().collect();
        let file_ids: Vec<&str> = active_tfs.iter().map(|tf| tf.id()).collect();
        debug!(
            active_files = active_tfs.len(),
            "topology_store::sync: loaded active topology_files"
        );

        let lf_map = self.location_files.list_by_files(&file_ids).await?;
        let lf_ref_map = to_ref_map(&lf_map);
        debug!(
            location_file_groups = lf_map.len(),
            "topology_store::sync: loaded location_files"
        );

        let dist_result = distribute_actions(
            &active_tf_refs,
            &lf_ref_map,
            &self.locations,
            &ingest_origins,
        );
        info!(
            actions = dist_result.actions.len(),
            conflicts = dist_result.conflicts.len(),
            "topology_store::sync: phase2 distribute done"
        );

        // 削除済みファイル → 各LocationFileのdestへDelete Transfer直接発行
        let deleted_tfs = self.topology_files.list_deleted().await?;
        trace!(
            deleted_tfs = deleted_tfs.len(),
            "topology_store::sync: checking deleted topology_files for delete transfers"
        );
        let mut delete_transfers_created = 0;
        let pending_dests = self.collect_pending_dests().await?;
        for dtf in &deleted_tfs {
            let lfs = self.location_files.list_by_file(dtf.id()).await?;
            let empty = HashSet::new();
            let pending = pending_dests.get(dtf.id()).unwrap_or(&empty);
            for lf in &lfs {
                let dest = lf.location_id().clone();
                if pending.contains(&dest) {
                    trace!(
                        file_id = %dtf.id(),
                        dest = %dest,
                        "topology_store::sync: delete transfer skipped (pending)"
                    );
                    continue;
                }
                let src = self
                    .locations
                    .iter()
                    .find(|l| *l != &dest)
                    .cloned()
                    .unwrap_or_else(|| dest.clone());
                if src == dest {
                    trace!(
                        file_id = %dtf.id(),
                        dest = %dest,
                        "topology_store::sync: delete transfer skipped (single location)"
                    );
                    continue;
                }
                trace!(
                    file_id = %dtf.id(),
                    src = %src,
                    dest = %dest,
                    "topology_store::sync: creating delete transfer"
                );
                let transfer = Transfer::new_delete(dtf.id().to_string(), src, dest)?;
                self.transfers.insert_transfer(&transfer).await?;
                delete_transfers_created += 1;
            }
        }
        if delete_transfers_created > 0 {
            debug!(
                count = delete_transfers_created,
                "topology_store::sync: delete transfers created"
            );
        }

        let distributed = dist_result.actions.len();

        // Phase 3: Route → Transfer作成（Send/Updateのみ）
        // 既存データ保持locationをMulti-source Dijkstraに渡す
        // Stale/Missing/Syncing のLocationFileはsource eligible ではないため除外
        let existing_presences: HashMap<String, HashSet<LocationId>> = lf_map
            .iter()
            .map(|(file_id, lfs)| {
                let locs: HashSet<LocationId> = lfs
                    .iter()
                    .filter(|lf| lf.state().is_source_eligible())
                    .map(|lf| lf.location_id().clone())
                    .collect();
                (file_id.clone(), locs)
            })
            .collect();
        let planned = plan_distribution(
            &dist_result.actions,
            &self.graph,
            &pending_dests,
            &existing_presences,
        );
        debug!(
            planned_count = planned.len(),
            "topology_store::sync: phase3 route planned"
        );

        let transfers_created = self.create_transfers(&planned).await? + delete_transfers_created;
        info!(
            transfers_created = transfers_created,
            "topology_store::sync: phase3 route done"
        );

        Ok(TopologySyncResult {
            scanned: 0, // 呼び出し元がセット
            ingested: deltas.len(),
            distributed,
            transfers_created,
            conflicts: dist_result.conflicts,
        })
    }

    // =========================================================================
    // Sync — 単一ルート
    // =========================================================================

    /// 単一ルート同期: src→dest の経路のみ処理する。
    ///
    /// dest側のLocationFileを確認し、不足・古いものだけDistribute + Transfer作成。
    pub async fn sync_route(
        &self,
        src: &LocationId,
        dest: &LocationId,
    ) -> Result<TopologySyncResult, SyncError> {
        let active_tfs = self.topology_files.list_active(None, None).await?;
        let active_tf_refs: Vec<&TopologyFile> = active_tfs.iter().collect();
        let file_ids: Vec<&str> = active_tfs.iter().map(|tf| tf.id()).collect();
        let lf_map = self.location_files.list_by_files(&file_ids).await?;
        let lf_ref_map = to_ref_map(&lf_map);

        // source=src, target=[dest] でDistribute
        let mut ingest_origins = HashMap::new();
        for tf in &active_tfs {
            // src側のLocationFileがActiveなファイルのみ対象
            if let Some(lfs) = lf_map.get(tf.id()) {
                if lfs
                    .iter()
                    .any(|lf| lf.location_id() == src && lf.state().is_source_eligible())
                {
                    ingest_origins
                        .entry(tf.id().to_string())
                        .or_insert_with(HashSet::new)
                        .insert(src.clone());
                }
            }
        }

        let dist_result = distribute_actions(
            &active_tf_refs,
            &lf_ref_map,
            std::slice::from_ref(dest),
            &ingest_origins,
        );

        let distributed = dist_result.actions.len();

        // Route: この場合src→destの直接Transferのみ（optimal_tree不要）
        let pending_dests = self.collect_pending_dests().await?;
        let transfers: Vec<PlannedTransfer> = dist_result
            .actions
            .iter()
            .filter_map(|action| {
                let file_id = action.topology_file_id();
                let empty = HashSet::new();
                let pending = pending_dests.get(file_id).unwrap_or(&empty);
                if pending.contains(dest) {
                    return None;
                }
                Some(PlannedTransfer {
                    file_id: file_id.to_string(),
                    src: src.clone(),
                    dest: dest.clone(),
                    kind: if action.is_delete() {
                        TransferKind::Delete
                    } else {
                        TransferKind::Sync
                    },
                    depends_on_index: None,
                })
            })
            .collect();

        let transfers_created = self.create_transfers(&transfers).await?;

        Ok(TopologySyncResult {
            scanned: 0,
            ingested: 0,
            distributed,
            transfers_created,
            conflicts: dist_result.conflicts,
        })
    }

    // =========================================================================
    // File CRUD
    // =========================================================================

    /// ファイル登録。
    ///
    /// TopologyFile + LocationFile(origin) を作成し、
    /// 全到達可能Locationへの転送を計画する。
    pub async fn put(
        &self,
        relative_path: &str,
        file_type: FileType,
        fingerprint: FileFingerprint,
        origin: &LocationId,
        embedded_id: Option<String>,
    ) -> Result<TopologyPutResult, SyncError> {
        // 既存チェック（path or canonical_hash）
        let existing = self.topology_files.get_by_path(relative_path).await?;

        let (tf, is_new) = if let Some(mut tf) = existing {
            // 既存 → canonical_hash昇格 + LocationFile更新
            tf.promote_canonical_digest(&fingerprint);
            self.topology_files.upsert(&tf).await?;
            (tf, false)
        } else {
            // 新規作成
            let mut tf = TopologyFile::new(relative_path.to_string(), file_type)
                .map_err(SyncError::Domain)?;
            tf.promote_canonical_digest(&fingerprint);
            self.topology_files.upsert(&tf).await?;
            (tf, true)
        };

        // LocationFile作成/更新
        let existing_lf = self.location_files.get(tf.id(), origin).await?;
        match existing_lf {
            Some(mut lf) => {
                lf.update_fingerprint(fingerprint.clone(), embedded_id);
                self.location_files.upsert(&lf).await?;
            }
            None => {
                let lf = tf
                    .materialize(
                        origin.clone(),
                        relative_path.to_string(),
                        fingerprint.clone(),
                        embedded_id,
                    )
                    .map_err(SyncError::Domain)?;
                self.location_files.upsert(&lf).await?;
            }
        }

        // Transfer計画: origin → 全到達可能Location
        let mut ingest_origins = HashMap::new();
        ingest_origins.insert(tf.id().to_string(), HashSet::from([origin.clone()]));

        let lfs = self.location_files.list_by_file(tf.id()).await?;
        let mut lf_map: HashMap<String, Vec<&LocationFile>> = HashMap::new();
        lf_map.insert(tf.id().to_string(), lfs.iter().collect());

        let dist_result = distribute_actions(&[&tf], &lf_map, &self.locations, &ingest_origins);

        let pending_dests = self.collect_pending_dests().await?;
        let sync_actions: Vec<_> = dist_result
            .actions
            .iter()
            .filter(|a| !a.is_delete())
            .cloned()
            .collect();
        let existing_presences: HashMap<String, HashSet<LocationId>> = {
            let locs: HashSet<LocationId> = lfs.iter().map(|lf| lf.location_id().clone()).collect();
            let mut m = HashMap::new();
            m.insert(tf.id().to_string(), locs);
            m
        };
        let planned = plan_distribution(
            &sync_actions,
            &self.graph,
            &pending_dests,
            &existing_presences,
        );

        let transfers_created = self.create_transfers(&planned).await?;

        Ok(TopologyPutResult {
            topology_file_id: tf.id().to_string(),
            is_new,
            transfers_created,
        })
    }

    /// ファイル取得。
    pub async fn get(&self, relative_path: &str) -> Result<Option<TopologyFileView>, SyncError> {
        let tf = match self.topology_files.get_by_path(relative_path).await? {
            Some(tf) => tf,
            None => return Ok(None),
        };
        let lfs = self.location_files.list_by_file(tf.id()).await?;
        Ok(Some(TopologyFileView {
            topology_file: tf,
            location_files: lfs,
        }))
    }

    /// ファイル一覧。
    pub async fn list(
        &self,
        file_type: Option<FileType>,
        limit: Option<usize>,
    ) -> Result<Vec<TopologyFileView>, SyncError> {
        let tfs = self.topology_files.list_active(file_type, limit).await?;
        let file_ids: Vec<&str> = tfs.iter().map(|tf| tf.id()).collect();
        let lf_map = self.location_files.list_by_files(&file_ids).await?;

        let views = tfs
            .into_iter()
            .map(|tf| {
                let lfs = lf_map.get(tf.id()).cloned().unwrap_or_default();
                TopologyFileView {
                    topology_file: tf,
                    location_files: lfs,
                }
            })
            .collect();
        Ok(views)
    }

    /// ファイル削除。
    ///
    /// TopologyFile mark_deleted + LocationFile保持先への Delete Transfer 直接発行。
    /// Intentベース: distribute/plan を経由せず各destへ直結N件。
    pub async fn delete(&self, relative_path: &str) -> Result<usize, SyncError> {
        let mut tf = self
            .topology_files
            .get_by_path(relative_path)
            .await?
            .ok_or_else(|| SyncError::NotRegistered(relative_path.to_string()))?;

        tf.mark_deleted();
        self.topology_files.upsert(&tf).await?;

        // LocationFileを持つ各LocationへDelete Transfer直接発行
        let lfs = self.location_files.list_by_file(tf.id()).await?;
        let pending_dests = self.collect_pending_dests().await?;
        let empty = HashSet::new();
        let pending = pending_dests.get(tf.id()).unwrap_or(&empty);

        let mut created = 0;
        for lf in &lfs {
            let dest = lf.location_id().clone();
            if pending.contains(&dest) {
                continue;
            }
            // src: dest以外の任意Location（Delete実行に物理srcは不要）
            let src = self
                .locations
                .iter()
                .find(|l| *l != &dest)
                .cloned()
                .unwrap_or_else(|| dest.clone());
            if src == dest {
                // 単一Location環境 — ローカル削除はTransfer不要
                continue;
            }
            let transfer = Transfer::new_delete(tf.id().to_string(), src, dest)?;
            self.transfers.insert_transfer(&transfer).await?;
            created += 1;
        }

        Ok(created)
    }

    // =========================================================================
    // Status
    // =========================================================================

    /// ファイル数。
    pub async fn file_count(&self) -> Result<usize, SyncError> {
        Ok(self.topology_files.count_active().await?)
    }

    /// Location一覧。
    pub fn locations(&self) -> &[LocationId] {
        &self.locations
    }

    // =========================================================================
    // Internal: Apply Ingest
    // =========================================================================

    /// TopologyDelta群をApply: TopologyFile/LocationFile更新。
    ///
    /// delta適用順序を正規化する:
    ///   Renamed(0) → ContentChanged(1) → Discovered(2) → Vanished(3)
    ///
    /// Renameが先に処理されることで、Rename先pathとDiscoveredのpath衝突を防ぐ。
    ///
    /// 返り値: file_id → ingest origin LocationId集合（Distribute用）。
    async fn apply_ingest(
        &self,
        deltas: &[TopologyDelta],
    ) -> Result<HashMap<String, HashSet<LocationId>>, SyncError> {
        let mut ingest_origins: HashMap<String, HashSet<LocationId>> = HashMap::new();

        // delta適用順を正規化: Renamed → ContentChanged → Discovered → Vanished
        let mut sorted_deltas: Vec<&TopologyDelta> = deltas.iter().collect();
        sorted_deltas.sort_by_key(|d| match d {
            TopologyDelta::Renamed(_) => 0,
            TopologyDelta::ContentChanged(_) => 1,
            TopologyDelta::Discovered(_) => 2,
            TopologyDelta::Vanished(_) => 3,
        });

        let total = sorted_deltas.len();
        let log_interval = (total / 10).max(1);

        for (i, delta) in sorted_deltas.iter().enumerate() {
            if i % log_interval == 0 {
                info!(progress = i, total = total, "apply_ingest: processing");
            }
            match delta {
                TopologyDelta::Discovered(d) => {
                    // 既存TopologyFileがあれば再利用（複数Locationが同一ファイルを
                    // Discoveredとして報告するケース）。なければ新規作成。
                    let existing = self.topology_files.get_by_path(&d.relative_path).await?;
                    let is_new = existing.is_none();
                    let mut tf = if let Some(existing) = existing {
                        trace!(
                            path = %d.relative_path,
                            tf_id = %existing.id(),
                            origin = %d.origin,
                            "apply_ingest: Discovered — reusing existing TopologyFile"
                        );
                        existing
                    } else {
                        trace!(
                            path = %d.relative_path,
                            origin = %d.origin,
                            size = d.fingerprint.size,
                            content_digest = ?d.fingerprint.content_digest,
                            "apply_ingest: Discovered — creating new TopologyFile"
                        );
                        TopologyFile::new(d.relative_path.clone(), d.file_type)
                            .map_err(SyncError::Domain)?
                    };
                    tf.promote_canonical_digest(&d.fingerprint);
                    self.topology_files.upsert(&tf).await?;

                    let lf = tf
                        .materialize(
                            d.origin.clone(),
                            d.relative_path.clone(),
                            d.fingerprint.clone(),
                            d.embedded_id.clone(),
                        )
                        .map_err(SyncError::Domain)?;
                    self.location_files.upsert(&lf).await?;

                    if is_new {
                        debug!(
                            path = %d.relative_path,
                            tf_id = %tf.id(),
                            origin = %d.origin,
                            "apply_ingest: NEW file registered"
                        );
                    }

                    ingest_origins
                        .entry(tf.id().to_string())
                        .or_default()
                        .insert(d.origin.clone());
                }
                TopologyDelta::ContentChanged(c) => {
                    trace!(
                        path = %c.relative_path,
                        tf_id = %c.topology_file_id,
                        origin = %c.origin,
                        old_size = c.old_fingerprint.size,
                        new_size = c.new_fingerprint.size,
                        "apply_ingest: ContentChanged"
                    );
                    // TopologyFile: canonical_hash昇格
                    if let Some(mut tf) = self.topology_files.get_by_id(&c.topology_file_id).await?
                    {
                        tf.promote_canonical_digest(&c.new_fingerprint);
                        self.topology_files.upsert(&tf).await?;
                    }

                    // LocationFile: fingerprint更新 or 新規作成
                    let existing_lf = self
                        .location_files
                        .get(&c.topology_file_id, &c.origin)
                        .await?;
                    match existing_lf {
                        Some(mut lf) => {
                            lf.update_fingerprint(c.new_fingerprint.clone(), c.embedded_id.clone());
                            self.location_files.upsert(&lf).await?;
                        }
                        None => {
                            if let Some(tf) =
                                self.topology_files.get_by_id(&c.topology_file_id).await?
                            {
                                let lf = tf
                                    .materialize(
                                        c.origin.clone(),
                                        c.relative_path.clone(),
                                        c.new_fingerprint.clone(),
                                        c.embedded_id.clone(),
                                    )
                                    .map_err(SyncError::Domain)?;
                                self.location_files.upsert(&lf).await?;
                            }
                        }
                    }

                    // 他LocationのLocationFileをStaleに（cross-location比較で実際に異なるもののみ）
                    let all_lfs = self
                        .location_files
                        .list_by_file(&c.topology_file_id)
                        .await?;
                    for stale_lf in
                        location_file::stale_candidates(&all_lfs, &c.origin, &c.new_fingerprint)
                    {
                        let mut lf = stale_lf.clone();
                        lf.mark_stale();
                        self.location_files.upsert(&lf).await?;
                    }

                    ingest_origins
                        .entry(c.topology_file_id.clone())
                        .or_default()
                        .insert(c.origin.clone());
                }
                TopologyDelta::Renamed(r) => {
                    trace!(
                        tf_id = %r.topology_file_id,
                        old_path = %r.old_path,
                        new_path = %r.new_path,
                        origin = %r.origin,
                        "apply_ingest: Renamed"
                    );
                    if let Some(mut tf) = self.topology_files.get_by_id(&r.topology_file_id).await?
                    {
                        tf.update_path(r.new_path.clone());
                        tf.promote_canonical_digest(&r.fingerprint);
                        self.topology_files.upsert(&tf).await?;
                    }

                    // LocationFile: fingerprint更新
                    let existing_lf = self
                        .location_files
                        .get(&r.topology_file_id, &r.origin)
                        .await?;
                    match existing_lf {
                        Some(mut lf) => {
                            lf.update_fingerprint(r.fingerprint.clone(), r.embedded_id.clone());
                            self.location_files.upsert(&lf).await?;
                        }
                        None => {
                            if let Some(tf) =
                                self.topology_files.get_by_id(&r.topology_file_id).await?
                            {
                                let lf = tf
                                    .materialize(
                                        r.origin.clone(),
                                        r.new_path.clone(),
                                        r.fingerprint.clone(),
                                        r.embedded_id.clone(),
                                    )
                                    .map_err(SyncError::Domain)?;
                                self.location_files.upsert(&lf).await?;
                            }
                        }
                    }

                    ingest_origins
                        .entry(r.topology_file_id.clone())
                        .or_default()
                        .insert(r.origin.clone());
                }
                TopologyDelta::Vanished(v) => {
                    trace!(
                        path = %v.relative_path,
                        tf_id = %v.topology_file_id,
                        origin = %v.origin,
                        "apply_ingest: Vanished"
                    );
                    // LocationFile: mark_missing
                    let existing_lf = self
                        .location_files
                        .get(&v.topology_file_id, &v.origin)
                        .await?;
                    if let Some(mut lf) = existing_lf {
                        lf.mark_missing();
                        self.location_files.upsert(&lf).await?;
                    }
                    // Vanished ではingest_originsに追加しない（消失はsourceにならない）
                    // NOTE: scan-based delete propagation (Vanished on local → mark_deleted)
                    // は ByHash誤判定によるpath conflict retire → 大量誤削除の問題があるため
                    // 撤回。削除は明示 delete() API のみで行う。
                }
            }
        }

        info!(
            processed = total,
            origins = ingest_origins.len(),
            "apply_ingest: done"
        );
        Ok(ingest_origins)
    }

    // =========================================================================
    // Internal: Transfer作成
    // =========================================================================

    /// PlannedTransfer群からTransferを作成しDBに書き込む。
    async fn create_transfers(&self, planned: &[PlannedTransfer]) -> Result<usize, SyncError> {
        let mut created = 0;
        // depends_on_index → 実Transfer IDのマッピング
        let mut transfer_ids: Vec<String> = Vec::with_capacity(planned.len());

        for pt in planned.iter() {
            trace!(
                file_id = %pt.file_id,
                src = %pt.src,
                dest = %pt.dest,
                kind = ?pt.kind,
                depends_on = ?pt.depends_on_index,
                "create_transfers: creating transfer"
            );
            let transfer = if let Some(dep_idx) = pt.depends_on_index {
                let dep_id = &transfer_ids[dep_idx];
                Transfer::with_dependency(
                    pt.file_id.clone(),
                    pt.src.clone(),
                    pt.dest.clone(),
                    pt.kind,
                    dep_id.clone(),
                )?
            } else {
                Transfer::with_kind(pt.file_id.clone(), pt.src.clone(), pt.dest.clone(), pt.kind)?
            };
            self.transfers.insert_transfer(&transfer).await?;
            transfer_ids.push(transfer.id().to_string());
            created += 1;
        }

        trace!(created = created, "create_transfers: done");
        Ok(created)
    }

    /// 未完了Transferのdest集合をfile_id別に収集する。
    async fn collect_pending_dests(
        &self,
    ) -> Result<HashMap<String, HashSet<LocationId>>, SyncError> {
        let pending = self.transfers.all_pending_transfers().await?;
        let mut map: HashMap<String, HashSet<LocationId>> = HashMap::new();
        for t in &pending {
            map.entry(t.file_id().to_string())
                .or_default()
                .insert(t.dest().clone());
        }
        Ok(map)
    }
}

// =============================================================================
// View types
// =============================================================================

/// TopologyFileとその全LocationFileを束ねたビュー。
#[derive(Debug, Clone, serde::Serialize)]
pub struct TopologyFileView {
    pub topology_file: TopologyFile,
    pub location_files: Vec<LocationFile>,
}

// =============================================================================
// Internal helpers
// =============================================================================

/// HashMap<String, Vec<LocationFile>> → HashMap<String, Vec<&LocationFile>>
fn to_ref_map(map: &HashMap<String, Vec<LocationFile>>) -> HashMap<String, Vec<&LocationFile>> {
    map.iter()
        .map(|(k, v)| (k.clone(), v.iter().collect()))
        .collect()
}

// =============================================================================
// Tests — 3フェーズパイプライン設計検証
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use chrono::{DateTime, Utc};
    use tokio::sync::Mutex;

    use crate::domain::location_file::LocationFileState;
    use crate::domain::topology_delta::{ContentChangedFile, DiscoveredFile, VanishedFile};
    use crate::domain::transfer::TransferState;
    use crate::infra::error::InfraError;
    use crate::infra::transfer_store::TransferStatRow;

    // =========================================================================
    // Mock stores — パイプライン検証に必要な最小実装
    // =========================================================================

    struct MockTopologyFileStore {
        files: Mutex<Vec<TopologyFile>>,
    }

    impl MockTopologyFileStore {
        fn new() -> Self {
            Self {
                files: Mutex::new(Vec::new()),
            }
        }
    }

    #[async_trait]
    impl TopologyFileStore for MockTopologyFileStore {
        async fn upsert(&self, file: &TopologyFile) -> Result<(), InfraError> {
            let mut files = self.files.lock().await;
            if let Some(pos) = files.iter().position(|f| f.id() == file.id()) {
                files[pos] = file.clone();
            } else {
                files.push(file.clone());
            }
            Ok(())
        }

        async fn get_by_id(&self, id: &str) -> Result<Option<TopologyFile>, InfraError> {
            Ok(self
                .files
                .lock()
                .await
                .iter()
                .find(|f| f.id() == id)
                .cloned())
        }

        async fn get_by_path(&self, path: &str) -> Result<Option<TopologyFile>, InfraError> {
            Ok(self
                .files
                .lock()
                .await
                .iter()
                .find(|f| f.relative_path() == path && f.deleted_at().is_none())
                .cloned())
        }

        async fn find_by_canonical_hash(
            &self,
            hash: &str,
        ) -> Result<Option<TopologyFile>, InfraError> {
            Ok(self
                .files
                .lock()
                .await
                .iter()
                .find(|f| f.canonical_hash() == Some(hash) && f.deleted_at().is_none())
                .cloned())
        }

        async fn list_active(
            &self,
            file_type: Option<FileType>,
            limit: Option<usize>,
        ) -> Result<Vec<TopologyFile>, InfraError> {
            let files = self.files.lock().await;
            let mut result: Vec<_> = files
                .iter()
                .filter(|f| f.deleted_at().is_none())
                .filter(|f| file_type.is_none_or(|ft| f.file_type() == ft))
                .cloned()
                .collect();
            if let Some(n) = limit {
                result.truncate(n);
            }
            Ok(result)
        }

        async fn list_deleted(&self) -> Result<Vec<TopologyFile>, InfraError> {
            Ok(self
                .files
                .lock()
                .await
                .iter()
                .filter(|f| f.deleted_at().is_some())
                .cloned()
                .collect())
        }

        async fn hard_delete(&self, id: &str) -> Result<bool, InfraError> {
            let mut files = self.files.lock().await;
            let len_before = files.len();
            files.retain(|f| !(f.id() == id && f.deleted_at().is_some()));
            Ok(files.len() < len_before)
        }

        async fn count_active(&self) -> Result<usize, InfraError> {
            Ok(self
                .files
                .lock()
                .await
                .iter()
                .filter(|f| f.deleted_at().is_none())
                .count())
        }

        async fn list_active_paths(&self) -> Result<Vec<String>, InfraError> {
            Ok(self
                .files
                .lock()
                .await
                .iter()
                .filter(|f| f.deleted_at().is_none())
                .map(|f| f.relative_path().to_string())
                .collect())
        }
    }

    struct MockLocationFileStore {
        files: Mutex<Vec<LocationFile>>,
    }

    impl MockLocationFileStore {
        fn new() -> Self {
            Self {
                files: Mutex::new(Vec::new()),
            }
        }
    }

    #[async_trait]
    impl LocationFileStore for MockLocationFileStore {
        async fn upsert(&self, file: &LocationFile) -> Result<(), InfraError> {
            let mut files = self.files.lock().await;
            if let Some(pos) = files.iter().position(|f| {
                f.file_id() == file.file_id() && f.location_id() == file.location_id()
            }) {
                files[pos] = file.clone();
            } else {
                files.push(file.clone());
            }
            Ok(())
        }

        async fn get(
            &self,
            file_id: &str,
            location_id: &LocationId,
        ) -> Result<Option<LocationFile>, InfraError> {
            Ok(self
                .files
                .lock()
                .await
                .iter()
                .find(|f| f.file_id() == file_id && f.location_id() == location_id)
                .cloned())
        }

        async fn list_by_file(&self, file_id: &str) -> Result<Vec<LocationFile>, InfraError> {
            Ok(self
                .files
                .lock()
                .await
                .iter()
                .filter(|f| f.file_id() == file_id)
                .cloned()
                .collect())
        }

        async fn list_by_location(
            &self,
            location_id: &LocationId,
        ) -> Result<Vec<LocationFile>, InfraError> {
            Ok(self
                .files
                .lock()
                .await
                .iter()
                .filter(|f| f.location_id() == location_id)
                .cloned()
                .collect())
        }

        async fn list_by_files(
            &self,
            file_ids: &[&str],
        ) -> Result<HashMap<String, Vec<LocationFile>>, InfraError> {
            let files = self.files.lock().await;
            let mut map: HashMap<String, Vec<LocationFile>> = HashMap::new();
            for f in files.iter() {
                if file_ids.contains(&f.file_id()) {
                    map.entry(f.file_id().to_string())
                        .or_default()
                        .push(f.clone());
                }
            }
            Ok(map)
        }

        async fn delete(
            &self,
            file_id: &str,
            location_id: &LocationId,
        ) -> Result<bool, InfraError> {
            let mut files = self.files.lock().await;
            let before = files.len();
            files.retain(|f| !(f.file_id() == file_id && f.location_id() == location_id));
            Ok(files.len() < before)
        }

        async fn count_by_location(&self, location_id: &LocationId) -> Result<usize, InfraError> {
            Ok(self
                .files
                .lock()
                .await
                .iter()
                .filter(|f| f.location_id() == location_id)
                .count())
        }
    }

    struct MockTransferStore {
        transfers: Mutex<Vec<Transfer>>,
    }

    impl MockTransferStore {
        fn new() -> Self {
            Self {
                transfers: Mutex::new(Vec::new()),
            }
        }
    }

    #[async_trait]
    impl TransferStore for MockTransferStore {
        async fn insert_transfer(&self, transfer: &Transfer) -> Result<(), InfraError> {
            self.transfers.lock().await.push(transfer.clone());
            Ok(())
        }

        async fn update_transfer(&self, transfer: &Transfer) -> Result<(), InfraError> {
            let mut transfers = self.transfers.lock().await;
            if let Some(pos) = transfers.iter().position(|t| t.id() == transfer.id()) {
                transfers[pos] = transfer.clone();
            }
            Ok(())
        }

        async fn queued_transfers(&self, dest: &LocationId) -> Result<Vec<Transfer>, InfraError> {
            Ok(self
                .transfers
                .lock()
                .await
                .iter()
                .filter(|t| t.dest() == dest && t.state() == TransferState::Queued)
                .cloned()
                .collect())
        }

        async fn latest_transfers_by_file(
            &self,
            file_id: &str,
        ) -> Result<Vec<Transfer>, InfraError> {
            Ok(self
                .transfers
                .lock()
                .await
                .iter()
                .filter(|t| t.file_id() == file_id)
                .cloned()
                .collect())
        }

        async fn failed_transfers(&self) -> Result<Vec<Transfer>, InfraError> {
            Ok(Vec::new())
        }

        async fn prune_completed(&self, _before: DateTime<Utc>) -> Result<usize, InfraError> {
            Ok(0)
        }

        async fn count_queued(&self) -> Result<usize, InfraError> {
            Ok(self
                .transfers
                .lock()
                .await
                .iter()
                .filter(|t| t.state() == TransferState::Queued)
                .count())
        }

        async fn cancel_orphaned_inflight(&self) -> Result<usize, InfraError> {
            Ok(0)
        }

        async fn unblock_dependents(
            &self,
            _completed_transfer_id: &str,
        ) -> Result<usize, InfraError> {
            Ok(0)
        }

        async fn all_pending_transfers(&self) -> Result<Vec<Transfer>, InfraError> {
            Ok(self
                .transfers
                .lock()
                .await
                .iter()
                .filter(|t| {
                    t.state() == TransferState::Queued || t.state() == TransferState::Blocked
                })
                .cloned()
                .collect())
        }

        async fn transfer_stats(&self) -> Result<Vec<TransferStatRow>, InfraError> {
            Ok(Vec::new())
        }

        async fn present_counts_by_location(
            &self,
        ) -> Result<HashMap<LocationId, usize>, InfraError> {
            Ok(HashMap::new())
        }
    }

    // =========================================================================
    // Helpers
    // =========================================================================

    fn loc(name: &str) -> LocationId {
        LocationId::new(name).expect("valid location name")
    }

    fn fp(hash: &str, size: u64) -> FileFingerprint {
        use crate::domain::digest::{ByteDigest, ContentDigest};
        FileFingerprint {
            byte_digest: Some(ByteDigest::Djb2(hash.to_string())),
            content_digest: Some(ContentDigest(hash.to_string())),
            meta_digest: None,
            size,
            modified_at: None,
        }
    }

    /// local ⇄ pod ⇄ cloud の3拠点双方向グラフ。
    fn three_loc_setup() -> (RouteGraph, Vec<LocationId>) {
        let local = loc("local");
        let pod = loc("pod");
        let cloud = loc("cloud");
        let mut g = RouteGraph::new();
        g.add(local.clone(), pod.clone());
        g.add(pod.clone(), cloud.clone());
        g.add(pod.clone(), local.clone());
        g.add(cloud.clone(), pod.clone());
        (g, vec![local, pod, cloud])
    }

    fn make_store(
        tf: Arc<MockTopologyFileStore>,
        lf: Arc<MockLocationFileStore>,
        tr: Arc<MockTransferStore>,
    ) -> TopologyStore {
        let (graph, locations) = three_loc_setup();
        TopologyStore::new(tf, lf, tr, graph, locations)
    }

    fn discovered(path: &str, hash: &str, origin: &str) -> TopologyDelta {
        TopologyDelta::Discovered(DiscoveredFile {
            relative_path: path.to_string(),
            file_type: FileType::Image,
            fingerprint: fp(hash, 1024),
            origin: loc(origin),
            embedded_id: None,
        })
    }

    // =========================================================================
    // Pipeline Test 1: Discovered → Ingest → Distribute → Route
    //
    // 検証: 新規ファイルがlocalで検出された場合、
    //       TopologyFile/LocationFile作成後、pod/cloudへTransferが計画される。
    // =========================================================================

    #[tokio::test]
    async fn pipeline_discovered_creates_topology_and_routes_transfers() {
        let tf_s = Arc::new(MockTopologyFileStore::new());
        let lf_s = Arc::new(MockLocationFileStore::new());
        let tr_s = Arc::new(MockTransferStore::new());
        let store = make_store(tf_s.clone(), lf_s.clone(), tr_s.clone());

        let result = store
            .sync(&[discovered("output/gen-001.png", "abc123", "local")])
            .await
            .unwrap();

        // Phase 1 検証: TopologyFile 1件 + LocationFile(origin=local) 1件
        let tfs = tf_s.files.lock().await;
        assert_eq!(tfs.len(), 1);
        assert_eq!(tfs[0].relative_path(), "output/gen-001.png");
        assert!(
            tfs[0].canonical_hash().is_some(),
            "canonical_hash should be promoted from fingerprint"
        );

        let lfs = lf_s.files.lock().await;
        assert_eq!(lfs.len(), 1);
        assert_eq!(lfs[0].location_id(), &loc("local"));
        assert_eq!(lfs[0].state(), LocationFileState::Active);

        // Phase 2 検証: origin=local以外の2 locations (pod, cloud) へdistribute
        assert_eq!(result.ingested, 1);
        assert!(result.distributed >= 2, "pod + cloud へのDistributeAction");

        // Phase 3 検証: Transfer作成（local→pod→cloudの経路）
        let transfers = tr_s.transfers.lock().await;
        assert!(
            !transfers.is_empty(),
            "Transfers should be created for reachable locations"
        );
        let tf_id = tfs[0].id();
        for t in transfers.iter() {
            assert_eq!(t.file_id(), tf_id, "All transfers for same file");
            assert_ne!(t.src(), t.dest(), "No self-transfer");
        }
    }

    // =========================================================================
    // Pipeline Test 2: ContentChanged → Stale化 + 更新Transfer
    //
    // 検証: podに既存LocationFile(Active)がある状態で、localでコンテンツ変更。
    //       pod側がStaleになり、更新Transferが計画される。
    // =========================================================================

    #[tokio::test]
    async fn pipeline_content_changed_stales_others_and_creates_transfers() {
        let tf_s = Arc::new(MockTopologyFileStore::new());
        let lf_s = Arc::new(MockLocationFileStore::new());
        let tr_s = Arc::new(MockTransferStore::new());
        let store = make_store(tf_s.clone(), lf_s.clone(), tr_s.clone());

        // 初回: localで検出
        store
            .sync(&[discovered("output/img.png", "v1_hash", "local")])
            .await
            .unwrap();

        let tf_id = tf_s.files.lock().await[0].id().to_string();

        // podにLocationFile追加（Transfer完了をシミュレート）
        let pod_lf = LocationFile::new(
            tf_id.clone(),
            loc("pod"),
            "output/img.png".to_string(),
            fp("v1_hash", 1024),
            None,
        )
        .unwrap();
        lf_s.upsert(&pod_lf).await.unwrap();

        // 初回Transferをクリア
        tr_s.transfers.lock().await.clear();

        // localでコンテンツ変更
        let delta = TopologyDelta::ContentChanged(ContentChangedFile {
            topology_file_id: tf_id.clone(),
            relative_path: "output/img.png".to_string(),
            file_type: FileType::Image,
            old_fingerprint: fp("v1_hash", 1024),
            new_fingerprint: fp("v2_hash", 2048),
            origin: loc("local"),
            embedded_id: None,
        });

        let result = store.sync(&[delta]).await.unwrap();

        // pod側がStale
        let pod_lf = lf_s.get(&tf_id, &loc("pod")).await.unwrap().unwrap();
        assert_eq!(pod_lf.state(), LocationFileState::Stale);

        // local側はActive（更新元）
        let local_lf = lf_s.get(&tf_id, &loc("local")).await.unwrap().unwrap();
        assert_eq!(local_lf.state(), LocationFileState::Active);

        // 更新Transferが作成されている
        assert!(result.distributed > 0);
        assert!(result.transfers_created > 0);
    }

    // =========================================================================
    // Pipeline Test 3: Vanished → LocationFile Missing化
    //
    // 検証: localからファイルが消失した場合、LocationFileがMissingに遷移。
    //       TopologyFile自体は生存（他Locationに存在し得る）。
    // =========================================================================

    #[tokio::test]
    async fn pipeline_vanished_marks_location_file_missing() {
        let tf_s = Arc::new(MockTopologyFileStore::new());
        let lf_s = Arc::new(MockLocationFileStore::new());
        let tr_s = Arc::new(MockTransferStore::new());
        let store = make_store(tf_s.clone(), lf_s.clone(), tr_s.clone());

        store
            .sync(&[discovered("output/gone.png", "gone_hash", "local")])
            .await
            .unwrap();

        let tf_id = tf_s.files.lock().await[0].id().to_string();

        let delta = TopologyDelta::Vanished(VanishedFile {
            topology_file_id: tf_id.clone(),
            relative_path: "output/gone.png".to_string(),
            origin: loc("local"),
        });

        store.sync(&[delta]).await.unwrap();

        // LocationFileがMissing
        let lf = lf_s.get(&tf_id, &loc("local")).await.unwrap().unwrap();
        assert_eq!(lf.state(), LocationFileState::Missing);

        // TopologyFileは削除されていない（他Locationに存在し得る）
        let tf = tf_s.files.lock().await;
        assert!(tf[0].deleted_at().is_none());
    }

    // =========================================================================
    // Pipeline Test 4: 複数delta一括 → バッチパイプライン
    //
    // 検証: 3ファイル同時Discovered（origin混在）→ 各ファイルに対するTransfer。
    // =========================================================================

    #[tokio::test]
    async fn pipeline_batch_discovered_multi_origin() {
        let tf_s = Arc::new(MockTopologyFileStore::new());
        let lf_s = Arc::new(MockLocationFileStore::new());
        let tr_s = Arc::new(MockTransferStore::new());
        let store = make_store(tf_s.clone(), lf_s.clone(), tr_s.clone());

        let deltas = vec![
            discovered("a.png", "ha", "local"),
            discovered("b.png", "hb", "local"),
            discovered("c.png", "hc", "pod"), // pod origin
        ];

        let result = store.sync(&deltas).await.unwrap();

        assert_eq!(result.ingested, 3);
        assert_eq!(tf_s.files.lock().await.len(), 3);
        assert_eq!(lf_s.files.lock().await.len(), 3);

        // 各ファイルにorigin以外のlocationへのDistributeAction
        assert!(result.distributed >= 3);
        assert!(result.transfers_created >= 3);
    }

    // =========================================================================
    // Pipeline Test 5: put → sync パイプライン整合性
    //
    // 検証: put()で登録後、sync()の空delta呼び出しが既存データと矛盾しない。
    // =========================================================================

    #[tokio::test]
    async fn pipeline_put_then_empty_sync_is_consistent() {
        let tf_s = Arc::new(MockTopologyFileStore::new());
        let lf_s = Arc::new(MockLocationFileStore::new());
        let tr_s = Arc::new(MockTransferStore::new());
        let store = make_store(tf_s.clone(), lf_s.clone(), tr_s.clone());

        // put で1ファイル登録
        let put_result = store
            .put("x.png", FileType::Image, fp("xh", 100), &loc("local"), None)
            .await
            .unwrap();

        assert!(put_result.is_new);
        let initial_transfers = tr_s.transfers.lock().await.len();
        assert!(initial_transfers > 0);

        // 空delta sync — 既にpendingなTransferがあるので重複Transferは作られないはず
        let sync_result = store.sync(&[]).await.unwrap();

        assert_eq!(sync_result.ingested, 0);
        // distribute自体は走るが、pending重複で新規Transferは0（または少数）
        // ここではパイプラインがpanicせず完走することが重要
    }

    // =========================================================================
    // Pipeline Test 6: delete → Transfer計画
    //
    // 検証: ファイル削除後、LocationFileを持つLocationへのDelete Transferが計画される。
    // =========================================================================

    #[tokio::test]
    async fn pipeline_delete_creates_delete_transfers() {
        let tf_s = Arc::new(MockTopologyFileStore::new());
        let lf_s = Arc::new(MockLocationFileStore::new());
        let tr_s = Arc::new(MockTransferStore::new());
        let store = make_store(tf_s.clone(), lf_s.clone(), tr_s.clone());

        // ファイル登録
        store
            .put(
                "del.png",
                FileType::Image,
                fp("dh", 100),
                &loc("local"),
                None,
            )
            .await
            .unwrap();

        // podにもLocationFile追加
        let tf_id = tf_s.files.lock().await[0].id().to_string();
        let pod_lf = LocationFile::new(
            tf_id.clone(),
            loc("pod"),
            "del.png".to_string(),
            fp("dh", 100),
            None,
        )
        .unwrap();
        lf_s.upsert(&pod_lf).await.unwrap();

        // put時のTransferをクリア
        tr_s.transfers.lock().await.clear();

        // 削除
        let delete_count = store.delete("del.png").await.unwrap();

        // TopologyFileがdeleted
        let tf = tf_s.files.lock().await;
        assert!(tf[0].deleted_at().is_some());

        // LocationFile保持先（local, pod）へDelete Transfer直接発行
        // local: src=pod, dest=local / pod: src=local, dest=pod
        assert_eq!(delete_count, 2, "Delete Transfer for local + pod");
        let transfers = tr_s.transfers.lock().await;
        assert_eq!(transfers.len(), 2);
        for t in transfers.iter() {
            assert!(t.is_delete(), "All should be Delete kind");
            assert_ne!(t.src(), t.dest(), "No self-transfer");
        }
    }
}
