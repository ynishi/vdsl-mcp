//! TransferPlan — FileDelta から必要な Transfer を計画する純粋関数。
//!
//! ドメインロジックの核心。インフラに依存しない。
//!
//! # 入力
//!
//! - `FileDelta[]` — scanフェーズの出力
//! - [`Topology`] — 経路トポロジー（到達可能性・最適経路の解決を抽象化）
//! - `HashMap<LocationId, PresenceState>` — 各locationでの現在の存在状態
//!
//! # 計画ルール
//!
//! origin から到達可能な全destination に到達する最小コスト経路を
//! [`Topology`] 経由で計算し、依存順序付きの `PlannedTransfer` 列に変換する。
//!
//! chain転送 (e.g., local→pod→cloud) は plan 時に全hop分のTransfer が作成され、
//! 後段hopは `depends_on_index` で前段への依存を表現する。
//! execute時の動的next-hop生成は不要。

use std::collections::{HashMap, HashSet};

use super::delta::FileDelta;
use super::location::LocationId;
use super::topology_delta::DistributeAction;
use super::transfer::TransferKind;
use super::view::PresenceState;

/// 経路トポロジーの抽象。
///
/// plan.rs はこのトレイト経由でのみトポロジーにアクセスする。
/// 内部実装がGraph/DOD/静的テーブルの何であれ、このインタフェースが同じなら
/// plan.rs は変更不要。
pub trait Topology: Send + Sync {
    /// `origin` から到達可能な全locationの集合（origin自身は含まない）。
    fn reachable_from(&self, origin: &LocationId) -> HashSet<LocationId>;

    /// `origin` から `required_dests` 全てに到達する最小コスト経路の辺リスト。
    /// 辺は依存順序: (A,B) が (B,C) より前に来る。
    fn optimal_tree(
        &self,
        origin: &LocationId,
        required_dests: &HashSet<LocationId>,
    ) -> Vec<(LocationId, LocationId)>;
}

/// 計画されたTransfer。まだDB未書き込み。
///
/// Apply フェーズで `Transfer::new()` / `Transfer::with_dependency()` に変換される。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlannedTransfer {
    pub file_id: String,
    pub src: LocationId,
    pub dest: LocationId,
    pub kind: TransferKind,
    /// このTransferが依存する先行Transferのインデックス（同一Vec内）。
    /// `None` = 依存なし（即Queued）、`Some(i)` = i番目のTransfer完了後にQueued化。
    pub depends_on_index: Option<usize>,
}

/// 単一の FileDelta から最適なTransfer列を計画する。
///
/// `optimal_tree` で最小コスト到達木を計算し、依存順序付きTransfer列を返す。
///
/// # 引数
///
/// - `delta` — 1ファイルの変化
/// - `graph` — コスト付きルートトポロジー
/// - `presence` — このファイルの各locationでの存在状態（DB由来）。
///   キーが存在しない location は Absent として扱う。
///   **Modified の場合は内部で presence を無視する**（dest の内容は古いため）。
/// - `pending_dests` — 既に未完了Transfer が存在する dest の集合。
///   重複Transfer の抑止に使用。
pub fn plan_transfers_for(
    delta: &FileDelta,
    topology: &dyn Topology,
    presence: &HashMap<LocationId, PresenceState>,
    pending_dests: &HashSet<LocationId>,
) -> Vec<PlannedTransfer> {
    let origin = delta.origin();
    let file_id = delta.file_id().to_string();

    match delta {
        FileDelta::Added(_) => {
            plan_sync(&file_id, origin, topology, presence, pending_dests, false)
        }
        FileDelta::Modified(_) => {
            plan_sync(&file_id, origin, topology, presence, pending_dests, true)
        }
        FileDelta::Removed(_) => plan_delete(&file_id, origin, topology, pending_dests),
    }
}

/// origin から全到達可能destへのSync Transfer列を計画する。
///
/// - `stale_presence = false` (Added): presenceがPresentのdestはスキップ
/// - `stale_presence = true` (Modified): presenceを無視して全destに送る
pub fn plan_sync(
    file_id: &str,
    origin: &LocationId,
    topology: &dyn Topology,
    presence: &HashMap<LocationId, PresenceState>,
    pending_dests: &HashSet<LocationId>,
    stale_presence: bool,
) -> Vec<PlannedTransfer> {
    // 1. 必要な destination を特定
    let all_reachable = topology.reachable_from(origin);
    let required_dests: HashSet<LocationId> = all_reachable
        .into_iter()
        .filter(|dest| {
            if pending_dests.contains(dest) {
                return false;
            }
            if stale_presence {
                return true;
            }
            let state = presence.get(dest).copied().unwrap_or(PresenceState::Absent);
            state != PresenceState::Present
        })
        .collect();

    if required_dests.is_empty() {
        return Vec::new();
    }

    // 2. 最適到達木を計算
    let tree_edges = topology.optimal_tree(origin, &required_dests);

    // 3. 辺をPlannedTransferに変換（依存関係付き）
    edges_to_planned_transfers(&tree_edges, file_id, TransferKind::Sync)
}

/// origin から全到達可能destへのDelete Transfer列を計画する。
pub fn plan_delete(
    file_id: &str,
    origin: &LocationId,
    topology: &dyn Topology,
    pending_dests: &HashSet<LocationId>,
) -> Vec<PlannedTransfer> {
    let all_reachable = topology.reachable_from(origin);
    let required_dests: HashSet<LocationId> = all_reachable
        .into_iter()
        .filter(|dest| !pending_dests.contains(dest))
        .collect();

    if required_dests.is_empty() {
        return Vec::new();
    }

    let tree_edges = topology.optimal_tree(origin, &required_dests);
    edges_to_planned_transfers(&tree_edges, file_id, TransferKind::Delete)
}

/// optimal_tree の辺リスト（依存順序済み）をPlannedTransfer列に変換。
///
/// tree_edges は `optimal_tree` が返す依存順序付きリスト:
/// edge[i] の dest が edge[j] の src であれば j > i が保証されている。
pub fn edges_to_planned_transfers(
    tree_edges: &[(LocationId, LocationId)],
    file_id: &str,
    kind: TransferKind,
) -> Vec<PlannedTransfer> {
    let mut result = Vec::with_capacity(tree_edges.len());

    for (i, (src, dest)) in tree_edges.iter().enumerate() {
        // この辺の src が、先行する辺の dest であれば依存関係を設定
        let depends_on = (0..i).rev().find(|&j| &tree_edges[j].1 == src);

        result.push(PlannedTransfer {
            file_id: file_id.to_string(),
            src: src.clone(),
            dest: dest.clone(),
            kind,
            depends_on_index: depends_on,
        });
    }

    result
}

/// 複数の FileDelta から全体の TransferPlan を生成。
pub fn plan_all(
    deltas: &[FileDelta],
    topology: &dyn Topology,
    presence_map: &HashMap<String, HashMap<LocationId, PresenceState>>,
    pending_map: &HashMap<String, HashSet<LocationId>>,
) -> Vec<PlannedTransfer> {
    let empty_presence = HashMap::new();
    let empty_pending = HashSet::new();

    deltas
        .iter()
        .flat_map(|delta| {
            let file_id = delta.file_id();
            let presence = presence_map.get(file_id).unwrap_or(&empty_presence);
            let pending = pending_map.get(file_id).unwrap_or(&empty_pending);
            plan_transfers_for(delta, topology, presence, pending)
        })
        .collect()
}

// =============================================================================
// Phase 3: DistributeAction → PlannedTransfer (Topology中心モデル)
// =============================================================================

/// DistributeAction群をRouteGraph経由でPlannedTransferに変換する。
///
/// DistributeActionを `(topology_file_id, source, TransferKind)` でグルーピングし、
/// 各グループの全targetに到達する最適木(optimal_tree)を計算する。
///
/// # 引数
///
/// - `actions` — distribute_actions()の出力
/// - `topology` — ルーティングに使用するTopology(RouteGraph等)
/// - `pending_dests` — `file_id → 既に未完了Transferが存在するdest集合`。重複抑止用。
///
/// # アルゴリズム
///
/// 1. actions を (file_id, source, kind) でグループ化
/// 2. 各グループ: targets - pending_dests = required_dests
/// 3. optimal_tree(source, required_dests) → 依存順序付き辺リスト
/// 4. edges_to_planned_transfers() で PlannedTransfer に変換
pub fn plan_distribution(
    actions: &[DistributeAction],
    topology: &dyn Topology,
    pending_dests: &HashMap<String, HashSet<LocationId>>,
) -> Vec<PlannedTransfer> {
    // 1. グループ化: (file_id, source, kind) → targets
    let mut groups: HashMap<DistributeGroup, HashSet<LocationId>> = HashMap::new();

    for action in actions {
        let group = DistributeGroup::from_action(action);
        groups
            .entry(group)
            .or_default()
            .insert(action.target().clone());
    }

    let empty_pending = HashSet::new();
    let mut all_transfers = Vec::new();

    // 2. 各グループについて最適木を計算
    for (group, mut targets) in groups {
        // pending除外
        let pending = pending_dests.get(&group.file_id).unwrap_or(&empty_pending);
        targets.retain(|t| !pending.contains(t));

        if targets.is_empty() {
            continue;
        }

        // 3. optimal_tree
        let tree_edges = topology.optimal_tree(&group.source, &targets);

        // 4. PlannedTransferに変換
        let transfers = edges_to_planned_transfers(&tree_edges, &group.file_id, group.kind);
        all_transfers.extend(transfers);
    }

    all_transfers
}

/// plan_distribution のグループキー。
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct DistributeGroup {
    file_id: String,
    source: LocationId,
    kind: TransferKind,
}

impl DistributeGroup {
    fn from_action(action: &DistributeAction) -> Self {
        match action {
            DistributeAction::Send(a) => Self {
                file_id: a.topology_file_id.clone(),
                source: a.source.clone(),
                kind: TransferKind::Sync,
            },
            DistributeAction::Update(a) => Self {
                file_id: a.topology_file_id.clone(),
                source: a.source.clone(),
                kind: TransferKind::Sync,
            },
            DistributeAction::Delete(a) => Self {
                file_id: a.topology_file_id.clone(),
                // Deleteにはsourceがない。origin不要だが
                // optimal_treeの起点が必要なため、targetを仮のsourceとする。
                // → Delete はグループ単位ではなく個別にTransferを生成する。
                source: a.target.clone(),
                kind: TransferKind::Delete,
            },
        }
    }
}

/// Deleteアクション群を個別のPlannedTransferに変換する。
///
/// DeleteはRouteGraphの最適木を使わない（各Locationへの個別削除指示）。
/// plan_distributionから呼ばれるのではなく、Deleteを別経路で処理する場合に使用。
pub fn plan_deletes(
    delete_actions: &[DistributeAction],
    pending_dests: &HashMap<String, HashSet<LocationId>>,
) -> Vec<PlannedTransfer> {
    let empty_pending = HashSet::new();

    delete_actions
        .iter()
        .filter_map(|action| {
            if let DistributeAction::Delete(a) = action {
                let pending = pending_dests
                    .get(&a.topology_file_id)
                    .unwrap_or(&empty_pending);
                if pending.contains(&a.target) {
                    return None;
                }
                Some(PlannedTransfer {
                    file_id: a.topology_file_id.clone(),
                    src: a.target.clone(), // Delete: target自身で削除実行
                    dest: a.target.clone(),
                    kind: TransferKind::Delete,
                    depends_on_index: None,
                })
            } else {
                None
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::delta::{AddedFile, ModifiedFile, RemovedFile};
    use crate::domain::file_type::FileType;
    use crate::domain::fingerprint::FileFingerprint;
    use crate::domain::graph::{EdgeCost, RouteGraph};

    fn local() -> LocationId {
        LocationId::local()
    }
    fn cloud() -> LocationId {
        LocationId::new("cloud").unwrap()
    }
    fn pod() -> LocationId {
        LocationId::new("pod").unwrap()
    }

    fn fp(hash: &str, size: u64) -> FileFingerprint {
        FileFingerprint {
            file_hash: Some(hash.to_string()),
            content_hash: None,
            meta_hash: None,
            size,
            modified_at: None,
        }
    }

    /// local→cloud の単一辺グラフ
    fn simple_graph() -> RouteGraph {
        let mut g = RouteGraph::new();
        g.add(local(), cloud());
        g
    }

    /// local→pod→cloud のチェーン（経路最適化テスト用、コスト付き）
    fn chain_graph() -> RouteGraph {
        let mut g = RouteGraph::new();
        g.add_with_cost(local(), pod(), EdgeCost::new(1.0, 10));
        g.add_with_cost(pod(), cloud(), EdgeCost::new(2.0, 10));
        g
    }

    /// local→pod→cloud + local→cloud（直接辺あり、チェーンの方が安い）
    fn chain_with_direct_graph() -> RouteGraph {
        let mut g = RouteGraph::new();
        g.add_with_cost(local(), pod(), EdgeCost::new(1.0, 10));
        g.add_with_cost(pod(), cloud(), EdgeCost::new(2.0, 10));
        g.add_with_cost(local(), cloud(), EdgeCost::new(10.0, 10)); // 高コスト直接辺
        g
    }

    fn no_presence() -> HashMap<LocationId, PresenceState> {
        HashMap::new()
    }

    fn no_pending() -> HashSet<LocationId> {
        HashSet::new()
    }

    // =========================================================================
    // Added — simple graph
    // =========================================================================

    #[test]
    fn added_creates_sync_to_direct_dests() {
        let delta = FileDelta::Added(AddedFile {
            id: "f1".into(),
            relative_path: "a.png".into(),
            file_type: FileType::Image,
            fingerprint: fp("h1", 100),
            origin: local(),
            embedded_id: None,
        });

        let planned = plan_transfers_for(&delta, &simple_graph(), &no_presence(), &no_pending());

        assert_eq!(planned.len(), 1);
        assert_eq!(planned[0].src, local());
        assert_eq!(planned[0].dest, cloud());
        assert_eq!(planned[0].kind, TransferKind::Sync);
        assert_eq!(planned[0].depends_on_index, None);
    }

    #[test]
    fn added_skips_present_dest() {
        let delta = FileDelta::Added(AddedFile {
            id: "f1".into(),
            relative_path: "a.png".into(),
            file_type: FileType::Image,
            fingerprint: fp("h1", 100),
            origin: local(),
            embedded_id: None,
        });

        let mut presence = HashMap::new();
        presence.insert(cloud(), PresenceState::Present);

        let planned = plan_transfers_for(&delta, &simple_graph(), &presence, &no_pending());
        assert_eq!(planned.len(), 0, "Present dest should be skipped");
    }

    #[test]
    fn added_does_not_skip_pending_presence() {
        let delta = FileDelta::Added(AddedFile {
            id: "f1".into(),
            relative_path: "a.png".into(),
            file_type: FileType::Image,
            fingerprint: fp("h1", 100),
            origin: local(),
            embedded_id: None,
        });

        let mut presence = HashMap::new();
        presence.insert(cloud(), PresenceState::Pending);

        let planned = plan_transfers_for(&delta, &simple_graph(), &presence, &no_pending());
        assert_eq!(planned.len(), 1, "Pending dest needs sync");
    }

    #[test]
    fn added_skips_already_pending_transfer() {
        let delta = FileDelta::Added(AddedFile {
            id: "f1".into(),
            relative_path: "a.png".into(),
            file_type: FileType::Image,
            fingerprint: fp("h1", 100),
            origin: local(),
            embedded_id: None,
        });

        let mut pending = HashSet::new();
        pending.insert(cloud());

        let planned = plan_transfers_for(&delta, &simple_graph(), &no_presence(), &pending);
        assert_eq!(
            planned.len(),
            0,
            "Already pending transfer should not duplicate"
        );
    }

    // =========================================================================
    // Added — chain graph (全hop計画)
    // =========================================================================

    #[test]
    fn added_chain_plans_all_hops_with_dependencies() {
        let delta = FileDelta::Added(AddedFile {
            id: "f1".into(),
            relative_path: "a.png".into(),
            file_type: FileType::Image,
            fingerprint: fp("h1", 100),
            origin: local(),
            embedded_id: None,
        });

        let planned = plan_transfers_for(&delta, &chain_graph(), &no_presence(), &no_pending());

        // local→pod (Queued) + pod→cloud (depends on local→pod)
        assert_eq!(planned.len(), 2);
        assert_eq!(planned[0].src, local());
        assert_eq!(planned[0].dest, pod());
        assert_eq!(planned[0].depends_on_index, None);

        assert_eq!(planned[1].src, pod());
        assert_eq!(planned[1].dest, cloud());
        assert_eq!(planned[1].depends_on_index, Some(0));
    }

    // =========================================================================
    // Added — chain + direct (optimal tree picks chain)
    // =========================================================================

    #[test]
    fn added_chain_with_direct_picks_chain() {
        let delta = FileDelta::Added(AddedFile {
            id: "f1".into(),
            relative_path: "a.png".into(),
            file_type: FileType::Image,
            fingerprint: fp("h1", 100),
            origin: local(),
            embedded_id: None,
        });

        let planned = plan_transfers_for(
            &delta,
            &chain_with_direct_graph(),
            &no_presence(),
            &no_pending(),
        );

        // Should pick local→pod→cloud (cost 3.0), NOT local→pod + local→cloud (cost 11.0)
        assert_eq!(planned.len(), 2);
        assert_eq!(planned[0].src, local());
        assert_eq!(planned[0].dest, pod());
        assert_eq!(planned[1].src, pod());
        assert_eq!(planned[1].dest, cloud());
    }

    #[test]
    fn added_chain_with_direct_cloud_present_only_pod() {
        // cloud is already Present → only need to reach pod
        let delta = FileDelta::Added(AddedFile {
            id: "f1".into(),
            relative_path: "a.png".into(),
            file_type: FileType::Image,
            fingerprint: fp("h1", 100),
            origin: local(),
            embedded_id: None,
        });

        let mut presence = HashMap::new();
        presence.insert(cloud(), PresenceState::Present);

        let planned =
            plan_transfers_for(&delta, &chain_with_direct_graph(), &presence, &no_pending());

        // Only pod needs sync, cloud is present
        assert_eq!(planned.len(), 1);
        assert_eq!(planned[0].src, local());
        assert_eq!(planned[0].dest, pod());
        assert_eq!(planned[0].depends_on_index, None);
    }

    // =========================================================================
    // Modified
    // =========================================================================

    #[test]
    fn modified_creates_sync() {
        let delta = FileDelta::Modified(ModifiedFile {
            file_id: "f1".into(),
            relative_path: "a.png".into(),
            file_type: FileType::Image,
            old_fingerprint: fp("old", 100),
            new_fingerprint: fp("new", 200),
            origin: local(),
            embedded_id: None,
        });

        let planned = plan_transfers_for(&delta, &simple_graph(), &no_presence(), &no_pending());
        assert_eq!(planned.len(), 1);
        assert_eq!(planned[0].kind, TransferKind::Sync);
    }

    #[test]
    fn modified_ignores_present_dest() {
        let delta = FileDelta::Modified(ModifiedFile {
            file_id: "f1".into(),
            relative_path: "a.png".into(),
            file_type: FileType::Image,
            old_fingerprint: fp("old", 100),
            new_fingerprint: fp("new", 200),
            origin: local(),
            embedded_id: None,
        });

        let mut presence = HashMap::new();
        presence.insert(cloud(), PresenceState::Present);

        let planned = plan_transfers_for(&delta, &simple_graph(), &presence, &no_pending());
        assert_eq!(
            planned.len(),
            1,
            "Modified must sync even if dest is Present (content is stale)"
        );
    }

    #[test]
    fn modified_still_skips_pending_transfer() {
        let delta = FileDelta::Modified(ModifiedFile {
            file_id: "f1".into(),
            relative_path: "a.png".into(),
            file_type: FileType::Image,
            old_fingerprint: fp("old", 100),
            new_fingerprint: fp("new", 200),
            origin: local(),
            embedded_id: None,
        });

        let mut pending = HashSet::new();
        pending.insert(cloud());

        let planned = plan_transfers_for(&delta, &simple_graph(), &no_presence(), &pending);
        assert_eq!(
            planned.len(),
            0,
            "Modified must still respect pending_dests"
        );
    }

    #[test]
    fn modified_chain_plans_all_hops() {
        let delta = FileDelta::Modified(ModifiedFile {
            file_id: "f1".into(),
            relative_path: "a.png".into(),
            file_type: FileType::Image,
            old_fingerprint: fp("old", 100),
            new_fingerprint: fp("new", 200),
            origin: local(),
            embedded_id: None,
        });

        let planned = plan_transfers_for(&delta, &chain_graph(), &no_presence(), &no_pending());
        assert_eq!(planned.len(), 2);
        assert_eq!(planned[0].depends_on_index, None);
        assert_eq!(planned[1].depends_on_index, Some(0));
    }

    // =========================================================================
    // Removed
    // =========================================================================

    #[test]
    fn removed_creates_delete_transfers() {
        let delta = FileDelta::Removed(RemovedFile {
            file_id: "f1".into(),
            relative_path: "a.png".into(),
            origin: local(),
        });

        let planned = plan_transfers_for(&delta, &simple_graph(), &no_presence(), &no_pending());
        assert_eq!(planned.len(), 1);
        assert_eq!(planned[0].kind, TransferKind::Delete);
        assert_eq!(planned[0].src, local());
        assert_eq!(planned[0].dest, cloud());
    }

    #[test]
    fn removed_skips_pending_dest() {
        let delta = FileDelta::Removed(RemovedFile {
            file_id: "f1".into(),
            relative_path: "a.png".into(),
            origin: local(),
        });

        let mut pending = HashSet::new();
        pending.insert(cloud());

        let planned = plan_transfers_for(&delta, &simple_graph(), &no_presence(), &pending);
        assert_eq!(planned.len(), 0);
    }

    #[test]
    fn removed_chain_plans_all_hops() {
        let delta = FileDelta::Removed(RemovedFile {
            file_id: "f1".into(),
            relative_path: "a.png".into(),
            origin: local(),
        });

        let planned = plan_transfers_for(&delta, &chain_graph(), &no_presence(), &no_pending());
        assert_eq!(planned.len(), 2);
        assert_eq!(planned[0].kind, TransferKind::Delete);
        assert_eq!(planned[1].kind, TransferKind::Delete);
        assert_eq!(planned[1].depends_on_index, Some(0));
    }

    // =========================================================================
    // plan_all
    // =========================================================================

    #[test]
    fn plan_all_multiple_deltas() {
        let deltas = vec![
            FileDelta::Added(AddedFile {
                id: "f1".into(),
                relative_path: "a.png".into(),
                file_type: FileType::Image,
                fingerprint: fp("h1", 100),
                origin: local(),
                embedded_id: None,
            }),
            FileDelta::Removed(RemovedFile {
                file_id: "f2".into(),
                relative_path: "b.png".into(),
                origin: local(),
            }),
        ];

        let planned = plan_all(&deltas, &simple_graph(), &HashMap::new(), &HashMap::new());
        assert_eq!(planned.len(), 2);
        assert_eq!(planned[0].kind, TransferKind::Sync);
        assert_eq!(planned[1].kind, TransferKind::Delete);
    }

    #[test]
    fn plan_all_modified_ignores_presence() {
        let deltas = vec![FileDelta::Modified(ModifiedFile {
            file_id: "f1".into(),
            relative_path: "a.png".into(),
            file_type: FileType::Image,
            old_fingerprint: fp("old", 100),
            new_fingerprint: fp("new", 200),
            origin: local(),
            embedded_id: None,
        })];

        let mut presence_map = HashMap::new();
        let mut f1_presence = HashMap::new();
        f1_presence.insert(cloud(), PresenceState::Present);
        presence_map.insert("f1".to_string(), f1_presence);

        let planned = plan_all(&deltas, &simple_graph(), &presence_map, &HashMap::new());
        assert_eq!(planned.len(), 1, "Modified must sync even via plan_all");
    }

    #[test]
    fn plan_all_respects_per_file_presence() {
        let deltas = vec![
            FileDelta::Added(AddedFile {
                id: "f1".into(),
                relative_path: "a.png".into(),
                file_type: FileType::Image,
                fingerprint: fp("h1", 100),
                origin: local(),
                embedded_id: None,
            }),
            FileDelta::Added(AddedFile {
                id: "f2".into(),
                relative_path: "b.png".into(),
                file_type: FileType::Image,
                fingerprint: fp("h2", 200),
                origin: local(),
                embedded_id: None,
            }),
        ];

        let mut presence_map = HashMap::new();
        let mut f1_presence = HashMap::new();
        f1_presence.insert(cloud(), PresenceState::Present);
        presence_map.insert("f1".to_string(), f1_presence);

        let planned = plan_all(&deltas, &simple_graph(), &presence_map, &HashMap::new());
        assert_eq!(planned.len(), 1);
        assert_eq!(planned[0].file_id, "f2");
    }

    // =========================================================================
    // plan_distribution — Phase 3 (Topology中心モデル)
    // =========================================================================

    use crate::domain::topology_delta::{DeleteAction, DistributeAction, SendAction, UpdateAction};

    fn send_action(file_id: &str, source: LocationId, target: LocationId) -> DistributeAction {
        DistributeAction::Send(SendAction {
            topology_file_id: file_id.into(),
            relative_path: format!("{file_id}.png"),
            file_type: FileType::Image,
            target,
            source,
        })
    }

    fn update_action(file_id: &str, source: LocationId, target: LocationId) -> DistributeAction {
        DistributeAction::Update(UpdateAction {
            topology_file_id: file_id.into(),
            relative_path: format!("{file_id}.png"),
            target,
            source,
        })
    }

    fn delete_action(file_id: &str, target: LocationId) -> DistributeAction {
        DistributeAction::Delete(DeleteAction {
            topology_file_id: file_id.into(),
            relative_path: format!("{file_id}.png"),
            target,
        })
    }

    #[test]
    fn plan_distribution_single_send() {
        let actions = vec![send_action("f1", local(), cloud())];

        let planned = plan_distribution(&actions, &simple_graph(), &HashMap::new());

        assert_eq!(planned.len(), 1);
        assert_eq!(planned[0].file_id, "f1");
        assert_eq!(planned[0].src, local());
        assert_eq!(planned[0].dest, cloud());
        assert_eq!(planned[0].kind, TransferKind::Sync);
        assert_eq!(planned[0].depends_on_index, None);
    }

    #[test]
    fn plan_distribution_groups_same_file_targets() {
        // 同じファイルをpodとcloudに送る → optimal_treeでグルーピング
        let actions = vec![
            send_action("f1", local(), pod()),
            send_action("f1", local(), cloud()),
        ];

        let planned = plan_distribution(&actions, &chain_graph(), &HashMap::new());

        // chain_graph: local→pod→cloud
        // optimal_tree(local, {pod, cloud}) → local→pod, pod→cloud
        assert_eq!(planned.len(), 2);
        assert_eq!(planned[0].src, local());
        assert_eq!(planned[0].dest, pod());
        assert_eq!(planned[0].depends_on_index, None);
        assert_eq!(planned[1].src, pod());
        assert_eq!(planned[1].dest, cloud());
        assert_eq!(planned[1].depends_on_index, Some(0));
    }

    #[test]
    fn plan_distribution_respects_pending() {
        let actions = vec![
            send_action("f1", local(), pod()),
            send_action("f1", local(), cloud()),
        ];

        let mut pending = HashMap::new();
        pending.insert("f1".to_string(), HashSet::from([cloud()]));

        let planned = plan_distribution(&actions, &chain_graph(), &pending);

        // cloudはpending → skip。podのみ
        assert_eq!(planned.len(), 1);
        assert_eq!(planned[0].dest, pod());
    }

    #[test]
    fn plan_distribution_update_uses_sync_kind() {
        let actions = vec![update_action("f1", local(), cloud())];

        let planned = plan_distribution(&actions, &simple_graph(), &HashMap::new());

        assert_eq!(planned.len(), 1);
        assert_eq!(planned[0].kind, TransferKind::Sync);
    }

    #[test]
    fn plan_distribution_multiple_files() {
        let actions = vec![
            send_action("f1", local(), cloud()),
            send_action("f2", local(), cloud()),
        ];

        let planned = plan_distribution(&actions, &simple_graph(), &HashMap::new());

        assert_eq!(planned.len(), 2);
        let file_ids: HashSet<_> = planned.iter().map(|p| p.file_id.as_str()).collect();
        assert!(file_ids.contains("f1"));
        assert!(file_ids.contains("f2"));
    }

    #[test]
    fn plan_distribution_chain_with_direct_picks_optimal() {
        // chain_with_direct_graph: local→pod(1.0), pod→cloud(2.0), local→cloud(10.0)
        let actions = vec![
            send_action("f1", local(), pod()),
            send_action("f1", local(), cloud()),
        ];

        let planned = plan_distribution(&actions, &chain_with_direct_graph(), &HashMap::new());

        // optimal: local→pod→cloud (cost 3.0) < local→pod + local→cloud (cost 11.0)
        assert_eq!(planned.len(), 2);
        assert_eq!(planned[0].src, local());
        assert_eq!(planned[0].dest, pod());
        assert_eq!(planned[1].src, pod());
        assert_eq!(planned[1].dest, cloud());
    }

    // =========================================================================
    // plan_deletes
    // =========================================================================

    #[test]
    fn plan_deletes_creates_individual_transfers() {
        let actions = vec![delete_action("f1", pod()), delete_action("f1", cloud())];

        let planned = plan_deletes(&actions, &HashMap::new());

        assert_eq!(planned.len(), 2);
        assert!(planned.iter().all(|p| p.kind == TransferKind::Delete));
        assert!(planned.iter().all(|p| p.depends_on_index.is_none()));
        let dests: HashSet<_> = planned.iter().map(|p| p.dest.clone()).collect();
        assert!(dests.contains(&pod()));
        assert!(dests.contains(&cloud()));
    }

    #[test]
    fn plan_deletes_respects_pending() {
        let actions = vec![delete_action("f1", pod()), delete_action("f1", cloud())];

        let mut pending = HashMap::new();
        pending.insert("f1".to_string(), HashSet::from([cloud()]));

        let planned = plan_deletes(&actions, &pending);

        assert_eq!(planned.len(), 1);
        assert_eq!(planned[0].dest, pod());
    }

    #[test]
    fn plan_deletes_ignores_non_delete_actions() {
        let actions = vec![
            send_action("f1", local(), cloud()),
            delete_action("f2", pod()),
        ];

        let planned = plan_deletes(&actions, &HashMap::new());

        assert_eq!(planned.len(), 1);
        assert_eq!(planned[0].file_id, "f2");
    }
}
