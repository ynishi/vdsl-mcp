//! TransferPlan — DistributeAction から必要な Transfer を計画する純粋関数。
//!
//! ドメインロジックの核心。インフラに依存しない。
//!
//! # 入力
//!
//! - `DistributeAction[]` — distribute_actions() の出力
//! - [`Topology`] — 経路トポロジー（到達可能性・最適経路の解決を抽象化）
//! - `pending_dests` / `existing_presences` — 重複抑止・マルチソース最適化用
//!
//! # 計画ルール
//!
//! source から全target に到達する最小コスト経路を
//! [`Topology`] 経由で計算し、依存順序付きの `PlannedTransfer` 列に変換する。
//!
//! chain転送 (e.g., local→pod→cloud) は plan 時に全hop分のTransfer が作成され、
//! 後段hopは `depends_on_index` で前段への依存を表現する。
//! execute時の動的next-hop生成は不要。

use std::collections::{HashMap, HashSet};

use tracing::trace;

use super::location::LocationId;
use super::topology_delta::DistributeAction;
use super::transfer::TransferKind;

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

    /// 複数ソースから `required_dests` 全てに到達する最小コスト経路の辺リスト。
    ///
    /// `sources` 内の全locationがデータを保持している前提で、Multi-source Dijkstraにより
    /// 最も安いsource→dest経路を選択する。
    /// デフォルト実装は最初のsourceにフォールバック。
    fn optimal_tree_multi_source(
        &self,
        sources: &HashSet<LocationId>,
        required_dests: &HashSet<LocationId>,
    ) -> Vec<(LocationId, LocationId)> {
        // Default: pick first source and use single-source optimal_tree
        if let Some(origin) = sources.iter().next() {
            self.optimal_tree(origin, required_dests)
        } else {
            Vec::new()
        }
    }
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
/// - `existing_presences` — `file_id → 既にデータを保持しているlocation集合`。
///   Multi-source Dijkstraでsource以外の保持locationも考慮し、最安経路を選択する。
///
/// # アルゴリズム
///
/// 1. actions を (file_id, source, kind) でグループ化
/// 2. 各グループ: targets - pending_dests = required_dests
/// 3. optimal_tree_multi_source(sources, required_dests) → 依存順序付き辺リスト
///    sources = {group.source} ∪ existing_presences[file_id]
/// 4. edges_to_planned_transfers() で PlannedTransfer に変換
pub fn plan_distribution(
    actions: &[DistributeAction],
    topology: &dyn Topology,
    pending_dests: &HashMap<String, HashSet<LocationId>>,
    existing_presences: &HashMap<String, HashSet<LocationId>>,
) -> Vec<PlannedTransfer> {
    trace!(
        actions = actions.len(),
        pending_dests = pending_dests.len(),
        existing_presences = existing_presences.len(),
        "plan_distribution: start"
    );

    // 1. グループ化: (file_id, source, kind) → targets
    let mut groups: HashMap<DistributeGroup, HashSet<LocationId>> = HashMap::new();

    for action in actions {
        let group = DistributeGroup::from_action(action);
        groups
            .entry(group)
            .or_default()
            .insert(action.target().clone());
    }

    trace!(groups = groups.len(), "plan_distribution: grouped");

    let empty_pending = HashSet::new();
    let empty_presences = HashSet::new();
    let mut all_transfers = Vec::new();

    // 2. 各グループについて最適木を計算
    for (group, mut targets) in groups {
        // pending除外
        let pending = pending_dests.get(&group.file_id).unwrap_or(&empty_pending);
        let pre_filter = targets.len();
        targets.retain(|t| !pending.contains(t));

        if targets.is_empty() {
            trace!(
                file_id = %group.file_id,
                kind = ?group.kind,
                pre_filter = pre_filter,
                "plan_distribution: all targets pending, skip"
            );
            continue;
        }

        // 3. Multi-source optimal_tree
        // sources = ingest origin + 既にデータを保持しているlocation
        let existing = existing_presences
            .get(&group.file_id)
            .unwrap_or(&empty_presences);
        let mut sources = existing.clone();
        sources.insert(group.source.clone());

        trace!(
            file_id = %group.file_id,
            kind = ?group.kind,
            sources = ?sources.iter().map(|s| s.to_string()).collect::<Vec<_>>(),
            targets = ?targets.iter().map(|t| t.to_string()).collect::<Vec<_>>(),
            "plan_distribution: computing optimal tree"
        );

        let tree_edges = topology.optimal_tree_multi_source(&sources, &targets);

        trace!(
            file_id = %group.file_id,
            edges = tree_edges.len(),
            "plan_distribution: tree computed"
        );

        // 4. PlannedTransferに変換（インデックスをグローバルオフセット）
        let base_offset = all_transfers.len();
        let transfers = edges_to_planned_transfers(&tree_edges, &group.file_id, group.kind);
        for mut pt in transfers {
            pt.depends_on_index = pt.depends_on_index.map(|i| i + base_offset);
            all_transfers.push(pt);
        }
    }

    trace!(
        total_transfers = all_transfers.len(),
        "plan_distribution: done"
    );
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
    use crate::domain::file_type::FileType;
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

        let planned =
            plan_distribution(&actions, &simple_graph(), &HashMap::new(), &HashMap::new());

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

        let planned = plan_distribution(&actions, &chain_graph(), &HashMap::new(), &HashMap::new());

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

        let planned = plan_distribution(&actions, &chain_graph(), &pending, &HashMap::new());

        // cloudはpending → skip。podのみ
        assert_eq!(planned.len(), 1);
        assert_eq!(planned[0].dest, pod());
    }

    #[test]
    fn plan_distribution_update_uses_sync_kind() {
        let actions = vec![update_action("f1", local(), cloud())];

        let planned =
            plan_distribution(&actions, &simple_graph(), &HashMap::new(), &HashMap::new());

        assert_eq!(planned.len(), 1);
        assert_eq!(planned[0].kind, TransferKind::Sync);
    }

    #[test]
    fn plan_distribution_multiple_files() {
        let actions = vec![
            send_action("f1", local(), cloud()),
            send_action("f2", local(), cloud()),
        ];

        let planned =
            plan_distribution(&actions, &simple_graph(), &HashMap::new(), &HashMap::new());

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

        let planned = plan_distribution(
            &actions,
            &chain_with_direct_graph(),
            &HashMap::new(),
            &HashMap::new(),
        );

        // optimal: local→pod→cloud (cost 3.0) < local→pod + local→cloud (cost 11.0)
        assert_eq!(planned.len(), 2);
        assert_eq!(planned[0].src, local());
        assert_eq!(planned[0].dest, pod());
        assert_eq!(planned[1].src, pod());
        assert_eq!(planned[1].dest, cloud());
    }

    // =========================================================================
    // plan_distribution — multi-source (existing_presences)
    // =========================================================================

    #[test]
    fn plan_distribution_multi_source_picks_cheaper_relay() {
        // Scenario: file "f1" exists on local (source) AND pod (existing presence).
        // Need to reach cloud. pod→cloud(2.0) is cheaper than local→cloud(5.0).
        //
        // Graph: local→pod(1.0), pod→cloud(2.0), local→cloud(5.0), cloud→local(5.0), cloud→pod(2.0)
        let mut g = RouteGraph::new();
        g.add_with_cost(local(), pod(), EdgeCost::new(1.0, 10));
        g.add_with_cost(pod(), cloud(), EdgeCost::new(2.0, 10));
        g.add_with_cost(local(), cloud(), EdgeCost::new(5.0, 10));
        g.add_with_cost(cloud(), local(), EdgeCost::new(5.0, 10));
        g.add_with_cost(cloud(), pod(), EdgeCost::new(2.0, 10));

        let actions = vec![send_action("f1", local(), cloud())];

        // pod already has f1
        let mut existing = HashMap::new();
        existing.insert("f1".to_string(), HashSet::from([local(), pod()]));

        let planned = plan_distribution(&actions, &g, &HashMap::new(), &existing);

        assert_eq!(planned.len(), 1);
        // Should pick pod→cloud (2.0) instead of local→cloud (5.0)
        assert_eq!(planned[0].src, pod());
        assert_eq!(planned[0].dest, cloud());
    }

    #[test]
    fn plan_distribution_no_existing_presences_uses_source() {
        // Without existing_presences, falls back to source-only routing.
        let mut g = RouteGraph::new();
        g.add_with_cost(local(), pod(), EdgeCost::new(1.0, 10));
        g.add_with_cost(pod(), cloud(), EdgeCost::new(2.0, 10));
        g.add_with_cost(local(), cloud(), EdgeCost::new(5.0, 10));

        let actions = vec![send_action("f1", local(), cloud())];

        let planned = plan_distribution(&actions, &g, &HashMap::new(), &HashMap::new());

        // target is {cloud}. Dijkstra from local:
        // local→pod(1.0)→cloud(3.0) < local→cloud(5.0) → chain path via pod
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

    // =========================================================================
    // plan_distribution — depends_on_index offset across multiple files
    // =========================================================================

    #[test]
    fn plan_distribution_multi_file_chain_depends_on_index_offset() {
        // 2ファイル × chain_graph(local→pod→cloud)
        // f1: local→pod(idx=0), pod→cloud(dep=0)
        // f2: local→pod(idx=2), pod→cloud(dep=2)
        // depends_on_indexがファイル間でずれないことを検証
        let actions = vec![
            send_action("f1", local(), pod()),
            send_action("f1", local(), cloud()),
            send_action("f2", local(), pod()),
            send_action("f2", local(), cloud()),
        ];

        let planned = plan_distribution(&actions, &chain_graph(), &HashMap::new(), &HashMap::new());

        assert_eq!(planned.len(), 4);

        // f1とf2のTransferを分離
        let f1: Vec<_> = planned.iter().filter(|p| p.file_id == "f1").collect();
        let f2: Vec<_> = planned.iter().filter(|p| p.file_id == "f2").collect();
        assert_eq!(f1.len(), 2);
        assert_eq!(f2.len(), 2);

        // 各ファイルのchain: hop1(dep=None), hop2(dep=hop1のグローバルindex)
        let f1_hop1_idx = planned
            .iter()
            .position(|p| p.file_id == "f1" && p.dest == pod())
            .unwrap();
        let f1_hop2 = planned
            .iter()
            .find(|p| p.file_id == "f1" && p.dest == cloud())
            .unwrap();
        assert_eq!(f1_hop2.depends_on_index, Some(f1_hop1_idx));

        let f2_hop1_idx = planned
            .iter()
            .position(|p| p.file_id == "f2" && p.dest == pod())
            .unwrap();
        let f2_hop2 = planned
            .iter()
            .find(|p| p.file_id == "f2" && p.dest == cloud())
            .unwrap();
        assert_eq!(f2_hop2.depends_on_index, Some(f2_hop1_idx));

        // f2のdepends_onがf1のTransferを指していないことを確認
        assert_ne!(f2_hop2.depends_on_index, Some(f1_hop1_idx));
    }
}
