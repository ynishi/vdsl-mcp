//! Phase 2: Distribute — Topology → Location への配布アクション。
//!
//! TopologyFileの状態と各LocationのLocationFileを比較し、
//! 必要な転送アクション（Send/Update/Delete）を生成する。
//! コンフリクト検出も担当する。

use std::collections::{HashMap, HashSet};

use tracing::trace;

use super::digest::CrossLocationIdentity;
use super::file_type::FileType;
use super::fingerprint::FileFingerprint;
use super::location::LocationId;
use super::location_file::LocationFile;
use super::topology_file::TopologyFile;

// =============================================================================
// Types
// =============================================================================

/// 配布アクション。Topology→Locationへの転送指示。
///
/// Phase 2の出力。Transfer計画の入力となる。
#[derive(Debug, Clone)]
pub enum DistributeAction {
    /// Locationにファイルが存在しない → 転送が必要。
    Send(SendAction),
    /// Locationのファイルが古い → 更新転送が必要。
    Update(UpdateAction),
    /// Topology上で削除済み → Locationからも削除。
    #[allow(dead_code)] // Delete設計はテスト済み、production配線は未実装
    Delete(DeleteAction),
}

/// Locationへファイルを新規送信。
#[derive(Debug, Clone)]
pub struct SendAction {
    pub(crate) topology_file_id: String,
    pub(crate) relative_path: String,
    #[allow(dead_code)] // file_type別の同期ルール分岐で使用予定
    pub(crate) file_type: FileType,
    /// 転送先Location。
    pub(crate) target: LocationId,
    /// 転送元として最適なLocation（Topology/RouteGraphから決定）。
    pub(crate) source: LocationId,
}

/// Locationのファイルを最新版に更新。
#[derive(Debug, Clone)]
pub struct UpdateAction {
    pub(crate) topology_file_id: String,
    pub(crate) relative_path: String,
    pub(crate) target: LocationId,
    pub(crate) source: LocationId,
}

/// Locationからファイルを削除。
#[derive(Debug, Clone)]
pub struct DeleteAction {
    pub(crate) topology_file_id: String,
    pub(crate) relative_path: String,
    pub(crate) target: LocationId,
}

// =============================================================================
// Conflict Detection
// =============================================================================

/// 複数Locationで同一ファイルが異なる内容に更新されたコンフリクト。
///
/// ingest_originsに2つ以上のLocationが含まれ、かつそれらのfingerprintが
/// 相互に一致しない場合に生成される。
#[derive(Debug, Clone, serde::Serialize)]
pub struct ConflictEntry {
    pub(crate) topology_file_id: String,
    pub(crate) relative_path: String,
    /// コンフリクトしているLocation群とそのfingerprint。2要素以上。
    pub(crate) variants: Vec<ConflictVariant>,
}

impl ConflictEntry {
    pub fn topology_file_id(&self) -> &str {
        &self.topology_file_id
    }

    pub fn relative_path(&self) -> &str {
        &self.relative_path
    }

    pub fn variants(&self) -> &[ConflictVariant] {
        &self.variants
    }
}

/// コンフリクトの各バリアント。どのLocationがどのfingerprintを持っているか。
#[derive(Debug, Clone, serde::Serialize)]
pub struct ConflictVariant {
    pub(crate) location_id: LocationId,
    pub(crate) fingerprint: FileFingerprint,
}

impl ConflictVariant {
    pub fn location_id(&self) -> &LocationId {
        &self.location_id
    }

    pub fn fingerprint(&self) -> &FileFingerprint {
        &self.fingerprint
    }
}

/// distribute_actionsの戻り値。アクションとコンフリクトを同時に返す。
///
/// コンフリクトがあるファイルについてはUpdate actionを生成せず、
/// 代わりにconflictsにエントリを追加する（Report戦略）。
/// Send/Delete actionはコンフリクトに関係なく生成される。
///
/// # Strategy拡張（未実装）
///
/// 現在はReport戦略のみ実装。将来的にUpdateStrategy enumを導入し、
/// コンフリクト時の挙動を呼び出し元が選択できるようにする。
///
/// ```text
/// enum UpdateStrategy {
///     Report,    // デフォルト。コンフリクトを報告し、Update転送を抑止
///     Overwrite, // 明示的に指定。pick_sourceで選ばれたsourceで上書き
/// }
/// ```
///
/// Overwrite戦略では、distribute_actionsにstrategy引数を追加するのではなく、
/// Application層（SdkImpl等）がconflictsを受け取った後に、
/// 対象ファイルのUpdate actionを再生成して実行する設計を想定。
/// Domain関数は常にReport（検出のみ）を担当する。
#[derive(Debug)]
pub struct DistributeResult {
    pub actions: Vec<DistributeAction>,
    pub conflicts: Vec<ConflictEntry>,
}

impl std::fmt::Display for ConflictEntry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "CONFLICT {} [", self.relative_path)?;
        for (i, v) in self.variants.iter().enumerate() {
            if i > 0 {
                write!(f, ", ")?;
            }
            write!(f, "{}", v.location_id)?;
        }
        write!(f, "]")
    }
}

// =============================================================================
// Accessors — DistributeAction
// =============================================================================

impl DistributeAction {
    pub fn topology_file_id(&self) -> &str {
        match self {
            Self::Send(a) => &a.topology_file_id,
            Self::Update(a) => &a.topology_file_id,
            Self::Delete(a) => &a.topology_file_id,
        }
    }

    #[cfg(test)]
    pub fn relative_path(&self) -> &str {
        match self {
            Self::Send(a) => &a.relative_path,
            Self::Update(a) => &a.relative_path,
            Self::Delete(a) => &a.relative_path,
        }
    }

    pub fn target(&self) -> &LocationId {
        match self {
            Self::Send(a) => &a.target,
            Self::Update(a) => &a.target,
            Self::Delete(a) => &a.target,
        }
    }

    #[cfg(test)]
    pub fn is_send(&self) -> bool {
        matches!(self, Self::Send(_))
    }

    #[cfg(test)]
    pub fn is_update(&self) -> bool {
        matches!(self, Self::Update(_))
    }

    pub fn is_delete(&self) -> bool {
        matches!(self, Self::Delete(_))
    }
}

// =============================================================================
// Display
// =============================================================================

impl std::fmt::Display for DistributeAction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Send(a) => write!(f, "SEND {} → [{}]", a.relative_path, a.target),
            Self::Update(a) => write!(f, "UPDATE {} → [{}]", a.relative_path, a.target),
            Self::Delete(a) => write!(f, "DELETE {} @ [{}]", a.relative_path, a.target),
        }
    }
}

// =============================================================================
// Phase 2: distribute_actions — TopologyFile × LocationFile → DistributeAction[]
// =============================================================================

/// Phase 2: TopologyFileの状態と各LocationのLocationFileを比較し、
/// 必要な配布アクションを生成する。
///
/// # 引数
///
/// - `topology_files` — 配布対象のTopologyFile群（deleted除外済みであること）
/// - `location_files` — `file_id → LocationFile[]` のインデックス。
///   全Locationの全LocationFileを含む。
/// - `target_locations` — 配布先の候補Location一覧
/// - `ingest_origins` — `file_id → このIngestサイクルで変更を検出したLocationId集合`。
///   自分がPUTしたものを自分に送り返さないための除外用。
///
/// # ルール
///
/// 各TopologyFileについて、各target_locationを走査:
/// 1. ingest_origins に含まれる → skip（送り返し防止）
/// 2. LocationFile が存在 + Archived → skip（転送対象外）
/// 3. LocationFile が存在 + Syncing → skip（転送中）
/// 4. LocationFile が存在 + fingerprint一致 → skip（最新）
/// 5. LocationFile が存在 + fingerprint不一致 → Update
/// 6. LocationFile が存在しない → Send
///
/// source の決定: ingest_origins 内のLocationIdから1つ選択。
/// 複数ある場合は最初のもの（ルーティング最適化はRoute層が担当）。
pub fn distribute_actions(
    topology_files: &[&TopologyFile],
    location_files: &HashMap<String, Vec<&LocationFile>>,
    target_locations: &[LocationId],
    ingest_origins: &HashMap<String, HashSet<LocationId>>,
) -> DistributeResult {
    trace!(
        topology_files = topology_files.len(),
        target_locations = target_locations.len(),
        ingest_origins = ingest_origins.len(),
        "distribute_actions: start"
    );
    let mut actions = Vec::new();
    let mut conflicts = Vec::new();
    let empty_origins = HashSet::new();

    for tf in topology_files {
        let origins = ingest_origins.get(tf.id()).unwrap_or(&empty_origins);
        distribute_file(
            tf,
            origins,
            location_files,
            target_locations,
            &mut actions,
            &mut conflicts,
        );
    }

    trace!(
        actions = actions.len(),
        conflicts = conflicts.len(),
        "distribute_actions: done"
    );
    DistributeResult { actions, conflicts }
}

/// 単一TopologyFileに対する配布アクション生成。
///
/// コンフリクト検出 → source決定 → target走査の流れを担当。
fn distribute_file(
    tf: &TopologyFile,
    origins: &HashSet<LocationId>,
    location_files: &HashMap<String, Vec<&LocationFile>>,
    target_locations: &[LocationId],
    actions: &mut Vec<DistributeAction>,
    conflicts: &mut Vec<ConflictEntry>,
) {
    let file_id = tf.id();

    // コンフリクト検出
    let has_conflict = if let Some(entry) =
        detect_conflict(file_id, tf.relative_path(), origins, location_files)
    {
        conflicts.push(entry);
        true
    } else {
        false
    };

    let Some(source) = pick_source(file_id, origins, location_files) else {
        trace!(file_id = %file_id, path = %tf.relative_path(), "no source found, skip");
        return;
    };

    // Domain不変条件: file_id + location_id でLocationFileはユニーク。
    // collectで後勝ちになるが、重複は発生しない前提。
    let empty_lfs: Vec<&LocationFile> = Vec::new();
    let lfs = location_files.get(file_id).unwrap_or(&empty_lfs);
    let lf_by_location: HashMap<&LocationId, &&LocationFile> =
        lfs.iter().map(|lf| (lf.location_id(), lf)).collect();

    let latest_fp = latest_fingerprint(file_id, origins, location_files);

    for target in target_locations {
        if origins.contains(target) || target == &source {
            continue;
        }
        emit_action_for_target(
            tf,
            target,
            &source,
            &lf_by_location,
            latest_fp.as_ref(),
            has_conflict,
            actions,
        );
    }
}

/// target 1つに対するアクション判定・生成。
fn emit_action_for_target(
    tf: &TopologyFile,
    target: &LocationId,
    source: &LocationId,
    lf_by_location: &HashMap<&LocationId, &&LocationFile>,
    latest_fp: Option<&FileFingerprint>,
    has_conflict: bool,
    actions: &mut Vec<DistributeAction>,
) {
    let file_id = tf.id();
    match lf_by_location.get(target) {
        Some(lf) => {
            if has_conflict {
                return; // コンフリクト時はUpdate抑止
            }
            if !lf.state().is_distribute_target() {
                return; // Archived → skip
            }
            if lf.state() == super::location_file::LocationFileState::Syncing {
                return; // Syncing → skip
            }
            // Missing → ファイルが消失したLocation。fp比較をバイパスして再送。
            if lf.state() == super::location_file::LocationFileState::Missing {
                trace!(file_id = %file_id, target = %target, source = %source, "Update (missing)");
                actions.push(DistributeAction::Update(UpdateAction {
                    topology_file_id: file_id.to_string(),
                    relative_path: tf.relative_path().to_string(),
                    target: target.clone(),
                    source: source.clone(),
                }));
                return;
            }
            let Some(fp) = latest_fp else {
                trace!(file_id = %file_id, target = %target, "no latest fingerprint, skip Update");
                return;
            };
            // cross-location比較: ByteDigestを構造的に除外するCrossLocationIdentityを使用
            let target_id = CrossLocationIdentity::from_fingerprint(lf.fingerprint());
            let latest_id = CrossLocationIdentity::from_fingerprint(fp);
            if !latest_id.matches(&target_id) {
                trace!(file_id = %file_id, target = %target, source = %source, "Update");
                actions.push(DistributeAction::Update(UpdateAction {
                    topology_file_id: file_id.to_string(),
                    relative_path: tf.relative_path().to_string(),
                    target: target.clone(),
                    source: source.clone(),
                }));
            }
        }
        None => {
            trace!(file_id = %file_id, target = %target, source = %source, "Send");
            actions.push(DistributeAction::Send(SendAction {
                topology_file_id: file_id.to_string(),
                relative_path: tf.relative_path().to_string(),
                file_type: tf.file_type(),
                target: target.clone(),
                source: source.clone(),
            }));
        }
    }
}

/// 削除済みTopologyFileに対するDelete配布アクションを生成する。
///
/// # 引数
///
/// - `deleted_topology_files` — 削除済みのTopologyFile群
/// - `location_files` — `file_id → LocationFile[]`
/// - `target_locations` — 配布先候補
#[cfg(test)]
pub fn distribute_delete_actions(
    deleted_topology_files: &[&TopologyFile],
    location_files: &HashMap<String, Vec<&LocationFile>>,
    target_locations: &[LocationId],
) -> Vec<DistributeAction> {
    let mut actions = Vec::new();

    for tf in deleted_topology_files {
        let file_id = tf.id();
        let empty_lfs: Vec<&LocationFile> = Vec::new();
        let lfs = location_files.get(file_id).unwrap_or(&empty_lfs);
        let lf_locations: HashSet<&LocationId> = lfs.iter().map(|lf| lf.location_id()).collect();

        for target in target_locations {
            // LocationFileが存在するLocationにのみDeleteを発行
            if lf_locations.contains(target) {
                actions.push(DistributeAction::Delete(DeleteAction {
                    topology_file_id: file_id.to_string(),
                    relative_path: tf.relative_path().to_string(),
                    target: target.clone(),
                }));
            }
        }
    }

    actions
}

/// 複数originsのfingerprint不一致を検出する。
///
/// originsが2つ以上あり、それらのLocationFileのfingerprintが相互に一致しない場合、
/// ConflictEntryを返す。originsが0〜1個、または全fingerprint一致なら None。
fn detect_conflict(
    file_id: &str,
    relative_path: &str,
    origins: &HashSet<LocationId>,
    location_files: &HashMap<String, Vec<&LocationFile>>,
) -> Option<ConflictEntry> {
    if origins.len() < 2 {
        return None;
    }

    let lfs = location_files.get(file_id)?;

    // originsに対応するLocationFileのfingerprintを収集
    let mut variants: Vec<ConflictVariant> = Vec::new();
    for origin in origins {
        if let Some(lf) = lfs.iter().find(|lf| lf.location_id() == origin) {
            variants.push(ConflictVariant {
                location_id: origin.clone(),
                fingerprint: lf.fingerprint().clone(),
            });
        }
    }

    if variants.len() < 2 {
        return None;
    }

    // 全fingerprint一致チェック: CrossLocationIdentityで比較
    // （cross-location比較のため ByteDigest ではなく ContentDigest/size で判定）
    let base_identity = CrossLocationIdentity::from_fingerprint(&variants[0].fingerprint);
    let all_match = variants[1..]
        .iter()
        .all(|v| base_identity.matches(&CrossLocationIdentity::from_fingerprint(&v.fingerprint)));
    if all_match {
        return None;
    }

    Some(ConflictEntry {
        topology_file_id: file_id.to_string(),
        relative_path: relative_path.to_string(),
        variants,
    })
}

/// source Location を決定する。
///
/// 1. ingest_origins から辞書順最小のLocationIdを選択（決定的）
/// 2. fallback: Active状態のLocationFileを持つLocationから辞書順最小
fn pick_source(
    file_id: &str,
    origins: &HashSet<LocationId>,
    location_files: &HashMap<String, Vec<&LocationFile>>,
) -> Option<LocationId> {
    // 1. Ingest originから辞書順最小を選択（決定的な結果を保証）
    if let Some(origin) = origins.iter().min() {
        return Some(origin.clone());
    }
    // 2. Active LocationFileを持つLocationから辞書順最小をfallback
    if let Some(lfs) = location_files.get(file_id) {
        let mut candidates: Vec<&LocationId> = lfs
            .iter()
            .filter(|lf| lf.state().is_source_eligible())
            .map(|lf| lf.location_id())
            .collect();
        candidates.sort();
        return candidates.first().map(|id| (*id).clone());
    }
    None
}

/// ingest_originsのLocationが持つ最新fingerprintを取得する。
///
/// originsの辞書順最小LocationのLocationFileからfingerprintを取る。
/// fallback: Active状態のLocationFileのfingerprint（辞書順最小）。
/// 該当なしの場合は None を返す（呼び出し元でUpdate skipの判断に使用）。
fn latest_fingerprint(
    file_id: &str,
    origins: &HashSet<LocationId>,
    location_files: &HashMap<String, Vec<&LocationFile>>,
) -> Option<FileFingerprint> {
    let lfs = location_files.get(file_id)?;

    // originsの辞書順最小Locationのfingerprintを優先
    let mut sorted_origins: Vec<&LocationId> = origins.iter().collect();
    sorted_origins.sort();
    for origin in &sorted_origins {
        if let Some(lf) = lfs.iter().find(|lf| lf.location_id() == *origin) {
            return Some(lf.fingerprint().clone());
        }
    }
    // fallback: Active状態のLocationFile（辞書順最小）
    let mut active: Vec<&LocationFile> = lfs
        .iter()
        .copied()
        .filter(|lf| lf.state().is_source_eligible())
        .collect();
    active.sort_by_key(|lf| lf.location_id());
    active.first().map(|lf| lf.fingerprint().clone())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::test_helpers::{cloud, local, local_fp, pod};
    use crate::domain::topology_file::TopologyFile;
    use std::collections::{HashMap, HashSet};

    // =========================================================================
    // DistributeAction — accessors
    // =========================================================================

    #[test]
    fn send_action_accessors() {
        let action = DistributeAction::Send(SendAction {
            topology_file_id: "tf-1".into(),
            relative_path: "output/001.png".into(),
            file_type: FileType::Image,
            target: pod(),
            source: local(),
        });
        assert_eq!(action.topology_file_id(), "tf-1");
        assert_eq!(action.relative_path(), "output/001.png");
        assert_eq!(action.target(), &pod());
        assert!(action.is_send());
        assert!(!action.is_update());
        assert!(!action.is_delete());
    }

    #[test]
    fn update_action_accessors() {
        let action = DistributeAction::Update(UpdateAction {
            topology_file_id: "tf-1".into(),
            relative_path: "output/002.png".into(),
            target: cloud(),
            source: local(),
        });
        assert!(action.is_update());
    }

    #[test]
    fn delete_action_accessors() {
        let action = DistributeAction::Delete(DeleteAction {
            topology_file_id: "tf-1".into(),
            relative_path: "output/gone.png".into(),
            target: pod(),
        });
        assert!(action.is_delete());
        assert_eq!(action.target(), &pod());
    }

    // =========================================================================
    // Display — DistributeAction
    // =========================================================================

    #[test]
    fn display_send_action() {
        let action = DistributeAction::Send(SendAction {
            topology_file_id: "tf-1".into(),
            relative_path: "a.png".into(),
            file_type: FileType::Image,
            target: pod(),
            source: local(),
        });
        assert!(action.to_string().starts_with("SEND"));
    }

    #[test]
    fn display_delete_action() {
        let action = DistributeAction::Delete(DeleteAction {
            topology_file_id: "tf-1".into(),
            relative_path: "a.png".into(),
            target: pod(),
        });
        assert!(action.to_string().starts_with("DELETE"));
    }

    // =========================================================================
    // distribute_actions — Phase 2
    // =========================================================================

    /// テスト用: TopologyFileとLocationFileを一括で作成するヘルパー
    fn make_tf(path: &str) -> TopologyFile {
        TopologyFile::new(path.to_string(), FileType::Image).unwrap()
    }

    fn make_lf(file_id: &str, location: &LocationId, fp: &FileFingerprint) -> LocationFile {
        LocationFile::new(
            file_id.to_string(),
            location.clone(),
            "dummy.png".to_string(),
            fp.clone(),
            None,
        )
        .unwrap()
    }

    fn make_lf_with_state(
        file_id: &str,
        location: &LocationId,
        fp: &FileFingerprint,
        state: crate::domain::location_file::LocationFileState,
    ) -> LocationFile {
        use chrono::Utc;
        LocationFile::reconstitute(
            file_id.to_string(),
            location.clone(),
            "dummy.png".to_string(),
            fp.clone(),
            state,
            None,
            Utc::now(),
        )
    }

    // -------------------------------------------------------------------------
    // Send: LocationFileが存在しない → Send
    // -------------------------------------------------------------------------

    #[test]
    fn distribute_sends_to_location_without_file() {
        let tf = make_tf("output/001.png");
        let lf_local = make_lf(tf.id(), &local(), &local_fp("h1", 1024));

        let mut location_files: HashMap<String, Vec<&LocationFile>> = HashMap::new();
        location_files.insert(tf.id().to_string(), vec![&lf_local]);

        let mut origins = HashMap::new();
        origins.insert(tf.id().to_string(), HashSet::from([local()]));

        let result = distribute_actions(
            &[&tf],
            &location_files,
            &[local(), pod(), cloud()],
            &origins,
        );

        // pod, cloudにSend（localはorigin → skip）
        assert_eq!(result.actions.len(), 2);
        assert!(result.actions.iter().all(|a| a.is_send()));
        let targets: HashSet<_> = result.actions.iter().map(|a| a.target().clone()).collect();
        assert!(targets.contains(&pod()));
        assert!(targets.contains(&cloud()));
        assert!(result.conflicts.is_empty());
    }

    // -------------------------------------------------------------------------
    // Update: LocationFileが存在 + fingerprint不一致 → Update
    // -------------------------------------------------------------------------

    #[test]
    fn distribute_updates_stale_location() {
        let tf = make_tf("output/001.png");
        let lf_local = make_lf(tf.id(), &local(), &local_fp("new_hash", 2048));
        let lf_pod = make_lf(tf.id(), &pod(), &local_fp("old_hash", 1024));

        let mut location_files: HashMap<String, Vec<&LocationFile>> = HashMap::new();
        location_files.insert(tf.id().to_string(), vec![&lf_local, &lf_pod]);

        let mut origins = HashMap::new();
        origins.insert(tf.id().to_string(), HashSet::from([local()]));

        let result = distribute_actions(&[&tf], &location_files, &[local(), pod()], &origins);

        // podにUpdate（fingerprint不一致）
        assert_eq!(result.actions.len(), 1);
        assert!(result.actions[0].is_update());
        assert_eq!(result.actions[0].target(), &pod());
        assert!(result.conflicts.is_empty());
    }

    // -------------------------------------------------------------------------
    // Skip: fingerprint一致 → 最新なのでskip
    // -------------------------------------------------------------------------

    #[test]
    fn distribute_skips_up_to_date_location() {
        let tf = make_tf("output/001.png");
        let fp = local_fp("same_hash", 1024);
        let lf_local = make_lf(tf.id(), &local(), &fp);
        let lf_pod = make_lf(tf.id(), &pod(), &fp);

        let mut location_files: HashMap<String, Vec<&LocationFile>> = HashMap::new();
        location_files.insert(tf.id().to_string(), vec![&lf_local, &lf_pod]);

        let mut origins = HashMap::new();
        origins.insert(tf.id().to_string(), HashSet::from([local()]));

        let result = distribute_actions(&[&tf], &location_files, &[local(), pod()], &origins);

        assert_eq!(result.actions.len(), 0, "fingerprint一致 → skip");
        assert!(result.conflicts.is_empty());
    }

    // -------------------------------------------------------------------------
    // Skip: Archived → 転送対象外
    // -------------------------------------------------------------------------

    #[test]
    fn distribute_skips_archived_location() {
        use crate::domain::location_file::LocationFileState;
        let tf = make_tf("output/001.png");
        let lf_local = make_lf(tf.id(), &local(), &local_fp("new", 2048));
        let lf_pod = make_lf_with_state(
            tf.id(),
            &pod(),
            &local_fp("old", 1024),
            LocationFileState::Archived,
        );

        let mut location_files: HashMap<String, Vec<&LocationFile>> = HashMap::new();
        location_files.insert(tf.id().to_string(), vec![&lf_local, &lf_pod]);

        let mut origins = HashMap::new();
        origins.insert(tf.id().to_string(), HashSet::from([local()]));

        let result = distribute_actions(&[&tf], &location_files, &[local(), pod()], &origins);

        assert_eq!(result.actions.len(), 0, "Archived → skip");
    }

    // -------------------------------------------------------------------------
    // Skip: Syncing → 転送中なのでskip
    // -------------------------------------------------------------------------

    #[test]
    fn distribute_skips_syncing_location() {
        use crate::domain::location_file::LocationFileState;
        let tf = make_tf("output/001.png");
        let lf_local = make_lf(tf.id(), &local(), &local_fp("new", 2048));
        let lf_pod = make_lf_with_state(
            tf.id(),
            &pod(),
            &local_fp("old", 1024),
            LocationFileState::Syncing,
        );

        let mut location_files: HashMap<String, Vec<&LocationFile>> = HashMap::new();
        location_files.insert(tf.id().to_string(), vec![&lf_local, &lf_pod]);

        let mut origins = HashMap::new();
        origins.insert(tf.id().to_string(), HashSet::from([local()]));

        let result = distribute_actions(&[&tf], &location_files, &[local(), pod()], &origins);

        assert_eq!(result.actions.len(), 0, "Syncing → skip");
    }

    // -------------------------------------------------------------------------
    // Skip: ingest origin → 送り返し防止
    // -------------------------------------------------------------------------

    #[test]
    fn distribute_excludes_ingest_origin() {
        let tf = make_tf("output/001.png");
        let lf_local = make_lf(tf.id(), &local(), &local_fp("h1", 1024));
        let lf_pod = make_lf(tf.id(), &pod(), &local_fp("h1", 1024));

        let mut location_files: HashMap<String, Vec<&LocationFile>> = HashMap::new();
        location_files.insert(tf.id().to_string(), vec![&lf_local, &lf_pod]);

        // local と pod 両方がorigin — fingerprint一致なのでコンフリクトではない
        let mut origins = HashMap::new();
        origins.insert(tf.id().to_string(), HashSet::from([local(), pod()]));

        let result = distribute_actions(
            &[&tf],
            &location_files,
            &[local(), pod(), cloud()],
            &origins,
        );

        // local, podはorigin → skip。cloudにはLocationFileなし → Send
        assert_eq!(result.actions.len(), 1);
        assert!(result.actions[0].is_send());
        assert_eq!(result.actions[0].target(), &cloud());
        assert!(result.conflicts.is_empty());
    }

    // -------------------------------------------------------------------------
    // source fallback: originがない場合、Active LocationFileのLocationを使う
    // -------------------------------------------------------------------------

    #[test]
    fn distribute_picks_active_source_when_no_origin() {
        let tf = make_tf("output/001.png");
        let lf_local = make_lf(tf.id(), &local(), &local_fp("h1", 1024));

        let mut location_files: HashMap<String, Vec<&LocationFile>> = HashMap::new();
        location_files.insert(tf.id().to_string(), vec![&lf_local]);

        // originなし（既存ファイルの再配布等）
        let origins: HashMap<String, HashSet<LocationId>> = HashMap::new();

        let result = distribute_actions(&[&tf], &location_files, &[local(), pod()], &origins);

        // source=local（Active LocationFileから）、target=pod → Send
        assert_eq!(result.actions.len(), 1);
        assert!(result.actions[0].is_send());
        assert_eq!(result.actions[0].target(), &pod());
    }

    // -------------------------------------------------------------------------
    // Conflict: 複数originでfingerprint不一致 → コンフリクト検出
    // -------------------------------------------------------------------------

    #[test]
    fn distribute_detects_conflict_when_origins_have_different_fingerprints() {
        let tf = make_tf("output/001.png");
        // local と pod で異なるfingerprintを持つ
        let lf_local = make_lf(tf.id(), &local(), &local_fp("hash_local", 1024));
        let lf_pod = make_lf(tf.id(), &pod(), &local_fp("hash_pod", 2048));

        let mut location_files: HashMap<String, Vec<&LocationFile>> = HashMap::new();
        location_files.insert(tf.id().to_string(), vec![&lf_local, &lf_pod]);

        // 両方がorigin（独立に更新された）
        let mut origins = HashMap::new();
        origins.insert(tf.id().to_string(), HashSet::from([local(), pod()]));

        let result = distribute_actions(
            &[&tf],
            &location_files,
            &[local(), pod(), cloud()],
            &origins,
        );

        // コンフリクト検出: Update action は生成されない
        assert_eq!(result.conflicts.len(), 1);
        let conflict = &result.conflicts[0];
        assert_eq!(conflict.topology_file_id(), tf.id());
        assert_eq!(conflict.relative_path(), "output/001.png");
        assert_eq!(conflict.variants().len(), 2);

        // cloudにはLocationFileがないのでSendは生成される
        assert_eq!(result.actions.len(), 1);
        assert!(result.actions[0].is_send());
        assert_eq!(result.actions[0].target(), &cloud());
    }

    #[test]
    fn distribute_no_conflict_when_origins_have_same_fingerprint() {
        let tf = make_tf("output/001.png");
        let fp = local_fp("same_hash", 1024);
        let lf_local = make_lf(tf.id(), &local(), &fp);
        let lf_pod = make_lf(tf.id(), &pod(), &fp);

        let mut location_files: HashMap<String, Vec<&LocationFile>> = HashMap::new();
        location_files.insert(tf.id().to_string(), vec![&lf_local, &lf_pod]);

        // 両方がorigin だが fingerprint一致 → コンフリクトではない
        let mut origins = HashMap::new();
        origins.insert(tf.id().to_string(), HashSet::from([local(), pod()]));

        let result = distribute_actions(
            &[&tf],
            &location_files,
            &[local(), pod(), cloud()],
            &origins,
        );

        assert!(result.conflicts.is_empty());
        // cloudにはLocationFileなし → Send
        assert_eq!(result.actions.len(), 1);
        assert!(result.actions[0].is_send());
        assert_eq!(result.actions[0].target(), &cloud());
    }

    #[test]
    fn distribute_conflict_skips_update_but_allows_send() {
        let tf = make_tf("output/001.png");
        // local, pod, cloud の3 Locations。local と pod で独立更新。cloudには未到達。
        let lf_local = make_lf(tf.id(), &local(), &local_fp("v_local", 1024));
        let lf_pod = make_lf(tf.id(), &pod(), &local_fp("v_pod", 2048));
        // cloudにはLocationFile不在

        let mut location_files: HashMap<String, Vec<&LocationFile>> = HashMap::new();
        location_files.insert(tf.id().to_string(), vec![&lf_local, &lf_pod]);

        let mut origins = HashMap::new();
        origins.insert(tf.id().to_string(), HashSet::from([local(), pod()]));

        let result = distribute_actions(
            &[&tf],
            &location_files,
            &[local(), pod(), cloud()],
            &origins,
        );

        // コンフリクト: local ↔ pod 間のUpdate は抑止
        assert_eq!(result.conflicts.len(), 1);
        // cloudへのSendは生成される（コンフリクトとは無関係の新規送信）
        assert_eq!(result.actions.len(), 1);
        assert!(result.actions[0].is_send());
        assert_eq!(result.actions[0].target(), &cloud());
    }

    // -------------------------------------------------------------------------
    // distribute_delete_actions
    // -------------------------------------------------------------------------

    #[test]
    fn distribute_delete_targets_existing_locations() {
        let mut tf = make_tf("output/001.png");
        tf.mark_deleted();

        let lf_local = make_lf(tf.id(), &local(), &local_fp("h1", 1024));
        let lf_pod = make_lf(tf.id(), &pod(), &local_fp("h1", 1024));

        let mut location_files: HashMap<String, Vec<&LocationFile>> = HashMap::new();
        location_files.insert(tf.id().to_string(), vec![&lf_local, &lf_pod]);

        let actions =
            distribute_delete_actions(&[&tf], &location_files, &[local(), pod(), cloud()]);

        // local, podにDelete。cloudにはLocationFileなし → skip
        assert_eq!(actions.len(), 2);
        assert!(actions.iter().all(|a| a.is_delete()));
        let targets: HashSet<_> = actions.iter().map(|a| a.target().clone()).collect();
        assert!(targets.contains(&local()));
        assert!(targets.contains(&pod()));
        assert!(!targets.contains(&cloud()));
    }

    #[test]
    fn distribute_delete_skips_location_without_file() {
        let mut tf = make_tf("output/001.png");
        tf.mark_deleted();

        let location_files: HashMap<String, Vec<&LocationFile>> = HashMap::new();

        let actions = distribute_delete_actions(&[&tf], &location_files, &[local(), pod()]);

        assert_eq!(actions.len(), 0, "LocationFileなし → Deleteなし");
    }
}
