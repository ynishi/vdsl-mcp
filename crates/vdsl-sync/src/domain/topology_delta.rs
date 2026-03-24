// Phase 1/2 のドメイン型定義。Apply/Distribute実装時にフィールドを使用する。
#![allow(dead_code)]

//! TopologyDelta — Topology視点のファイル変化。
//!
//! Phase 1 (Ingest): 各Locationのスキャン結果をTopologyに集約する際の差分。
//! L1→T, L2→T, L3→T ... と順にIngestし、Topologyの状態を更新する。
//!
//! Phase 2 (Distribute): 更新済みTopologyから各Locationへ配布する際は、
//! T→Li で「Liが持っていないもの」を転送計画に変換する。
//! ただし Li 自身がIngestしたものは除外する（自分のPUTを自分に送り返さない）。
//!
//! # フロー
//!
//! ```text
//! Scan(Li) → ScannedEntry[]
//!     ↓
//! ingest_deltas(scanned, topology_files, location_files_at_i)
//!     ↓
//! TopologyDelta[] ← Phase 1 出力
//!     ↓
//! apply_ingest(deltas) → Topology更新（TopologyFile/LocationFile作成・更新）
//!     ↓
//! distribute_deltas(topology_files, location_files_at_j, ingested_by_j)
//!     ↓
//! DistributeAction[] ← Phase 2 出力 → Transfer計画へ
//! ```

use std::collections::{HashMap, HashSet};

use tracing::trace;

use super::digest::CrossLocationIdentity;
use super::file_type::FileType;
use super::fingerprint::FileFingerprint;
use super::location::LocationId;
use super::location_file::LocationFile;
use super::topology_file::{ScanMatch, TopologyFile};

// =============================================================================
// Phase 1: Ingest — Location → Topology
// =============================================================================

/// Topology視点の差分。Locationスキャン結果からTopologyへの変化を表す。
///
/// Ingestフェーズの出力。各バリアントはスキャン時点で確定した事実のみを保持する。
#[derive(Debug, Clone)]
pub enum TopologyDelta {
    /// 新規ファイル検出。TopologyFile + LocationFile を新規作成する。
    Discovered(DiscoveredFile),
    /// 既知ファイルのコンテンツ変更。LocationFile更新 + 他Locationへ伝搬対象。
    ContentChanged(ContentChangedFile),
    /// canonical_hashマッチ + path不一致 → rename検出。
    /// TopologyFile.update_path + LocationFile更新。
    Renamed(RenamedFile),
    /// スキャン対象Locationから消失。
    /// 全Locationで消失した場合にTopologyFile.mark_deleted候補となる。
    Vanished(VanishedFile),
}

/// 新規ファイル。Topologyに未登録のファイルが検出された。
#[derive(Debug, Clone)]
pub struct DiscoveredFile {
    /// スキャン時に生成した仮ID（UUID v4）。Apply時にTopologyFile.idとなる。
    pub(crate) id: String,
    pub(crate) relative_path: String,
    pub(crate) file_type: FileType,
    pub(crate) fingerprint: FileFingerprint,
    /// ファイルが検出されたLocation。Phase 2でこのLocationへの配布を除外する起点。
    pub(crate) origin: LocationId,
    pub(crate) embedded_id: Option<String>,
}

/// 既知ファイルのコンテンツ変更。
#[derive(Debug, Clone)]
pub struct ContentChangedFile {
    /// TopologyFile上の既存ID。
    pub(crate) topology_file_id: String,
    pub(crate) relative_path: String,
    pub(crate) file_type: FileType,
    pub(crate) old_fingerprint: FileFingerprint,
    pub(crate) new_fingerprint: FileFingerprint,
    /// 変更が検出されたLocation。
    pub(crate) origin: LocationId,
    pub(crate) embedded_id: Option<String>,
}

/// rename検出。canonical_hash一致 + path不一致。
#[derive(Debug, Clone)]
pub struct RenamedFile {
    /// TopologyFile上の既存ID。
    pub(crate) topology_file_id: String,
    pub(crate) old_path: String,
    pub(crate) new_path: String,
    pub(crate) file_type: FileType,
    pub(crate) fingerprint: FileFingerprint,
    /// renameが検出されたLocation。
    pub(crate) origin: LocationId,
    pub(crate) embedded_id: Option<String>,
}

/// ファイル消失。スキャン対象Locationにファイルが存在しない。
#[derive(Debug, Clone)]
pub struct VanishedFile {
    /// TopologyFile上の既存ID。
    pub(crate) topology_file_id: String,
    pub(crate) relative_path: String,
    /// 消失が検出されたLocation。
    pub(crate) origin: LocationId,
}

// =============================================================================
// Phase 2: Distribute — Topology → Location
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
    Delete(DeleteAction),
}

/// Locationへファイルを新規送信。
#[derive(Debug, Clone)]
pub struct SendAction {
    pub(crate) topology_file_id: String,
    pub(crate) relative_path: String,
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
// Accessors — TopologyDelta
// =============================================================================

impl TopologyDelta {
    pub fn relative_path(&self) -> &str {
        match self {
            Self::Discovered(f) => &f.relative_path,
            Self::ContentChanged(f) => &f.relative_path,
            Self::Renamed(f) => &f.new_path,
            Self::Vanished(f) => &f.relative_path,
        }
    }

    pub fn origin(&self) -> &LocationId {
        match self {
            Self::Discovered(f) => &f.origin,
            Self::ContentChanged(f) => &f.origin,
            Self::Renamed(f) => &f.origin,
            Self::Vanished(f) => &f.origin,
        }
    }

    pub fn topology_file_id(&self) -> &str {
        match self {
            Self::Discovered(f) => &f.id,
            Self::ContentChanged(f) => &f.topology_file_id,
            Self::Renamed(f) => &f.topology_file_id,
            Self::Vanished(f) => &f.topology_file_id,
        }
    }

    pub fn is_discovered(&self) -> bool {
        matches!(self, Self::Discovered(_))
    }

    pub fn is_content_changed(&self) -> bool {
        matches!(self, Self::ContentChanged(_))
    }

    pub fn is_renamed(&self) -> bool {
        matches!(self, Self::Renamed(_))
    }

    pub fn is_vanished(&self) -> bool {
        matches!(self, Self::Vanished(_))
    }

    /// ScanMatchからTopologyDeltaを構築するファクトリ。
    ///
    /// # 引数
    ///
    /// - `scan_match` — TopologyFile.matches_scan()の結果
    /// - `topology_file` — マッチしたTopologyFile（NoMatchの場合はNone）
    /// - `location_fingerprint` — このLocationでの既存fingerprint（None = 未登録）
    /// - `scan_path` — スキャン結果のrelative_path
    /// - `scan_fingerprint` — スキャン結果のfingerprint
    /// - `scan_file_type` — スキャン結果のfile_type
    /// - `scan_origin` — スキャンを実行したLocation
    /// - `scan_embedded_id` — スキャン結果のembedded_id
    #[allow(clippy::too_many_arguments)]
    pub fn from_scan_match(
        scan_match: ScanMatch,
        topology_file: Option<&super::topology_file::TopologyFile>,
        location_fingerprint: Option<&FileFingerprint>,
        scan_path: &str,
        scan_fingerprint: &FileFingerprint,
        scan_file_type: FileType,
        scan_origin: &LocationId,
        scan_embedded_id: Option<String>,
    ) -> Option<Self> {
        match scan_match {
            ScanMatch::NoMatch => {
                // 新規ファイル
                Some(Self::Discovered(DiscoveredFile {
                    id: uuid::Uuid::new_v4().to_string(),
                    relative_path: scan_path.to_string(),
                    file_type: scan_file_type,
                    fingerprint: scan_fingerprint.clone(),
                    origin: scan_origin.clone(),
                    embedded_id: scan_embedded_id,
                }))
            }
            ScanMatch::ByHash => {
                // canonical_hash一致 + path不一致 = rename
                let tf = topology_file?;
                if tf.relative_path() != scan_path {
                    Some(Self::Renamed(RenamedFile {
                        topology_file_id: tf.id().to_string(),
                        old_path: tf.relative_path().to_string(),
                        new_path: scan_path.to_string(),
                        file_type: scan_file_type,
                        fingerprint: scan_fingerprint.clone(),
                        origin: scan_origin.clone(),
                        embedded_id: scan_embedded_id,
                    }))
                } else {
                    // hash一致 + path一致 → コンテンツ変更チェック
                    check_content_change(
                        tf,
                        location_fingerprint,
                        scan_path,
                        scan_fingerprint,
                        scan_file_type,
                        scan_origin,
                        scan_embedded_id,
                    )
                }
            }
            ScanMatch::ByPath => {
                // path一致 → コンテンツ変更チェック
                let tf = topology_file?;
                check_content_change(
                    tf,
                    location_fingerprint,
                    scan_path,
                    scan_fingerprint,
                    scan_file_type,
                    scan_origin,
                    scan_embedded_id,
                )
            }
        }
    }
}

/// LocationFileのfingerprint比較でコンテンツ変更を検出する。
///
/// location_fingerprintがNone（このLocationに未登録）の場合、
/// 新規追加としてDiscoveredを返す。
fn check_content_change(
    topology_file: &super::topology_file::TopologyFile,
    location_fingerprint: Option<&FileFingerprint>,
    scan_path: &str,
    scan_fingerprint: &FileFingerprint,
    scan_file_type: FileType,
    scan_origin: &LocationId,
    scan_embedded_id: Option<String>,
) -> Option<TopologyDelta> {
    match location_fingerprint {
        None => {
            // このLocationに未登録 → path一致だがLocationFileがない
            // TopologyFileは存在するがこのLocationでは初めて → ContentChanged
            // （他Locationで先にDiscoveredされてTopologyに登録済みのケース）
            Some(TopologyDelta::ContentChanged(ContentChangedFile {
                topology_file_id: topology_file.id().to_string(),
                relative_path: scan_path.to_string(),
                file_type: scan_file_type,
                old_fingerprint: FileFingerprint {
                    byte_digest: None,
                    content_digest: None,
                    meta_digest: None,
                    size: 0,
                    modified_at: None,
                },
                new_fingerprint: scan_fingerprint.clone(),
                origin: scan_origin.clone(),
                embedded_id: scan_embedded_id,
            }))
        }
        Some(existing_fp) => {
            if existing_fp.matches_within_location(scan_fingerprint) {
                // fingerprint一致 → 変更なし
                None
            } else {
                Some(TopologyDelta::ContentChanged(ContentChangedFile {
                    topology_file_id: topology_file.id().to_string(),
                    relative_path: scan_path.to_string(),
                    file_type: scan_file_type,
                    old_fingerprint: existing_fp.clone(),
                    new_fingerprint: scan_fingerprint.clone(),
                    origin: scan_origin.clone(),
                    embedded_id: scan_embedded_id,
                }))
            }
        }
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

    pub fn is_send(&self) -> bool {
        matches!(self, Self::Send(_))
    }

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

impl std::fmt::Display for TopologyDelta {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Discovered(d) => write!(f, "+{} [{}]", d.relative_path, d.origin),
            Self::ContentChanged(c) => write!(f, "~{} [{}]", c.relative_path, c.origin),
            Self::Renamed(r) => {
                write!(f, ">{} → {} [{}]", r.old_path, r.new_path, r.origin)
            }
            Self::Vanished(v) => write!(f, "-{} [{}]", v.relative_path, v.origin),
        }
    }
}

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

    for tf in topology_files {
        let file_id = tf.id();
        let empty_origins = HashSet::new();
        let origins = ingest_origins.get(file_id).unwrap_or(&empty_origins);

        // コンフリクト検出: originsが2つ以上あり、fingerprint群が一致しない場合
        let conflict = detect_conflict(file_id, tf.relative_path(), origins, location_files);
        if let Some(entry) = conflict {
            conflicts.push(entry);
            // コンフリクト時はUpdate actionを生成せず、Send/Deleteのみ
            // （新規Locationへの送信は影響を受けない）
            let source = pick_source(file_id, origins, location_files);
            let Some(source) = source else { continue };

            let empty_lfs: Vec<&LocationFile> = Vec::new();
            let lfs = location_files.get(file_id).unwrap_or(&empty_lfs);
            let lf_by_location: HashMap<&LocationId, &&LocationFile> =
                lfs.iter().map(|lf| (lf.location_id(), lf)).collect();

            for target in target_locations {
                if origins.contains(target) || target == &source {
                    continue;
                }
                // コンフリクト時はLocationFile不在(Send)のみ生成。Update はskip
                if !lf_by_location.contains_key(target) {
                    actions.push(DistributeAction::Send(SendAction {
                        topology_file_id: file_id.to_string(),
                        relative_path: tf.relative_path().to_string(),
                        file_type: tf.file_type(),
                        target: target.clone(),
                        source: source.clone(),
                    }));
                }
            }
            continue;
        }

        // source決定: originsから取得。originsが空なら
        // LocationFile(Active)を持つ任意のLocationをfallback
        let source = pick_source(file_id, origins, location_files);
        let Some(source) = source else {
            // sourceが特定できない = どこにもActiveな実体がない → skip
            trace!(
                file_id = %file_id,
                path = %tf.relative_path(),
                origins = ?origins.iter().map(|o| o.to_string()).collect::<Vec<_>>(),
                "distribute_actions: no source found, skip"
            );
            continue;
        };
        trace!(
            file_id = %file_id,
            path = %tf.relative_path(),
            source = %source,
            origins = ?origins.iter().map(|o| o.to_string()).collect::<Vec<_>>(),
            "distribute_actions: processing file"
        );

        // このファイルの全LocationFileをlocation_idで引けるようにする
        let empty_lfs: Vec<&LocationFile> = Vec::new();
        let lfs = location_files.get(file_id).unwrap_or(&empty_lfs);
        let lf_by_location: HashMap<&LocationId, &&LocationFile> =
            lfs.iter().map(|lf| (lf.location_id(), lf)).collect();

        for target in target_locations {
            // 自分がIngestしたものは送り返さない
            if origins.contains(target) {
                continue;
            }
            // sourceと同じLocationには送らない
            if target == &source {
                continue;
            }

            match lf_by_location.get(target) {
                Some(lf) => {
                    // Archived → skip
                    if !lf.state().is_distribute_target() {
                        continue;
                    }
                    // Syncing → skip（転送中）
                    if lf.state() == super::location_file::LocationFileState::Syncing {
                        continue;
                    }
                    // fingerprint比較 (同一location内比較)
                    if lf.has_changed(&latest_fingerprint(file_id, origins, location_files)) {
                        trace!(
                            file_id = %file_id,
                            target = %target,
                            source = %source,
                            lf_state = ?lf.state(),
                            "distribute_actions: Update (fingerprint changed)"
                        );
                        actions.push(DistributeAction::Update(UpdateAction {
                            topology_file_id: file_id.to_string(),
                            relative_path: tf.relative_path().to_string(),
                            target: target.clone(),
                            source: source.clone(),
                        }));
                    }
                    // fingerprint一致 → skip（最新）
                }
                None => {
                    // LocationFile不在 → Send
                    trace!(
                        file_id = %file_id,
                        target = %target,
                        source = %source,
                        path = %tf.relative_path(),
                        "distribute_actions: Send (no LocationFile at target)"
                    );
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
    }

    trace!(
        actions = actions.len(),
        conflicts = conflicts.len(),
        "distribute_actions: done"
    );
    DistributeResult { actions, conflicts }
}

/// 削除済みTopologyFileに対するDelete配布アクションを生成する。
///
/// # 引数
///
/// - `deleted_topology_files` — 削除済みのTopologyFile群
/// - `location_files` — `file_id → LocationFile[]`
/// - `target_locations` — 配布先候補
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
/// 1. ingest_origins から最初のLocationId
/// 2. fallback: Active状態のLocationFileを持つ任意のLocation
fn pick_source(
    file_id: &str,
    origins: &HashSet<LocationId>,
    location_files: &HashMap<String, Vec<&LocationFile>>,
) -> Option<LocationId> {
    // 1. Ingest originから選択
    if let Some(origin) = origins.iter().next() {
        return Some(origin.clone());
    }
    // 2. Active LocationFileを持つLocationからfallback
    if let Some(lfs) = location_files.get(file_id) {
        for lf in lfs {
            if lf.state().is_source_eligible() {
                return Some(lf.location_id().clone());
            }
        }
    }
    None
}

/// ingest_originsのLocationが持つ最新fingerprintを取得する。
///
/// originsの最初のLocationのLocationFileからfingerprintを取る。
/// fallback: 任意のActive LocationFileのfingerprint。
fn latest_fingerprint(
    file_id: &str,
    origins: &HashSet<LocationId>,
    location_files: &HashMap<String, Vec<&LocationFile>>,
) -> FileFingerprint {
    if let Some(lfs) = location_files.get(file_id) {
        // originsのLocationのfingerprintを優先
        for origin in origins {
            if let Some(lf) = lfs.iter().find(|lf| lf.location_id() == origin) {
                return lf.fingerprint().clone();
            }
        }
        // fallback: Active状態のLocationFile
        for lf in lfs {
            if lf.state().is_source_eligible() {
                return lf.fingerprint().clone();
            }
        }
    }
    // 到達しないはずだが安全なフォールバック
    FileFingerprint {
        byte_digest: None,
        content_digest: None,
        meta_digest: None,
        size: 0,
        modified_at: None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::topology_file::TopologyFile;

    fn local() -> LocationId {
        LocationId::local()
    }

    fn pod() -> LocationId {
        LocationId::new("pod").unwrap()
    }

    fn cloud() -> LocationId {
        LocationId::new("cloud").unwrap()
    }

    fn local_fp(hash: &str, size: u64) -> FileFingerprint {
        use crate::domain::digest::ByteDigest;
        FileFingerprint {
            byte_digest: Some(ByteDigest::Djb2(hash.to_string())),
            content_digest: None,
            meta_digest: None,
            size,
            modified_at: None,
        }
    }

    fn content_fp(file_hash: &str, content_hash: &str, size: u64) -> FileFingerprint {
        use crate::domain::digest::{ByteDigest, ContentDigest};
        FileFingerprint {
            byte_digest: Some(ByteDigest::Djb2(file_hash.to_string())),
            content_digest: Some(ContentDigest(content_hash.to_string())),
            meta_digest: None,
            size,
            modified_at: None,
        }
    }

    fn cloud_fp(size: u64) -> FileFingerprint {
        FileFingerprint {
            byte_digest: None,
            content_digest: None,
            meta_digest: None,
            size,
            modified_at: None,
        }
    }

    // =========================================================================
    // TopologyDelta — accessors
    // =========================================================================

    #[test]
    fn discovered_accessors() {
        let delta = TopologyDelta::Discovered(DiscoveredFile {
            id: "uuid-1".into(),
            relative_path: "output/001.png".into(),
            file_type: FileType::Image,
            fingerprint: local_fp("abc", 1024),
            origin: local(),
            embedded_id: Some("gen-1".into()),
        });
        assert_eq!(delta.relative_path(), "output/001.png");
        assert_eq!(delta.origin(), &local());
        assert_eq!(delta.topology_file_id(), "uuid-1");
        assert!(delta.is_discovered());
        assert!(!delta.is_content_changed());
        assert!(!delta.is_renamed());
        assert!(!delta.is_vanished());
    }

    #[test]
    fn content_changed_accessors() {
        let delta = TopologyDelta::ContentChanged(ContentChangedFile {
            topology_file_id: "tf-1".into(),
            relative_path: "output/002.png".into(),
            file_type: FileType::Image,
            old_fingerprint: local_fp("old", 1024),
            new_fingerprint: local_fp("new", 2048),
            origin: local(),
            embedded_id: None,
        });
        assert_eq!(delta.topology_file_id(), "tf-1");
        assert!(delta.is_content_changed());
    }

    #[test]
    fn renamed_accessors() {
        let delta = TopologyDelta::Renamed(RenamedFile {
            topology_file_id: "tf-1".into(),
            old_path: "old/name.png".into(),
            new_path: "new/name.png".into(),
            file_type: FileType::Image,
            fingerprint: local_fp("h1", 1024),
            origin: local(),
            embedded_id: None,
        });
        // relative_path()はnew_pathを返す
        assert_eq!(delta.relative_path(), "new/name.png");
        assert!(delta.is_renamed());
    }

    #[test]
    fn vanished_accessors() {
        let delta = TopologyDelta::Vanished(VanishedFile {
            topology_file_id: "tf-1".into(),
            relative_path: "output/gone.png".into(),
            origin: local(),
        });
        assert_eq!(delta.topology_file_id(), "tf-1");
        assert!(delta.is_vanished());
    }

    // =========================================================================
    // Display
    // =========================================================================

    #[test]
    fn display_discovered() {
        let delta = TopologyDelta::Discovered(DiscoveredFile {
            id: "id".into(),
            relative_path: "a.png".into(),
            file_type: FileType::Image,
            fingerprint: local_fp("h", 10),
            origin: local(),
            embedded_id: None,
        });
        assert!(delta.to_string().starts_with('+'));
    }

    #[test]
    fn display_renamed() {
        let delta = TopologyDelta::Renamed(RenamedFile {
            topology_file_id: "id".into(),
            old_path: "old.png".into(),
            new_path: "new.png".into(),
            file_type: FileType::Image,
            fingerprint: local_fp("h", 10),
            origin: local(),
            embedded_id: None,
        });
        let s = delta.to_string();
        assert!(s.starts_with('>'));
        assert!(s.contains("old.png"));
        assert!(s.contains("new.png"));
    }

    // =========================================================================
    // from_scan_match — Discovered (NoMatch)
    // =========================================================================

    #[test]
    fn no_match_produces_discovered() {
        let delta = TopologyDelta::from_scan_match(
            ScanMatch::NoMatch,
            None,
            None,
            "brand_new.png",
            &local_fp("abc", 1024),
            FileType::Image,
            &local(),
            Some("gen-1".into()),
        );
        assert!(delta.is_some());
        let d = delta.unwrap();
        assert!(d.is_discovered());
        assert_eq!(d.relative_path(), "brand_new.png");
        assert_eq!(d.origin(), &local());
    }

    // =========================================================================
    // from_scan_match — Renamed (ByHash + path不一致)
    // =========================================================================

    #[test]
    fn by_hash_different_path_produces_renamed() {
        let mut tf = TopologyFile::new("old/path.png".into(), FileType::Image).unwrap();
        let fp = content_fp("h1", "pixel_abc", 1024);
        tf.promote_canonical_digest(&fp);

        let scan_fp = content_fp("h2", "pixel_abc", 2048);
        let delta = TopologyDelta::from_scan_match(
            ScanMatch::ByHash,
            Some(&tf),
            None,
            "new/path.png",
            &scan_fp,
            FileType::Image,
            &local(),
            None,
        );
        assert!(delta.is_some());
        let d = delta.unwrap();
        assert!(d.is_renamed());
        if let TopologyDelta::Renamed(r) = &d {
            assert_eq!(r.old_path, "old/path.png");
            assert_eq!(r.new_path, "new/path.png");
        }
    }

    // =========================================================================
    // from_scan_match — ContentChanged (ByPath + fingerprint不一致)
    // =========================================================================

    #[test]
    fn by_path_changed_fingerprint_produces_content_changed() {
        let tf = TopologyFile::new("output/001.png".into(), FileType::Image).unwrap();
        let existing_fp = local_fp("old_hash", 1024);

        let scan_fp = local_fp("new_hash", 2048);
        let delta = TopologyDelta::from_scan_match(
            ScanMatch::ByPath,
            Some(&tf),
            Some(&existing_fp),
            "output/001.png",
            &scan_fp,
            FileType::Image,
            &local(),
            None,
        );
        assert!(delta.is_some());
        let d = delta.unwrap();
        assert!(d.is_content_changed());
        if let TopologyDelta::ContentChanged(c) = &d {
            assert_eq!(
                c.old_fingerprint.byte_digest.as_ref().map(|d| d.as_str()),
                Some("old_hash")
            );
            assert_eq!(
                c.new_fingerprint.byte_digest.as_ref().map(|d| d.as_str()),
                Some("new_hash")
            );
        }
    }

    #[test]
    fn by_path_unchanged_fingerprint_produces_none() {
        let tf = TopologyFile::new("output/001.png".into(), FileType::Image).unwrap();
        let existing_fp = local_fp("same_hash", 1024);

        let scan_fp = local_fp("same_hash", 1024);
        let delta = TopologyDelta::from_scan_match(
            ScanMatch::ByPath,
            Some(&tf),
            Some(&existing_fp),
            "output/001.png",
            &scan_fp,
            FileType::Image,
            &local(),
            None,
        );
        assert!(delta.is_none(), "unchanged file should produce no delta");
    }

    // =========================================================================
    // from_scan_match — ByPath + LocationFile未登録 → ContentChanged
    // =========================================================================

    #[test]
    fn by_path_no_location_file_produces_content_changed() {
        // TopologyFileは存在するが、このLocationにLocationFileがないケース
        // （他Locationで先にDiscoveredされたファイル）
        let tf = TopologyFile::new("output/001.png".into(), FileType::Image).unwrap();

        let scan_fp = local_fp("abc", 1024);
        let delta = TopologyDelta::from_scan_match(
            ScanMatch::ByPath,
            Some(&tf),
            None, // このLocationにLocationFileなし
            "output/001.png",
            &scan_fp,
            FileType::Image,
            &pod(),
            None,
        );
        assert!(delta.is_some());
        let d = delta.unwrap();
        assert!(d.is_content_changed());
        assert_eq!(d.origin(), &pod());
    }

    // =========================================================================
    // from_scan_match — ByHash + path一致 → コンテンツ変更チェック
    // =========================================================================

    #[test]
    fn by_hash_same_path_unchanged_produces_none() {
        let mut tf = TopologyFile::new("output/001.png".into(), FileType::Image).unwrap();
        let fp = content_fp("h1", "pixel_abc", 1024);
        tf.promote_canonical_digest(&fp);

        let existing_lf_fp = content_fp("h1", "pixel_abc", 1024);
        let scan_fp = content_fp("h1", "pixel_abc", 1024);

        let delta = TopologyDelta::from_scan_match(
            ScanMatch::ByHash,
            Some(&tf),
            Some(&existing_lf_fp),
            "output/001.png",
            &scan_fp,
            FileType::Image,
            &local(),
            None,
        );
        assert!(
            delta.is_none(),
            "hash match + path match + fp match = no change"
        );
    }

    // =========================================================================
    // from_scan_match — Cloud (hashなし)
    // =========================================================================

    #[test]
    fn cloud_new_file_produces_discovered() {
        let delta = TopologyDelta::from_scan_match(
            ScanMatch::NoMatch,
            None,
            None,
            "cloud/photo.png",
            &cloud_fp(4096),
            FileType::Image,
            &cloud(),
            None,
        );
        assert!(delta.is_some());
        let d = delta.unwrap();
        assert!(d.is_discovered());
        assert_eq!(d.origin(), &cloud());
    }

    #[test]
    fn cloud_same_size_unchanged() {
        let tf = TopologyFile::new("cloud/photo.png".into(), FileType::Image).unwrap();
        let existing_fp = cloud_fp(4096);
        let scan_fp = cloud_fp(4096);

        let delta = TopologyDelta::from_scan_match(
            ScanMatch::ByPath,
            Some(&tf),
            Some(&existing_fp),
            "cloud/photo.png",
            &scan_fp,
            FileType::Image,
            &cloud(),
            None,
        );
        assert!(delta.is_none(), "cloud same size = no change");
    }

    #[test]
    fn cloud_different_size_produces_content_changed() {
        let tf = TopologyFile::new("cloud/photo.png".into(), FileType::Image).unwrap();
        let existing_fp = cloud_fp(4096);
        let scan_fp = cloud_fp(8192);

        let delta = TopologyDelta::from_scan_match(
            ScanMatch::ByPath,
            Some(&tf),
            Some(&existing_fp),
            "cloud/photo.png",
            &scan_fp,
            FileType::Image,
            &cloud(),
            None,
        );
        assert!(delta.is_some());
        assert!(delta.unwrap().is_content_changed());
    }

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
