//! TopologyDelta — Topology視点のファイル変化（Phase 1: Ingest）。
//!
//! 各Locationのスキャン結果をTopologyに集約する際の差分を表す。
//! L1→T, L2→T, L3→T ... と順にIngestし、Topologyの状態を更新する。
//!
//! Phase 2 (Distribute) は [`distribute`](super::distribute) モジュールに分離。
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
//! ```

use super::file_type::FileType;
use super::fingerprint::FileFingerprint;
use super::location::LocationId;
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
    #[allow(dead_code)] // file_type別の同期ルール分岐で使用予定
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
    #[allow(dead_code)] // file_type別の同期ルール分岐で使用予定
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
// ScanContext — from_scan_match の入力パラメータ
// =============================================================================

/// `TopologyDelta::from_scan_match()` の入力をまとめた構造体。
///
/// 8引数を構造体に集約し、呼び出し側の可読性を向上させる。
pub struct ScanContext<'a> {
    /// TopologyFile.matches_scan()の結果。
    pub scan_match: ScanMatch,
    /// マッチしたTopologyFile（NoMatchの場合はNone）。
    pub topology_file: Option<&'a TopologyFile>,
    /// このLocationでの既存fingerprint（None = 未登録）。
    pub location_fingerprint: Option<&'a FileFingerprint>,
    /// スキャン結果のrelative_path。
    pub scan_path: &'a str,
    /// スキャン結果のfingerprint。
    pub scan_fingerprint: &'a FileFingerprint,
    /// スキャン結果のfile_type。
    pub scan_file_type: FileType,
    /// スキャンを実行したLocation。
    pub scan_origin: &'a LocationId,
    /// スキャン結果のembedded_id。
    pub scan_embedded_id: Option<String>,
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
    pub fn from_scan_match(ctx: ScanContext<'_>) -> Option<Self> {
        match ctx.scan_match {
            ScanMatch::NoMatch => {
                // 新規ファイル
                Some(Self::Discovered(DiscoveredFile {
                    id: uuid::Uuid::new_v4().to_string(),
                    relative_path: ctx.scan_path.to_string(),
                    file_type: ctx.scan_file_type,
                    fingerprint: ctx.scan_fingerprint.clone(),
                    origin: ctx.scan_origin.clone(),
                    embedded_id: ctx.scan_embedded_id,
                }))
            }
            ScanMatch::ByHash => {
                // canonical_hash一致 + path不一致 = rename
                let tf = ctx.topology_file?;
                if tf.relative_path() != ctx.scan_path {
                    Some(Self::Renamed(RenamedFile {
                        topology_file_id: tf.id().to_string(),
                        old_path: tf.relative_path().to_string(),
                        new_path: ctx.scan_path.to_string(),
                        file_type: ctx.scan_file_type,
                        fingerprint: ctx.scan_fingerprint.clone(),
                        origin: ctx.scan_origin.clone(),
                        embedded_id: ctx.scan_embedded_id,
                    }))
                } else {
                    // hash一致 + path一致 → コンテンツ変更チェック
                    check_content_change(
                        tf,
                        ctx.location_fingerprint,
                        ctx.scan_path,
                        ctx.scan_fingerprint,
                        ctx.scan_file_type,
                        ctx.scan_origin,
                        ctx.scan_embedded_id,
                    )
                }
            }
            ScanMatch::ByPath => {
                // path一致 → コンテンツ変更チェック
                let tf = ctx.topology_file?;
                check_content_change(
                    tf,
                    ctx.location_fingerprint,
                    ctx.scan_path,
                    ctx.scan_fingerprint,
                    ctx.scan_file_type,
                    ctx.scan_origin,
                    ctx.scan_embedded_id,
                )
            }
        }
    }
}

/// LocationFileのfingerprint比較でコンテンツ変更を検出する。
///
/// location_fingerprintがNone（このLocationに未登録）の場合、
/// 新規追加としてContentChangedを返す。
fn check_content_change(
    topology_file: &TopologyFile,
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::test_helpers::{cloud, cloud_fp, content_fp, local, local_fp, pod};
    use crate::domain::topology_file::TopologyFile;

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
        let fp = local_fp("abc", 1024);
        let origin = local();
        let delta = TopologyDelta::from_scan_match(ScanContext {
            scan_match: ScanMatch::NoMatch,
            topology_file: None,
            location_fingerprint: None,
            scan_path: "brand_new.png",
            scan_fingerprint: &fp,
            scan_file_type: FileType::Image,
            scan_origin: &origin,
            scan_embedded_id: Some("gen-1".into()),
        });
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
        let origin = local();
        let delta = TopologyDelta::from_scan_match(ScanContext {
            scan_match: ScanMatch::ByHash,
            topology_file: Some(&tf),
            location_fingerprint: None,
            scan_path: "new/path.png",
            scan_fingerprint: &scan_fp,
            scan_file_type: FileType::Image,
            scan_origin: &origin,
            scan_embedded_id: None,
        });
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
        let origin = local();
        let delta = TopologyDelta::from_scan_match(ScanContext {
            scan_match: ScanMatch::ByPath,
            topology_file: Some(&tf),
            location_fingerprint: Some(&existing_fp),
            scan_path: "output/001.png",
            scan_fingerprint: &scan_fp,
            scan_file_type: FileType::Image,
            scan_origin: &origin,
            scan_embedded_id: None,
        });
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
        let origin = local();
        let delta = TopologyDelta::from_scan_match(ScanContext {
            scan_match: ScanMatch::ByPath,
            topology_file: Some(&tf),
            location_fingerprint: Some(&existing_fp),
            scan_path: "output/001.png",
            scan_fingerprint: &scan_fp,
            scan_file_type: FileType::Image,
            scan_origin: &origin,
            scan_embedded_id: None,
        });
        assert!(delta.is_none(), "unchanged file should produce no delta");
    }

    // =========================================================================
    // from_scan_match — ByPath + LocationFile未登録 → ContentChanged
    // =========================================================================

    #[test]
    fn by_path_no_location_file_produces_content_changed() {
        let tf = TopologyFile::new("output/001.png".into(), FileType::Image).unwrap();
        let scan_fp = local_fp("abc", 1024);
        let origin = pod();
        let delta = TopologyDelta::from_scan_match(ScanContext {
            scan_match: ScanMatch::ByPath,
            topology_file: Some(&tf),
            location_fingerprint: None,
            scan_path: "output/001.png",
            scan_fingerprint: &scan_fp,
            scan_file_type: FileType::Image,
            scan_origin: &origin,
            scan_embedded_id: None,
        });
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
        let origin = local();
        let delta = TopologyDelta::from_scan_match(ScanContext {
            scan_match: ScanMatch::ByHash,
            topology_file: Some(&tf),
            location_fingerprint: Some(&existing_lf_fp),
            scan_path: "output/001.png",
            scan_fingerprint: &scan_fp,
            scan_file_type: FileType::Image,
            scan_origin: &origin,
            scan_embedded_id: None,
        });
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
        let fp = cloud_fp(4096);
        let origin = cloud();
        let delta = TopologyDelta::from_scan_match(ScanContext {
            scan_match: ScanMatch::NoMatch,
            topology_file: None,
            location_fingerprint: None,
            scan_path: "cloud/photo.png",
            scan_fingerprint: &fp,
            scan_file_type: FileType::Image,
            scan_origin: &origin,
            scan_embedded_id: None,
        });
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
        let origin = cloud();
        let delta = TopologyDelta::from_scan_match(ScanContext {
            scan_match: ScanMatch::ByPath,
            topology_file: Some(&tf),
            location_fingerprint: Some(&existing_fp),
            scan_path: "cloud/photo.png",
            scan_fingerprint: &scan_fp,
            scan_file_type: FileType::Image,
            scan_origin: &origin,
            scan_embedded_id: None,
        });
        assert!(delta.is_none(), "cloud same size = no change");
    }

    #[test]
    fn cloud_different_size_produces_content_changed() {
        let tf = TopologyFile::new("cloud/photo.png".into(), FileType::Image).unwrap();
        let existing_fp = cloud_fp(4096);
        let scan_fp = cloud_fp(8192);
        let origin = cloud();
        let delta = TopologyDelta::from_scan_match(ScanContext {
            scan_match: ScanMatch::ByPath,
            topology_file: Some(&tf),
            location_fingerprint: Some(&existing_fp),
            scan_path: "cloud/photo.png",
            scan_fingerprint: &scan_fp,
            scan_file_type: FileType::Image,
            scan_origin: &origin,
            scan_embedded_id: None,
        });
        assert!(delta.is_some());
        assert!(delta.unwrap().is_content_changed());
    }
}
