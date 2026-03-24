//! FileDelta — ファイルツリーの差分を表す値オブジェクト。
//!
//! scanフェーズの出力であり、planフェーズの入力。
//! インフラ（DB, FS）に依存しない純粋なドメイン型。
//!
//! # ライフサイクル
//!
//! ```text
//! Scanner.scan_all()  → Vec<FileDelta>    (Phase 1: Scan)
//! plan_transfers()    → Vec<PlannedTransfer> (Phase 2: Plan)
//! Store.apply()       → DB反映            (Phase 3: Apply)
//! ```

use super::file_type::FileType;
use super::fingerprint::FileFingerprint;
use super::location::LocationId;

/// ファイルツリーの1要素の変化。
///
/// scan結果とDB状態の差分から導出される。
/// 各バリアントはscan時点で確定した事実のみを持つ。
#[derive(Debug, Clone)]
pub enum FileDelta {
    /// scan元に存在するがDB未登録のファイル。
    Added(AddedFile),
    /// DB登録済みでfingerprintが変化したファイル。
    Modified(ModifiedFile),
    /// DB登録済みだがscan元から消失したファイル。
    Removed(RemovedFile),
}

/// 新規ファイル。scan元で初めて検出された。
#[derive(Debug, Clone)]
pub struct AddedFile {
    /// scan時に生成する仮ID (UUID)。Apply時にTrackedFileのIDとなる。
    pub(crate) id: String,
    pub(crate) relative_path: String,
    pub(crate) file_type: FileType,
    pub(crate) fingerprint: FileFingerprint,
    /// ファイルが検出されたlocation。Transfer計画の起点。
    pub(crate) origin: LocationId,
    /// ファイル内メタデータから抽出されたID（PNG tEXt等）。
    pub(crate) embedded_id: Option<String>,
}

/// 変更ファイル。DB上のfingerprintとscan結果が不一致。
#[derive(Debug, Clone)]
#[allow(dead_code)] // old_fingerprint: 差分ログ・監査用に保持
pub struct ModifiedFile {
    /// DB上の既存TrackedFileのID。
    pub(crate) file_id: String,
    pub(crate) relative_path: String,
    pub(crate) file_type: FileType,
    pub(crate) old_fingerprint: FileFingerprint,
    pub(crate) new_fingerprint: FileFingerprint,
    pub(crate) origin: LocationId,
    pub(crate) embedded_id: Option<String>,
}

/// 削除ファイル。DB登録済みだがscan元から消失。
#[derive(Debug, Clone)]
pub struct RemovedFile {
    pub(crate) file_id: String,
    pub(crate) relative_path: String,
    /// 消失が検出されたlocation。Delete Transfer の起点。
    pub(crate) origin: LocationId,
}

impl FileDelta {
    pub fn relative_path(&self) -> &str {
        match self {
            Self::Added(f) => &f.relative_path,
            Self::Modified(f) => &f.relative_path,
            Self::Removed(f) => &f.relative_path,
        }
    }

    pub fn origin(&self) -> &LocationId {
        match self {
            Self::Added(f) => &f.origin,
            Self::Modified(f) => &f.origin,
            Self::Removed(f) => &f.origin,
        }
    }

    /// Plan用: このdeltaに対応するファイルID。
    /// Addedの場合は仮ID、Modified/Removedの場合はDB上のID。
    pub fn file_id(&self) -> &str {
        match self {
            Self::Added(f) => &f.id,
            Self::Modified(f) => &f.file_id,
            Self::Removed(f) => &f.file_id,
        }
    }

    pub fn is_added(&self) -> bool {
        matches!(self, Self::Added(_))
    }

    pub fn is_modified(&self) -> bool {
        matches!(self, Self::Modified(_))
    }

    pub fn is_removed(&self) -> bool {
        matches!(self, Self::Removed(_))
    }
}

impl std::fmt::Display for FileDelta {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Added(a) => write!(f, "+{} [{}]", a.relative_path, a.origin),
            Self::Modified(m) => write!(f, "~{} [{}]", m.relative_path, m.origin),
            Self::Removed(r) => write!(f, "-{} [{}]", r.relative_path, r.origin),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;

    fn local() -> LocationId {
        LocationId::local()
    }

    fn cloud() -> LocationId {
        LocationId::new("cloud").unwrap()
    }

    fn sample_fp(hash: &str, size: u64) -> FileFingerprint {
        FileFingerprint {
            file_hash: Some(hash.to_string()),
            content_hash: None,
            meta_hash: None,
            size,
            modified_at: None,
        }
    }

    fn cloud_fp(size: u64) -> FileFingerprint {
        FileFingerprint {
            file_hash: None,
            content_hash: None,
            meta_hash: None,
            size,
            modified_at: Some(Utc::now()),
        }
    }

    #[test]
    fn added_accessors() {
        let delta = FileDelta::Added(AddedFile {
            id: "uuid-1".into(),
            relative_path: "output/001.png".into(),
            file_type: FileType::Image,
            fingerprint: sample_fp("abc", 1024),
            origin: local(),
            embedded_id: Some("gen-1".into()),
        });

        assert_eq!(delta.relative_path(), "output/001.png");
        assert_eq!(delta.origin(), &local());
        assert_eq!(delta.file_id(), "uuid-1");
        assert!(delta.is_added());
        assert!(!delta.is_modified());
        assert!(!delta.is_removed());
    }

    #[test]
    fn modified_accessors() {
        let delta = FileDelta::Modified(ModifiedFile {
            file_id: "existing-id".into(),
            relative_path: "output/002.png".into(),
            file_type: FileType::Image,
            old_fingerprint: sample_fp("old", 1024),
            new_fingerprint: sample_fp("new", 2048),
            origin: local(),
            embedded_id: None,
        });

        assert_eq!(delta.file_id(), "existing-id");
        assert!(delta.is_modified());
    }

    #[test]
    fn removed_accessors() {
        let delta = FileDelta::Removed(RemovedFile {
            file_id: "del-id".into(),
            relative_path: "output/gone.png".into(),
            origin: local(),
        });

        assert_eq!(delta.file_id(), "del-id");
        assert!(delta.is_removed());
    }

    #[test]
    fn display_format() {
        let added = FileDelta::Added(AddedFile {
            id: "id".into(),
            relative_path: "a.png".into(),
            file_type: FileType::Image,
            fingerprint: sample_fp("h", 10),
            origin: local(),
            embedded_id: None,
        });
        assert!(added.to_string().starts_with('+'));

        let modified = FileDelta::Modified(ModifiedFile {
            file_id: "id".into(),
            relative_path: "b.png".into(),
            file_type: FileType::Image,
            old_fingerprint: sample_fp("old", 10),
            new_fingerprint: sample_fp("new", 20),
            origin: cloud(),
            embedded_id: None,
        });
        assert!(modified.to_string().starts_with('~'));

        let removed = FileDelta::Removed(RemovedFile {
            file_id: "id".into(),
            relative_path: "c.png".into(),
            origin: local(),
        });
        assert!(removed.to_string().starts_with('-'));
    }

    #[test]
    fn cloud_delta_uses_metadata_fingerprint() {
        let delta = FileDelta::Added(AddedFile {
            id: "cloud-file".into(),
            relative_path: "remote/photo.png".into(),
            file_type: FileType::Image,
            fingerprint: cloud_fp(4096),
            origin: cloud(),
            embedded_id: None,
        });
        if let FileDelta::Added(f) = &delta {
            assert!(f.fingerprint.file_hash.is_none());
            assert!(f.fingerprint.modified_at.is_some());
        }
    }
}
