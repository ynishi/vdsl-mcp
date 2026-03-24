//! TopologyFile — トポロジー上のファイル身元 (Master / inode)。
//!
//! ファイルの「何であるか」を管理する。各locationでの実体は
//! [`LocationFile`](super::location_file::LocationFile) が管理する。
//!
//! # 画像ファイル = Entity モデル
//!
//! Content（ピクセルデータ）がEntityのIdentity。
//! `canonical_digest` は ContentDigest を正規化したもので、
//! location非依存のEntity同一性判定に使用する。
//!
//! # 型安全性
//!
//! `canonical_digest` は [`ContentDigest`] 型。
//! [`ByteDigest`](super::digest::ByteDigest) からの混入は型レベルで不可能。
//!
//! # スキャン結果とのマッチング
//!
//! [`matches_scan()`](TopologyFile::matches_scan) が多段フォールバックで
//! スキャン結果を既知TopologyFileに紐付ける:
//! 1. canonical_digest一致 → 同一Entity（rename検出対応）
//! 2. relative_path一致 → 同一パス（最も一般的）
//! 3. 全不一致 → 新規ファイル

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use super::digest::ContentDigest;
use super::error::DomainError;
use super::file_type::FileType;
use super::fingerprint::FileFingerprint;

/// スキャン結果とTopologyFileのマッチング結果。
///
/// [`TopologyFile::matches_scan()`] が返す。
/// マッチ精度の高い順: ByHash > ByPath。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ScanMatch {
    /// canonical_digest一致。同一Entity。
    /// pathが異なればrename検出。
    ByHash,
    /// relative_path一致。同一パス。
    ByPath,
    /// マッチしない。
    NoMatch,
}

impl ScanMatch {
    pub fn is_match(&self) -> bool {
        !matches!(self, Self::NoMatch)
    }
}

/// トポロジー上のファイル身元情報 (Master / inode)。
///
/// # 設計原則
///
/// - ファイルの「身元」+ ContentDigestベースのcanonical_digest
/// - location状態は持たない — [`LocationFile`] が管理
/// - 転送状態は持たない — [`Transfer`](super::transfer::Transfer) が管理
///
/// # フィールド
///
/// | フィールド | 意味 |
/// |---|---|
/// | `id` | UUID v4。全locationで共通の識別子 |
/// | `relative_path` | sync_rootからの相対パス（正規パス） |
/// | `canonical_digest` | location非依存の正規ContentDigest。ByteDigest混入不可 |
/// | `file_type` | 拡張子から判定 |
/// | `registered_at` | 初回検出日時 |
/// | `deleted_at` | 削除検出日時。Noneなら生存中 |
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TopologyFile {
    id: String,
    relative_path: String,
    /// location非依存の正規ContentDigest。
    /// ContentDigest型のみ受け入れ — ByteDigest混入はコンパイルエラー。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    canonical_digest: Option<ContentDigest>,
    file_type: FileType,
    registered_at: DateTime<Utc>,
    /// ファイルが削除検出された日時。Noneなら生存中。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    deleted_at: Option<DateTime<Utc>>,
}

impl TopologyFile {
    // =========================================================================
    // Factory
    // =========================================================================

    /// 新規ファイル登録。idはUUID v4で自動生成。
    ///
    /// # Errors
    ///
    /// - `relative_path` が空文字列の場合
    pub fn new(relative_path: String, file_type: FileType) -> Result<Self, DomainError> {
        if relative_path.is_empty() {
            return Err(DomainError::Validation {
                field: "relative_path".into(),
                reason: "must not be empty".into(),
            });
        }
        Ok(Self {
            id: uuid::Uuid::new_v4().to_string(),
            relative_path,
            canonical_digest: None,
            file_type,
            registered_at: Utc::now(),
            deleted_at: None,
        })
    }

    /// DB復元用。永続化済みデータからの再構成。
    pub(crate) fn reconstitute(
        id: String,
        relative_path: String,
        canonical_digest: Option<ContentDigest>,
        file_type: FileType,
        registered_at: DateTime<Utc>,
        deleted_at: Option<DateTime<Utc>>,
    ) -> Self {
        Self {
            id,
            relative_path,
            canonical_digest,
            file_type,
            registered_at,
            deleted_at,
        }
    }

    // =========================================================================
    // Commands
    // =========================================================================

    /// 削除済みマーク。冪等。
    pub fn mark_deleted(&mut self) {
        if self.deleted_at.is_none() {
            self.deleted_at = Some(Utc::now());
        }
    }

    /// 削除済みマーク解除（再登録時）。
    pub fn unmark_deleted(&mut self) {
        self.deleted_at = None;
    }

    /// canonical_digestを昇格更新する。
    ///
    /// LocationFileのfingerprintからContentDigestを抽出し、
    /// 現在のcanonical_digestと異なれば上書きする。
    ///
    /// **ByteDigestはContentDigestと型が異なるため混入不可能。**
    ///
    /// # Returns
    ///
    /// 更新があった場合true。
    pub fn promote_canonical_digest(&mut self, fingerprint: &FileFingerprint) -> bool {
        let candidate = &fingerprint.content_digest;
        match (&self.canonical_digest, candidate) {
            // 新候補がNone → 昇格なし
            (_, None) => false,
            // 現在None → 任意のContentDigestで昇格
            (None, Some(cd)) => {
                self.canonical_digest = Some(cd.clone());
                true
            }
            // 両方Some → 値が異なれば更新
            (Some(old), Some(new)) => {
                if old != new {
                    self.canonical_digest = Some(new.clone());
                    true
                } else {
                    false
                }
            }
        }
    }

    /// relative_pathを更新する（rename検出時）。
    pub fn update_path(&mut self, new_path: String) {
        self.relative_path = new_path;
    }

    // =========================================================================
    // Queries
    // =========================================================================

    pub fn id(&self) -> &str {
        &self.id
    }

    pub fn relative_path(&self) -> &str {
        &self.relative_path
    }

    pub fn canonical_digest(&self) -> Option<&ContentDigest> {
        self.canonical_digest.as_ref()
    }

    /// canonical_hashの文字列表現（後方互換）。
    pub fn canonical_hash(&self) -> Option<&str> {
        self.canonical_digest.as_ref().map(|cd| cd.as_str())
    }

    pub fn file_type(&self) -> FileType {
        self.file_type
    }

    pub fn registered_at(&self) -> DateTime<Utc> {
        self.registered_at
    }

    pub fn deleted_at(&self) -> Option<DateTime<Utc>> {
        self.deleted_at
    }

    pub fn is_deleted(&self) -> bool {
        self.deleted_at.is_some()
    }

    // =========================================================================
    // LocationFile — Hard Link (materialize)
    // =========================================================================

    /// このTopologyFileをlocationに実体化する。
    ///
    /// Hard Link: TopologyFile(inode) → LocationFile(directory entry)。
    /// file_idは構造的に保証される — 外部からのID指定は不可。
    ///
    /// スキャン結果をドメイン境界でLocationFileに変換する唯一の経路。
    pub fn materialize(
        &self,
        location_id: super::location::LocationId,
        relative_path: String,
        fingerprint: FileFingerprint,
        embedded_id: Option<String>,
    ) -> Result<super::location_file::LocationFile, DomainError> {
        super::location_file::LocationFile::new(
            self.id.clone(),
            location_id,
            relative_path,
            fingerprint,
            embedded_id,
        )
    }

    // =========================================================================
    // Matching — スキャン結果との多段マッチング
    // =========================================================================

    /// スキャン結果とのマッチング。
    ///
    /// 多段フォールバックで同一ファイルを判定する:
    /// 1. canonical_digest一致 → ByHash（rename検出対応）
    /// 2. relative_path一致 → ByPath（最も一般的）
    /// 3. 全不一致 → NoMatch
    ///
    /// # 引数
    ///
    /// - `scan_path`: スキャン結果のrelative_path
    /// - `scan_fingerprint`: スキャン結果のfingerprint
    pub fn matches_scan(&self, scan_path: &str, scan_fingerprint: &FileFingerprint) -> ScanMatch {
        // 1. canonical_digest一致チェック（ContentDigest同士の比較）
        if let Some(ref canonical) = self.canonical_digest {
            if let Some(ref scan_cd) = scan_fingerprint.content_digest {
                if canonical == scan_cd {
                    return ScanMatch::ByHash;
                }
            }
        }
        // 2. relative_path一致チェック
        if self.relative_path == scan_path {
            return ScanMatch::ByPath;
        }
        ScanMatch::NoMatch
    }
}

// =============================================================================
// TopologyFile == LocationFile (Hard Link equality)
// =============================================================================

/// Hard Link等価性: TopologyFile(inode) == LocationFile(directory entry at location)。
///
/// `materialize()`で張られたリンクを確認する。
/// file_idの一致 = 同一ファイルの異なるlocationでの実体。
impl PartialEq<super::location_file::LocationFile> for TopologyFile {
    fn eq(&self, other: &super::location_file::LocationFile) -> bool {
        self.id == other.file_id()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::digest::ByteDigest;

    fn make_fp(
        byte_digest: Option<ByteDigest>,
        content_digest: Option<&str>,
        size: u64,
    ) -> FileFingerprint {
        FileFingerprint {
            byte_digest,
            content_digest: content_digest.map(|s| ContentDigest(s.to_string())),
            meta_digest: None,
            size,
            modified_at: None,
        }
    }

    // =========================================================================
    // Factory
    // =========================================================================

    #[test]
    fn new_sets_fields() {
        let f = TopologyFile::new("output/gen-001.png".into(), FileType::Image)
            .expect("valid test data");
        assert_eq!(f.relative_path(), "output/gen-001.png");
        assert_eq!(f.file_type(), FileType::Image);
        assert!(!f.id().is_empty());
        assert!(!f.is_deleted());
        assert!(f.canonical_digest().is_none());
    }

    #[test]
    fn new_rejects_empty_path() {
        let result = TopologyFile::new("".into(), FileType::Image);
        assert!(result.is_err());
    }

    // =========================================================================
    // Commands — mark_deleted / unmark_deleted
    // =========================================================================

    #[test]
    fn mark_deleted_is_idempotent() {
        let mut f = TopologyFile::new("a.png".into(), FileType::Image).unwrap();
        assert!(!f.is_deleted());
        f.mark_deleted();
        let first_deleted_at = f.deleted_at().unwrap();
        assert!(f.is_deleted());
        f.mark_deleted();
        assert_eq!(f.deleted_at().unwrap(), first_deleted_at);
    }

    #[test]
    fn unmark_deleted_clears() {
        let mut f = TopologyFile::new("a.png".into(), FileType::Image).unwrap();
        f.mark_deleted();
        assert!(f.is_deleted());
        f.unmark_deleted();
        assert!(!f.is_deleted());
    }

    // =========================================================================
    // promote_canonical_digest
    // =========================================================================

    #[test]
    fn promote_from_none_to_content_digest() {
        let mut f = TopologyFile::new("a.png".into(), FileType::Image).unwrap();
        assert!(f.canonical_digest().is_none());
        let fp = make_fp(
            Some(ByteDigest::Djb2("djb2_abc".into())),
            Some("pixel_xyz"),
            1024,
        );
        assert!(f.promote_canonical_digest(&fp));
        assert_eq!(f.canonical_hash(), Some("pixel_xyz"));
    }

    #[test]
    fn promote_no_content_digest_no_change() {
        let mut f = TopologyFile::new("a.png".into(), FileType::Image).unwrap();
        // byte_digestのみ、content_digestなし → 昇格なし（型安全: ByteDigest混入不可）
        let fp = make_fp(Some(ByteDigest::Djb2("djb2_abc".into())), None, 1024);
        assert!(!f.promote_canonical_digest(&fp));
        assert!(f.canonical_digest().is_none());
    }

    #[test]
    fn promote_content_digest_updates() {
        let mut f = TopologyFile::new("a.png".into(), FileType::Image).unwrap();
        let fp1 = make_fp(None, Some("pixel_v1"), 1024);
        f.promote_canonical_digest(&fp1);
        assert_eq!(f.canonical_hash(), Some("pixel_v1"));
        // 新しいcontent_digestで上書き
        let fp2 = make_fp(None, Some("pixel_v2"), 1024);
        assert!(f.promote_canonical_digest(&fp2));
        assert_eq!(f.canonical_hash(), Some("pixel_v2"));
    }

    #[test]
    fn promote_same_content_digest_no_change() {
        let mut f = TopologyFile::new("a.png".into(), FileType::Image).unwrap();
        let fp = make_fp(None, Some("pixel_xyz"), 1024);
        f.promote_canonical_digest(&fp);
        assert!(!f.promote_canonical_digest(&fp));
    }

    #[test]
    fn promote_none_fingerprint_no_change() {
        let mut f = TopologyFile::new("a.png".into(), FileType::Image).unwrap();
        let fp = FileFingerprint {
            byte_digest: None,
            content_digest: None,
            meta_digest: None,
            size: 100,
            modified_at: None,
        };
        assert!(!f.promote_canonical_digest(&fp));
        assert!(f.canonical_digest().is_none());
    }

    // =========================================================================
    // matches_scan — 多段マッチング
    // =========================================================================

    #[test]
    fn matches_scan_by_hash() {
        let mut f = TopologyFile::new("output/001.png".into(), FileType::Image).unwrap();
        let fp = make_fp(Some(ByteDigest::Djb2("h1".into())), Some("pixel_abc"), 1024);
        f.promote_canonical_digest(&fp);

        // 同一content_digest、異なるpath → ByHash（rename検出）
        let scan_fp = make_fp(Some(ByteDigest::Djb2("h2".into())), Some("pixel_abc"), 2048);
        assert_eq!(
            f.matches_scan("output/renamed.png", &scan_fp),
            ScanMatch::ByHash
        );
    }

    #[test]
    fn matches_scan_by_path() {
        let f = TopologyFile::new("output/001.png".into(), FileType::Image).unwrap();
        // canonical_digestなし、path一致
        let scan_fp = make_fp(Some(ByteDigest::Djb2("h1".into())), None, 1024);
        assert_eq!(
            f.matches_scan("output/001.png", &scan_fp),
            ScanMatch::ByPath
        );
    }

    #[test]
    fn matches_scan_by_path_when_hash_differs() {
        let mut f = TopologyFile::new("output/001.png".into(), FileType::Image).unwrap();
        let fp = make_fp(Some(ByteDigest::Djb2("h1".into())), Some("pixel_abc"), 1024);
        f.promote_canonical_digest(&fp);

        // content_digest不一致だがpath一致 → ByPath（コンテンツ変更）
        let scan_fp = make_fp(
            Some(ByteDigest::Djb2("h2".into())),
            Some("pixel_different"),
            2048,
        );
        assert_eq!(
            f.matches_scan("output/001.png", &scan_fp),
            ScanMatch::ByPath
        );
    }

    #[test]
    fn matches_scan_no_match() {
        let mut f = TopologyFile::new("output/001.png".into(), FileType::Image).unwrap();
        let fp = make_fp(Some(ByteDigest::Djb2("h1".into())), Some("pixel_abc"), 1024);
        f.promote_canonical_digest(&fp);

        let scan_fp = make_fp(Some(ByteDigest::Djb2("h3".into())), Some("pixel_xyz"), 4096);
        assert_eq!(
            f.matches_scan("output/other.png", &scan_fp),
            ScanMatch::NoMatch
        );
    }

    #[test]
    fn matches_scan_cloud_no_hash() {
        let f = TopologyFile::new("output/001.png".into(), FileType::Image).unwrap();
        let scan_fp = FileFingerprint {
            byte_digest: None,
            content_digest: None,
            meta_digest: None,
            size: 2048,
            modified_at: None,
        };
        assert_eq!(
            f.matches_scan("output/001.png", &scan_fp),
            ScanMatch::ByPath
        );
    }

    #[test]
    fn matches_scan_cloud_no_hash_no_path() {
        let f = TopologyFile::new("output/001.png".into(), FileType::Image).unwrap();
        let scan_fp = FileFingerprint {
            byte_digest: None,
            content_digest: None,
            meta_digest: None,
            size: 2048,
            modified_at: None,
        };
        assert_eq!(
            f.matches_scan("output/other.png", &scan_fp),
            ScanMatch::NoMatch
        );
    }

    // =========================================================================
    // update_path
    // =========================================================================

    #[test]
    fn update_path_changes_relative_path() {
        let mut f = TopologyFile::new("old/path.png".into(), FileType::Image).unwrap();
        f.update_path("new/path.png".into());
        assert_eq!(f.relative_path(), "new/path.png");
    }

    // =========================================================================
    // reconstitute
    // =========================================================================

    #[test]
    fn reconstitute_preserves_all_fields() {
        let now = Utc::now();
        let f = TopologyFile::reconstitute(
            "id-1".into(),
            "path.png".into(),
            Some(ContentDigest("pixel_abc".into())),
            FileType::Image,
            now,
            None,
        );
        assert_eq!(f.id(), "id-1");
        assert_eq!(f.relative_path(), "path.png");
        assert_eq!(f.canonical_hash(), Some("pixel_abc"));
        assert_eq!(f.registered_at(), now);
        assert!(!f.is_deleted());
    }

    // =========================================================================
    // serde
    // =========================================================================

    #[test]
    fn serde_roundtrip() {
        let mut f = TopologyFile::new("test.png".into(), FileType::Image).unwrap();
        let fp = make_fp(Some(ByteDigest::Djb2("h1".into())), Some("pixel_abc"), 1024);
        f.promote_canonical_digest(&fp);

        let json = serde_json::to_value(&f).unwrap();
        let restored: TopologyFile = serde_json::from_value(json).unwrap();
        assert_eq!(restored.id(), f.id());
        assert_eq!(restored.relative_path(), f.relative_path());
        assert_eq!(restored.canonical_hash(), Some("pixel_abc"));
    }

    #[test]
    fn serde_omits_none_canonical_digest() {
        let f = TopologyFile::new("test.png".into(), FileType::Image).unwrap();
        let json = serde_json::to_value(&f).unwrap();
        assert!(
            json.get("canonical_digest").is_none(),
            "None canonical_digest must be omitted"
        );
    }
}
