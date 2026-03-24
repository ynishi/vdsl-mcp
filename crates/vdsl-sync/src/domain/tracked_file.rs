//! TrackedFile — ファイル実体の射影。
//!
//! ファイルの「身元」のみを管理する。配送のことは知らない。
//! 全フィールドがファイル実体から抽出/計算可能であり、
//! DB全消失してもファイルスキャンで完全復元できる。

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use super::error::DomainError;
use super::file_type::FileType;
use super::fingerprint::FileFingerprint;

/// Cloud Storageファイルの仮hash prefix。
///
/// Cloud検出時はfile_hashが不明なため `"size:{bytes}"` 形式の仮hashを使用する。
/// この値は [`FileFingerprint`] 構築時に `file_hash: None` として扱われ、
/// 精度が正確に Metadata/SizeOnly として反映される。
const PLACEHOLDER_HASH_PREFIX: &str = "size:";

/// 追跡対象ファイルの身元情報。
///
/// # 設計原則
///
/// - ファイル実体が唯一の真実の源 (Source of Truth)
/// - DBはこの構造体の射影/インデックスに過ぎない
/// - location状態（どこにあるか）は持たない — [`Transfer`](super::transfer::Transfer) が管理
///
/// # フィールドの由来
///
/// | フィールド | 抽出元 |
/// |---|---|
/// | `relative_path` | ファイルパス (sync_root からの相対) |
/// | `file_type` | 拡張子から判定 |
/// | `file_hash` | ファイル全体のDJB2 |
/// | `content_hash` | フォーマット固有ハッシュ (PNG IHDR+IDAT等) |
/// | `file_size` | `fs::metadata().len()` |
/// | `embedded_id` | ファイル内メタデータから抽出 (PNG tEXt, JSON field等) |
#[deprecated(
    note = "use TopologyFile + LocationFile — TrackedFile mixes identity and location state"
)]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TrackedFile {
    id: String,
    relative_path: String,
    file_type: FileType,
    file_hash: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    content_hash: Option<String>,
    file_size: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    embedded_id: Option<String>,

    /// ストレージが報告するファイルの最終更新日時。
    /// Cloud Storageの場合に設定される。ローカルファイルはNone（hashで判定するため）。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    modified_at: Option<DateTime<Utc>>,
    registered_at: DateTime<Utc>,
    updated_at: DateTime<Utc>,
    /// ファイルが削除検出された日時。Noneなら生存中。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    deleted_at: Option<DateTime<Utc>>,
}

impl TrackedFile {
    // =========================================================================
    // Factory
    // =========================================================================

    /// ファイル実体から構築。スキャンによるDB復元でも使用。
    ///
    /// idはUUID v4で自動生成。registered_at/updated_atは現在時刻。
    ///
    /// # Errors
    ///
    /// - `relative_path` が空文字列の場合
    /// - `file_hash` が空文字列の場合
    pub fn from_scan(
        relative_path: String,
        file_type: FileType,
        file_hash: String,
        content_hash: Option<String>,
        file_size: u64,
        embedded_id: Option<String>,
    ) -> Result<Self, DomainError> {
        if relative_path.is_empty() {
            return Err(DomainError::Validation {
                field: "relative_path".into(),
                reason: "must not be empty".into(),
            });
        }
        if file_hash.is_empty() {
            return Err(DomainError::Validation {
                field: "file_hash".into(),
                reason: "must not be empty".into(),
            });
        }
        let now = Utc::now();
        Ok(Self {
            id: uuid::Uuid::new_v4().to_string(),
            relative_path,
            file_type,
            file_hash,
            content_hash,
            file_size,
            embedded_id,
            modified_at: None,
            registered_at: now,
            updated_at: now,
            deleted_at: None,
        })
    }

    /// Cloud Storage検出ファイル用ファクトリ。
    ///
    /// file_hashが不明なため、sizeベースの仮hashを使用。
    /// ローカルにpull後、`update_from_scan()`でDJB2に置換される。
    pub fn from_cloud_scan(
        relative_path: String,
        file_type: FileType,
        size: u64,
        modified_at: Option<DateTime<Utc>>,
    ) -> Result<Self, DomainError> {
        if relative_path.is_empty() {
            return Err(DomainError::Validation {
                field: "relative_path".into(),
                reason: "must not be empty".into(),
            });
        }
        let now = Utc::now();
        Ok(Self {
            id: uuid::Uuid::new_v4().to_string(),
            relative_path,
            file_type,
            // sizeベース仮hash — pull後にDJB2で上書きされる
            file_hash: format!("{PLACEHOLDER_HASH_PREFIX}{size}"),
            content_hash: None,
            file_size: size,
            embedded_id: None,
            modified_at,
            registered_at: now,
            updated_at: now,
            deleted_at: None,
        })
    }

    /// DB復元用。永続化済みデータからの再構成。
    ///
    /// ドメイン不変条件の検証をバイパスする。データは初回作成時に
    /// 検証済みであることが前提。永続化インフラ専用。
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn reconstitute(
        id: String,
        relative_path: String,
        file_type: FileType,
        file_hash: String,
        content_hash: Option<String>,
        file_size: u64,
        embedded_id: Option<String>,
        modified_at: Option<DateTime<Utc>>,
        registered_at: DateTime<Utc>,
        updated_at: DateTime<Utc>,
        deleted_at: Option<DateTime<Utc>>,
    ) -> Self {
        Self {
            id,
            relative_path,
            file_type,
            file_hash,
            content_hash,
            file_size,
            embedded_id,
            modified_at,
            registered_at,
            updated_at,
            deleted_at,
        }
    }

    // =========================================================================
    // Commands
    // =========================================================================

    /// 削除済みマーク。削除伝播Transfer発行後に呼ぶ。
    ///
    /// 既にdeleted_atが設定済みの場合は何もしない（冪等）。
    pub fn mark_deleted(&mut self) {
        if self.deleted_at.is_none() {
            let now = Utc::now();
            self.deleted_at = Some(now);
            self.updated_at = now;
        }
    }

    /// 削除済みマークを解除（再登録時に使用）。
    pub fn unmark_deleted(&mut self) {
        self.deleted_at = None;
        self.updated_at = Utc::now();
    }

    /// ファイル実体の再スキャン結果でメタデータを更新。
    ///
    /// 変更があった場合はtrue、なかった場合はfalseを返す。
    /// 呼び出し側はtrueの場合に新しいTransferを作成する。
    ///
    /// 変更検知は [`FileFingerprint::matches()`] に委譲する。
    pub fn update_from_scan(
        &mut self,
        file_type: FileType,
        file_hash: String,
        content_hash: Option<String>,
        file_size: u64,
        embedded_id: Option<String>,
    ) -> bool {
        let scan_fp = FileFingerprint {
            file_hash: Some(file_hash.clone()),
            content_hash: content_hash.clone(),
            meta_hash: None, // TrackedFile(旧モデル)はmeta_hashを直接保持しない
            size: file_size,
            modified_at: None,
        };
        let changed = self.has_changed(&scan_fp);

        // hash精度昇格（Cloud仮hash→実hash）もメタデータ更新が必要
        let hash_upgraded = !self.has_real_file_hash();

        self.file_type = file_type;
        self.file_hash = file_hash;
        self.content_hash = content_hash;
        self.file_size = file_size;
        self.embedded_id = embedded_id;
        self.modified_at = None; // ローカルスキャン時はmtimeリセット

        if changed || hash_upgraded {
            self.updated_at = Utc::now();
        }

        changed
    }

    /// Cloud Storageのメタデータでファイル情報を更新。
    ///
    /// file_hashは不明のまま（sizeベース仮hash維持）。
    /// 変更があった場合はtrue。
    pub fn update_from_cloud_scan(
        &mut self,
        size: u64,
        modified_at: Option<DateTime<Utc>>,
    ) -> bool {
        let scan_fp = FileFingerprint {
            file_hash: None,
            content_hash: None,
            meta_hash: None,
            size,
            modified_at,
        };
        let changed = self.has_changed(&scan_fp);

        if changed {
            self.file_hash = format!("{PLACEHOLDER_HASH_PREFIX}{size}");
            self.file_size = size;
            self.modified_at = modified_at;
            self.updated_at = Utc::now();
        }

        changed
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

    pub fn file_type(&self) -> FileType {
        self.file_type
    }

    pub fn file_hash(&self) -> &str {
        &self.file_hash
    }

    pub fn content_hash(&self) -> Option<&str> {
        self.content_hash.as_deref()
    }

    pub fn file_size(&self) -> u64 {
        self.file_size
    }

    pub fn embedded_id(&self) -> Option<&str> {
        self.embedded_id.as_deref()
    }

    pub fn modified_at(&self) -> Option<DateTime<Utc>> {
        self.modified_at
    }

    /// ファイルシステムのmtimeを設定。incremental scan用。
    pub fn set_modified_at(&mut self, mtime: Option<DateTime<Utc>>) {
        self.modified_at = mtime;
    }

    /// 現在の身元情報からフィンガープリントを構築。
    ///
    /// Cloud Storage由来の仮hash (`"size:..."`) は実バイトハッシュではないため
    /// `file_hash: None` として扱い、精度が Metadata/SizeOnly に正しく反映される。
    pub fn fingerprint(&self) -> FileFingerprint {
        let real_hash = if self.file_hash.starts_with(PLACEHOLDER_HASH_PREFIX) {
            None
        } else {
            Some(self.file_hash.clone())
        };
        FileFingerprint {
            file_hash: real_hash,
            content_hash: self.content_hash.clone(),
            meta_hash: None, // TrackedFile(旧モデル)はmeta_hashを直接保持しない
            size: self.file_size,
            modified_at: self.modified_at,
        }
    }

    /// file_hashが実バイトハッシュか（仮hashでないか）。
    pub fn has_real_file_hash(&self) -> bool {
        !self.file_hash.starts_with(PLACEHOLDER_HASH_PREFIX)
    }

    /// スキャン結果のフィンガープリントと比較し、変更があればtrue。
    pub fn has_changed(&self, scan: &FileFingerprint) -> bool {
        !self.fingerprint().matches(scan)
    }

    pub fn registered_at(&self) -> DateTime<Utc> {
        self.registered_at
    }

    pub fn updated_at(&self) -> DateTime<Utc> {
        self.updated_at
    }

    pub fn deleted_at(&self) -> Option<DateTime<Utc>> {
        self.deleted_at
    }

    pub fn is_deleted(&self) -> bool {
        self.deleted_at.is_some()
    }

    /// 重複検出用の最適ハッシュ。
    /// content_hashがあればそちらを優先（セマンティック一致）、
    /// なければfile_hash（バイト一致）。
    pub fn identity_hash(&self) -> &str {
        self.content_hash.as_deref().unwrap_or(&self.file_hash)
    }

    /// Serialize to [`serde_json::Value`] for cross-boundary transport.
    pub fn to_value(&self) -> Result<serde_json::Value, serde_json::Error> {
        serde_json::to_value(self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_file() -> TrackedFile {
        TrackedFile::from_scan(
            "output/gen-001.png".into(),
            FileType::Image,
            "abc123".into(),
            Some("def456".into()),
            1024,
            Some("gen-001".into()),
        )
        .expect("valid test data")
    }

    #[test]
    fn from_scan_sets_fields() {
        let f = sample_file();
        assert_eq!(f.relative_path(), "output/gen-001.png");
        assert_eq!(f.file_type(), FileType::Image);
        assert_eq!(f.file_hash(), "abc123");
        assert_eq!(f.content_hash(), Some("def456"));
        assert_eq!(f.file_size(), 1024);
        assert_eq!(f.embedded_id(), Some("gen-001"));
        assert!(!f.id().is_empty());
    }

    #[test]
    fn update_from_scan_returns_true_on_hash_change() {
        let mut f = sample_file();
        let changed = f.update_from_scan(FileType::Image, "new_hash".into(), None, 2048, None);
        assert!(changed);
        assert_eq!(f.file_hash(), "new_hash");
        assert_eq!(f.content_hash(), None);
        assert_eq!(f.file_size(), 2048);
        assert_eq!(f.embedded_id(), None);
    }

    #[test]
    fn update_from_scan_returns_false_on_same_hash() {
        let mut f = sample_file();
        let changed = f.update_from_scan(
            FileType::Image,
            "abc123".into(), // same
            Some("new_content".into()),
            2048,
            Some("gen-002".into()),
        );
        assert!(!changed);
        assert_eq!(f.content_hash(), Some("new_content"));
        assert_eq!(f.file_size(), 2048);
        assert_eq!(f.embedded_id(), Some("gen-002"));
    }

    #[test]
    fn identity_hash_prefers_content_hash() {
        let f = sample_file();
        assert_eq!(f.identity_hash(), "def456");
    }

    #[test]
    fn identity_hash_falls_back_to_file_hash() {
        let f = TrackedFile::from_scan(
            "data.json".into(),
            FileType::Asset,
            "abc123".into(),
            None,
            64,
            None,
        )
        .expect("valid test data");
        assert_eq!(f.identity_hash(), "abc123");
    }

    #[test]
    fn reconstitute_preserves_all_fields() {
        let now = Utc::now();
        let f = TrackedFile::reconstitute(
            "id-1".into(),
            "path.png".into(),
            FileType::Image,
            "hash".into(),
            Some("chash".into()),
            512,
            Some("emb".into()),
            None, // modified_at
            now,
            now,
            None,
        );
        assert_eq!(f.id(), "id-1");
        assert_eq!(f.relative_path(), "path.png");
        assert_eq!(f.registered_at(), now);
    }

    #[test]
    fn serde_roundtrip() {
        let f = sample_file();
        let json = serde_json::to_value(&f).unwrap();
        let restored: TrackedFile = serde_json::from_value(json).unwrap();
        assert_eq!(restored.file_hash(), f.file_hash());
        assert_eq!(restored.content_hash(), f.content_hash());
        assert_eq!(restored.embedded_id(), f.embedded_id());
    }

    #[test]
    fn serde_omits_none_fields() {
        let f = TrackedFile::from_scan(
            "data.json".into(),
            FileType::Asset,
            "hash".into(),
            None,
            64,
            None,
        )
        .expect("valid test data");
        let json = serde_json::to_value(&f).unwrap();
        assert!(json.get("content_hash").is_none(), "None must be omitted");
        assert!(json.get("embedded_id").is_none(), "None must be omitted");
    }

    #[test]
    fn from_scan_rejects_empty_relative_path() {
        let result = TrackedFile::from_scan(
            "".into(),
            FileType::Image,
            "abc123".into(),
            None,
            1024,
            None,
        );
        assert!(result.is_err());
    }

    #[test]
    fn from_scan_rejects_empty_file_hash() {
        let result = TrackedFile::from_scan(
            "output/test.png".into(),
            FileType::Image,
            "".into(),
            None,
            1024,
            None,
        );
        assert!(result.is_err());
    }

    // =========================================================================
    // fingerprint() — 仮hashフィルタ
    // =========================================================================

    #[test]
    fn fingerprint_local_file_has_file_hash() {
        let f = sample_file(); // from_scan → 実hash "abc123"
        let fp = f.fingerprint();
        assert_eq!(fp.file_hash.as_deref(), Some("abc123"));
        assert!(f.has_real_file_hash());
    }

    #[test]
    fn fingerprint_cloud_file_has_no_file_hash() {
        let f = TrackedFile::from_cloud_scan("cloud/photo.png".into(), FileType::Image, 2048, None)
            .expect("valid");
        let fp = f.fingerprint();
        assert!(
            fp.file_hash.is_none(),
            "Cloud仮hashはfingerprintではNoneになるべき"
        );
        assert!(!f.has_real_file_hash());
    }

    #[test]
    fn fingerprint_cloud_same_size_not_byte_level_match() {
        // 同sizeのCloud同士がByteLevel一致と誤判定されないことを検証
        let a = TrackedFile::from_cloud_scan("a.png".into(), FileType::Image, 1024, None)
            .expect("valid");
        let b = TrackedFile::from_cloud_scan("b.png".into(), FileType::Image, 1024, None)
            .expect("valid");
        let fp_a = a.fingerprint();
        let fp_b = b.fingerprint();
        // 双方file_hash=None → size比較にフォールバック（SizeOnly精度）
        assert!(fp_a.matches(&fp_b));
        assert_eq!(
            fp_a.effective_precision(&fp_b),
            super::super::fingerprint::FingerprintPrecision::SizeOnly
        );
    }

    #[test]
    fn cloud_file_upgraded_after_local_scan_same_size() {
        // 同sizeでhash精度だけ上がる場合 → ファイル実体は同一なのでchanged=false
        let mut f = TrackedFile::from_cloud_scan("photo.png".into(), FileType::Image, 2048, None)
            .expect("valid");
        assert!(!f.has_real_file_hash());

        let changed =
            f.update_from_scan(FileType::Image, "djb2_real_hash".into(), None, 2048, None);
        assert!(!changed, "同sizeファイルのhash精度向上はTransfer不要");
        // フィールドは更新される（update_from_scanは常に全フィールド書き換え）
        assert!(f.has_real_file_hash());
        assert_eq!(f.fingerprint().file_hash.as_deref(), Some("djb2_real_hash"));
    }

    #[test]
    fn cloud_file_upgraded_after_local_scan_different_size() {
        // sizeが異なる場合 → 実体が変わっているのでchanged=true
        let mut f = TrackedFile::from_cloud_scan("photo.png".into(), FileType::Image, 2048, None)
            .expect("valid");

        let changed = f.update_from_scan(FileType::Image, "djb2_new".into(), None, 4096, None);
        assert!(changed, "size変化はファイル実体の変更");
        assert!(f.has_real_file_hash());
    }
}
