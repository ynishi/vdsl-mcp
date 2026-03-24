//! TrackedFile — ファイル実体の射影。
//!
//! ファイルの「身元」のみを管理する。配送のことは知らない。
//! 全フィールドがファイル実体から抽出/計算可能であり、
//! DB全消失してもファイルスキャンで完全復元できる。

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use super::error::SyncError;
use super::file_type::FileType;

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
    registered_at: DateTime<Utc>,
    updated_at: DateTime<Utc>,
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
    ) -> Result<Self, SyncError> {
        if relative_path.is_empty() {
            return Err(SyncError::Validation {
                field: "relative_path".into(),
                reason: "must not be empty".into(),
            });
        }
        if file_hash.is_empty() {
            return Err(SyncError::Validation {
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
            registered_at: now,
            updated_at: now,
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
        registered_at: DateTime<Utc>,
        updated_at: DateTime<Utc>,
    ) -> Self {
        Self {
            id,
            relative_path,
            file_type,
            file_hash,
            content_hash,
            file_size,
            embedded_id,
            registered_at,
            updated_at,
        }
    }

    // =========================================================================
    // Commands
    // =========================================================================

    /// ファイル実体の再スキャン結果でメタデータを更新。
    ///
    /// ハッシュが変わった場合はtrue、変わらなかった場合はfalseを返す。
    /// 呼び出し側はtrueの場合に新しいTransferを作成する。
    pub fn update_from_scan(
        &mut self,
        file_type: FileType,
        file_hash: String,
        content_hash: Option<String>,
        file_size: u64,
        embedded_id: Option<String>,
    ) -> bool {
        let hash_changed = self.file_hash != file_hash;

        self.file_type = file_type;
        self.file_hash = file_hash;
        self.content_hash = content_hash;
        self.file_size = file_size;
        self.embedded_id = embedded_id;
        self.updated_at = Utc::now();

        hash_changed
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

    pub fn registered_at(&self) -> DateTime<Utc> {
        self.registered_at
    }

    pub fn updated_at(&self) -> DateTime<Utc> {
        self.updated_at
    }

    /// 重複検出用の最適ハッシュ。
    /// content_hashがあればそちらを優先（セマンティック一致）、
    /// なければfile_hash（バイト一致）。
    pub fn identity_hash(&self) -> &str {
        self.content_hash.as_deref().unwrap_or(&self.file_hash)
    }

    /// Serialize to [`serde_json::Value`] for cross-boundary transport.
    pub fn to_value(&self) -> Result<serde_json::Value, SyncError> {
        serde_json::to_value(self)
            .map_err(|e| SyncError::Serialization(format!("TrackedFile: {e}")))
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
            FileType::Recipe,
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
            now,
            now,
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
            FileType::Recipe,
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
}
