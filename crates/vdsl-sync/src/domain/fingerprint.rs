//! FileFingerprint — ファイル同一性判定の値オブジェクト。
//!
//! # 画像ファイル = Entity モデル
//!
//! Content（ピクセルデータ）がEntityのIdentity。メタデータやファイルサイズは
//! 副次的属性に過ぎない。メタ変更でsizeが変わっても同一Entityである。
//!
//! # ストレージ種別と取得可能情報
//!
//! - Local/SSH: byte_digest(DJB2) + content_digest + meta_digest + size
//! - Pod/Remote: byte_digest(SHA-256) + size
//! - Cloud (B2/S3): size + modified_at のみ（hash取得不可）
//!
//! # 型安全なDigest
//!
//! [`ByteDigest`](super::digest::ByteDigest) は `PartialEq` 未実装。
//! 異アルゴリズム（DJB2 vs SHA-256）の `==` はコンパイルエラー。
//! 同一location内での比較は [`matches_within_location()`](Self::matches_within_location) のみ。
//! cross-location比較は [`CrossLocationIdentity`](super::digest::CrossLocationIdentity) を使用。
//!
//! # precision() と matches_within_location() の関係
//!
//! この2つのメソッドは**異なる軸**を扱う：
//!
//! - [`FileFingerprint::precision()`] — 単体が**保持する**最高精度（情報量順）。
//!   content_digest > meta_digest > byte_digest > modified_at > size の順。
//!
//! - [`FileFingerprint::matches_within_location()`] — 同一location内の2者比較。
//!   byte_digest > content_digest > meta_digest > size+mtime > size の順。
//!   byte_digestはバイト完全一致を保証するため、比較の確実性が最も高い。
//!   **content_digest/meta_digestはsize gateより前で判定** — メタ変更で
//!   sizeが変わっても同一Entityとして検出する。

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use super::digest::{ByteDigest, ContentDigest, MetaDigest};

/// ファイル同一性判定に使用するフィンガープリント。
///
/// [`matches_within_location()`](Self::matches_within_location) が同一location内で
/// 利用可能な最高精度の情報で比較を行う。
/// cross-location比較には [`CrossLocationIdentity`](super::digest::CrossLocationIdentity) を使用。
///
/// # 精度の優先順位（情報量順）
///
/// 1. `content_digest` (Semantic) — フォーマット固有の意味的ハッシュ（ピクセルデータ等）
/// 2. `meta_digest` (MetaLevel) — 埋め込みメタデータのハッシュ（PNG tEXt, EXIF等）
/// 3. `byte_digest` (ByteLevel) — ファイル全体のバイト列ハッシュ（一致=バイト同一）
/// 4. `size` + `modified_at` (Metadata) — メタデータ比較
/// 5. `size` のみ (SizeOnly) — 最低精度
///
/// # matches_within_location()の比較信頼性順
///
/// 1. `byte_digest` — バイト完全一致（最も確実、同一アルゴリズムのみ）
/// 2. `content_digest` — ピクセル同一性
/// 3. `meta_digest` — メタデータ同一性
/// 4. `size` + `modified_at`
/// 5. `size` のみ
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileFingerprint {
    /// ファイル全体のハッシュ。location固有アルゴリズム（DJB2/SHA-256）。
    /// Cloud Storageではダウンロードなしに取得不可のためNone。
    ///
    /// **PartialEq未実装** — cross-location比較はコンパイルエラー。
    #[serde(default, skip_serializing_if = "Option::is_none", rename = "file_hash")]
    pub byte_digest: Option<ByteDigest>,
    /// フォーマット固有セマンティックハッシュ (PNG IHDR+IDAT 等のピクセルデータ)。
    /// location非依存。PartialEq実装済み。
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        rename = "content_hash"
    )]
    pub content_digest: Option<ContentDigest>,
    /// 埋め込みメタデータのハッシュ (PNG tEXt, EXIF等)。
    /// content_digestと合わせて「何が変わったか」を区別する:
    /// - content_digest一致 + meta_digest不一致 → メタデータだけ変更
    /// - content_digest不一致 → コンテンツ自体が変更
    #[serde(default, skip_serializing_if = "Option::is_none", rename = "meta_hash")]
    pub meta_digest: Option<MetaDigest>,
    /// ファイルサイズ (bytes)。
    pub size: u64,
    /// 最終更新日時 (ストレージ報告値)。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub modified_at: Option<DateTime<Utc>>,
}

/// フィンガープリントが保持する情報の精度レベル。
///
/// 上位ほど豊かな情報を持つ。`Ord` はこの序列に基づく。
///
/// **注意**: `matches_within_location()` の比較優先順序とは異なる。
/// `matches_within_location()` は信頼性順（byte_digest > content_digest > meta_digest）で比較するが、
/// この enum は情報量順（Semantic > MetaLevel > ByteLevel）で序列化する。
/// 詳細はモジュールドキュメントを参照。
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum FingerprintPrecision {
    /// size のみ。同一サイズ・異内容のfalse positive リスクあり。
    SizeOnly = 0,
    /// size + mtime。ストレージのタイムスタンプ精度に依存。
    Metadata = 1,
    /// byte_digest (DJB2/sha256)。バイト列完全一致。
    ByteLevel = 2,
    /// meta_digest (PNG tEXt, EXIF等)。埋め込みメタデータの同一性。
    MetaLevel = 3,
    /// content_digest (PNG IHDR+IDAT 等)。ピクセルデータの意味的同一性。
    Semantic = 4,
}

impl FileFingerprint {
    /// 同一location内でのファイル同一性判定。
    ///
    /// **cross-location比較には使用不可** — `ByteDigest` はlocation固有アルゴリズムのため、
    /// 異location間で比較するとアルゴリズム不一致エラーになる。
    /// cross-location比較には [`CrossLocationIdentity`](super::digest::CrossLocationIdentity) を使用。
    ///
    /// # 画像ファイル = Entity モデル
    ///
    /// Content（ピクセルデータ）がEntityのIdentity。メタデータやファイルサイズは
    /// 副次的属性であり、メタ変更でsizeが変わっても同一Entityである。
    /// そのため content_digest / meta_digest 比較は **size gateより前** に実行し、
    /// hash一致時にsize不一致でも同一と判定する。
    ///
    /// # 信頼性フォールバック順
    ///
    /// 1. 双方に `byte_digest` → 同一アルゴリズム比較（最も確実）
    /// 2. 双方に `content_digest` → ピクセル同一性（size無関係で判定）
    /// 3. 双方に `meta_digest` → メタデータ同一性（size無関係で判定）
    /// 4. `size` gate → 上記digestが全て比較不可能な場合のみフォールバック
    /// 5. 双方に `modified_at` → mtime比較（size一致済み）
    /// 6. size一致のみ → 最低精度（false positiveリスクあり）
    pub fn matches_within_location(&self, other: &FileFingerprint) -> bool {
        // 1. ByteLevel: byte_digest（同一アルゴリズム同士のみ比較）
        if let (Some(a), Some(b)) = (&self.byte_digest, &other.byte_digest) {
            // matches_same_algo は異アルゴリズムでErr。
            // 同一location内なので通常はOk。万一Errならフォールバック。
            match a.matches_same_algo(b) {
                Ok(result) => return result,
                Err(_) => { /* 異アルゴリズム — フォールバック */ }
            }
        }
        // 2. Semantic: content_digest（ピクセルデータ等）
        //    sizeが異なってもcontent_digest一致なら同一Entity
        //    （メタデータ変更でファイルサイズが変わるケースに対応）
        if let (Some(a), Some(b)) = (&self.content_digest, &other.content_digest) {
            return a == b;
        }
        // 3. MetaLevel: meta_digest（埋め込みメタデータ）
        //    content_digestが双方にない場合のフォールバック
        if let (Some(a), Some(b)) = (&self.meta_digest, &other.meta_digest) {
            return a == b;
        }
        // 4. Size gate — digestが全て比較不可能な場合のフォールバック
        //    ※ content_digest/meta_digestがある場合はここに到達しない
        if self.size != other.size {
            return false;
        }
        // 5. Metadata: mtime (size は既に一致)
        if let (Some(a), Some(b)) = (&self.modified_at, &other.modified_at) {
            return a == b;
        }
        // 6. SizeOnly — size一致のみ (false positive リスクあり)
        true
    }

    /// このフィンガープリントが保持する最高精度レベル。
    ///
    /// content_digest（Semantic）を最上位とする情報量の序列。
    /// `matches_within_location()` の比較信頼性順（byte_digest最優先）とは独立した軸である。
    pub fn precision(&self) -> FingerprintPrecision {
        if self.content_digest.is_some() {
            FingerprintPrecision::Semantic
        } else if self.meta_digest.is_some() {
            FingerprintPrecision::MetaLevel
        } else if self.byte_digest.is_some() {
            FingerprintPrecision::ByteLevel
        } else if self.modified_at.is_some() {
            FingerprintPrecision::Metadata
        } else {
            FingerprintPrecision::SizeOnly
        }
    }

    /// 2つのフィンガープリントの比較で使われる実効精度。
    ///
    /// 双方の精度の**低い方**が実効精度となる。
    pub fn effective_precision(&self, other: &FileFingerprint) -> FingerprintPrecision {
        std::cmp::min(self.precision(), other.precision())
    }

    /// ローカルファイルのハッシュ結果から `FileFingerprint` を構築するファクトリ関数。
    ///
    /// `watcher` 等の外部クレートが `ByteDigest` / `ContentDigest` を直接構築せずに
    /// `FileFingerprint` を生成するためのパブリック API。
    ///
    /// # Parameters
    ///
    /// - `file_hash`: DJB2 ハッシュ文字列 (16 文字 hex)
    /// - `content_hash`: PNG 等のセマンティックハッシュ (Optional)
    /// - `size`: ファイルサイズ (bytes)
    /// - `modified_at`: 最終更新日時 (Optional)
    pub fn from_local_hash(
        file_hash: String,
        content_hash: Option<String>,
        size: u64,
        modified_at: Option<DateTime<Utc>>,
    ) -> Self {
        Self {
            byte_digest: Some(ByteDigest::Djb2(file_hash)),
            content_digest: content_hash.map(ContentDigest),
            meta_digest: None,
            size,
            modified_at,
        }
    }
}

impl std::fmt::Display for FingerprintPrecision {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::SizeOnly => f.write_str("size-only"),
            Self::Metadata => f.write_str("metadata"),
            Self::ByteLevel => f.write_str("byte-level"),
            Self::MetaLevel => f.write_str("meta-level"),
            Self::Semantic => f.write_str("semantic"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn hash_fp(
        byte_digest: ByteDigest,
        content_digest: Option<&str>,
        size: u64,
    ) -> FileFingerprint {
        FileFingerprint {
            byte_digest: Some(byte_digest),
            content_digest: content_digest.map(|s| ContentDigest(s.to_string())),
            meta_digest: None,
            size,
            modified_at: None,
        }
    }

    fn hash_fp_with_meta(
        byte_digest: ByteDigest,
        content_digest: Option<&str>,
        meta_digest: Option<&str>,
        size: u64,
    ) -> FileFingerprint {
        FileFingerprint {
            byte_digest: Some(byte_digest),
            content_digest: content_digest.map(|s| ContentDigest(s.to_string())),
            meta_digest: meta_digest.map(|s| MetaDigest(s.to_string())),
            size,
            modified_at: None,
        }
    }

    fn metadata_fp(size: u64, mtime: Option<DateTime<Utc>>) -> FileFingerprint {
        FileFingerprint {
            byte_digest: None,
            content_digest: None,
            meta_digest: None,
            size,
            modified_at: mtime,
        }
    }

    // =========================================================================
    // matches_within_location() — byte_digest 優先（バイト同一性が最も信頼性が高い）
    // =========================================================================

    #[test]
    fn matches_byte_level_trumps_semantic() {
        let a = hash_fp(ByteDigest::Djb2("h1".into()), Some("c1"), 100);
        let b = hash_fp(ByteDigest::Djb2("h1".into()), Some("c2"), 200);
        assert!(a.matches_within_location(&b));
    }

    #[test]
    fn matches_byte_level_different_trumps_semantic() {
        let a = hash_fp(ByteDigest::Djb2("h1".into()), Some("c1"), 100);
        let b = hash_fp(ByteDigest::Djb2("h2".into()), Some("c1"), 100);
        assert!(!a.matches_within_location(&b));
    }

    // =========================================================================
    // matches_within_location() — content_digest フォールバック（byte_digestなし時）
    // =========================================================================

    #[test]
    fn matches_semantic_fallback_same() {
        let a = FileFingerprint {
            byte_digest: None,
            content_digest: Some(ContentDigest("c1".into())),
            meta_digest: None,
            size: 100,
            modified_at: None,
        };
        let b = FileFingerprint {
            byte_digest: None,
            content_digest: Some(ContentDigest("c1".into())),
            meta_digest: None,
            size: 200,
            modified_at: None,
        };
        assert!(a.matches_within_location(&b));
    }

    #[test]
    fn matches_semantic_fallback_different() {
        let a = FileFingerprint {
            byte_digest: None,
            content_digest: Some(ContentDigest("c1".into())),
            meta_digest: None,
            size: 100,
            modified_at: None,
        };
        let b = FileFingerprint {
            byte_digest: None,
            content_digest: Some(ContentDigest("c2".into())),
            meta_digest: None,
            size: 100,
            modified_at: None,
        };
        assert!(!a.matches_within_location(&b));
    }

    // =========================================================================
    // matches_within_location() — metadata (size + mtime)
    // =========================================================================

    #[test]
    fn matches_metadata_same() {
        let t = Utc::now();
        let a = metadata_fp(1024, Some(t));
        let b = metadata_fp(1024, Some(t));
        assert!(a.matches_within_location(&b));
    }

    #[test]
    fn matches_metadata_size_differs() {
        let t = Utc::now();
        let a = metadata_fp(1024, Some(t));
        let b = metadata_fp(2048, Some(t));
        assert!(!a.matches_within_location(&b));
    }

    #[test]
    fn matches_metadata_mtime_differs() {
        let t1 = DateTime::parse_from_rfc3339("2024-01-01T00:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        let t2 = DateTime::parse_from_rfc3339("2024-06-01T00:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        let a = metadata_fp(1024, Some(t1));
        let b = metadata_fp(1024, Some(t2));
        assert!(!a.matches_within_location(&b));
    }

    // =========================================================================
    // matches_within_location() — size only (最低精度)
    // =========================================================================

    #[test]
    fn matches_size_only_same() {
        let a = metadata_fp(1024, None);
        let b = metadata_fp(1024, None);
        assert!(a.matches_within_location(&b));
    }

    #[test]
    fn matches_size_only_different() {
        let a = metadata_fp(1024, None);
        let b = metadata_fp(2048, None);
        assert!(!a.matches_within_location(&b));
    }

    // =========================================================================
    // matches_within_location() — 異精度の比較
    // =========================================================================

    #[test]
    fn matches_hash_vs_metadata_size_match() {
        let a = hash_fp(ByteDigest::Djb2("h1".into()), None, 1024);
        let b = metadata_fp(1024, None);
        assert!(a.matches_within_location(&b));
    }

    #[test]
    fn matches_hash_vs_metadata_size_differs() {
        let a = hash_fp(ByteDigest::Djb2("h1".into()), None, 1024);
        let b = metadata_fp(2048, None);
        assert!(!a.matches_within_location(&b));
    }

    // =========================================================================
    // matches_within_location() — 異アルゴリズムはフォールバック
    // =========================================================================

    #[test]
    fn cross_algorithm_falls_back_to_size() {
        // 同一location内では通常起きないが、万一の安全策
        let a = hash_fp(ByteDigest::Djb2("h1".into()), None, 1024);
        let b = hash_fp(ByteDigest::Sha256("h2".into()), None, 1024);
        // byte_digest比較はErr → size比較にフォールバック → size一致でtrue
        assert!(a.matches_within_location(&b));
    }

    // =========================================================================
    // precision()
    // =========================================================================

    #[test]
    fn precision_semantic() {
        let fp = hash_fp(ByteDigest::Djb2("h".into()), Some("c"), 100);
        assert_eq!(fp.precision(), FingerprintPrecision::Semantic);
    }

    #[test]
    fn precision_byte_level() {
        let fp = hash_fp(ByteDigest::Djb2("h".into()), None, 100);
        assert_eq!(fp.precision(), FingerprintPrecision::ByteLevel);
    }

    #[test]
    fn precision_metadata() {
        let fp = metadata_fp(100, Some(Utc::now()));
        assert_eq!(fp.precision(), FingerprintPrecision::Metadata);
    }

    #[test]
    fn precision_size_only() {
        let fp = metadata_fp(100, None);
        assert_eq!(fp.precision(), FingerprintPrecision::SizeOnly);
    }

    // =========================================================================
    // effective_precision()
    // =========================================================================

    #[test]
    fn effective_precision_downgrades() {
        let hash = hash_fp(ByteDigest::Djb2("h".into()), Some("c"), 100);
        let meta = metadata_fp(100, Some(Utc::now()));
        assert_eq!(
            hash.effective_precision(&meta),
            FingerprintPrecision::Metadata
        );
    }

    #[test]
    fn effective_precision_same_level() {
        let a = hash_fp(ByteDigest::Djb2("h1".into()), None, 100);
        let b = hash_fp(ByteDigest::Djb2("h2".into()), None, 200);
        assert_eq!(a.effective_precision(&b), FingerprintPrecision::ByteLevel);
    }

    // =========================================================================
    // Display
    // =========================================================================

    #[test]
    fn precision_display() {
        assert_eq!(FingerprintPrecision::Semantic.to_string(), "semantic");
        assert_eq!(FingerprintPrecision::MetaLevel.to_string(), "meta-level");
        assert_eq!(FingerprintPrecision::ByteLevel.to_string(), "byte-level");
        assert_eq!(FingerprintPrecision::Metadata.to_string(), "metadata");
        assert_eq!(FingerprintPrecision::SizeOnly.to_string(), "size-only");
    }

    // =========================================================================
    // meta_digest — メタデータ同一性
    // =========================================================================

    #[test]
    fn matches_meta_digest_fallback_same() {
        let a = FileFingerprint {
            byte_digest: None,
            content_digest: None,
            meta_digest: Some(MetaDigest("m1".into())),
            size: 100,
            modified_at: None,
        };
        let b = FileFingerprint {
            byte_digest: None,
            content_digest: None,
            meta_digest: Some(MetaDigest("m1".into())),
            size: 200,
            modified_at: None,
        };
        assert!(a.matches_within_location(&b));
    }

    #[test]
    fn matches_meta_digest_fallback_different() {
        let a = FileFingerprint {
            byte_digest: None,
            content_digest: None,
            meta_digest: Some(MetaDigest("m1".into())),
            size: 100,
            modified_at: None,
        };
        let b = FileFingerprint {
            byte_digest: None,
            content_digest: None,
            meta_digest: Some(MetaDigest("m2".into())),
            size: 100,
            modified_at: None,
        };
        assert!(!a.matches_within_location(&b));
    }

    #[test]
    fn content_same_meta_different_means_meta_only_change() {
        let a = hash_fp_with_meta(ByteDigest::Djb2("h1".into()), Some("c1"), Some("m1"), 1024);
        let b = hash_fp_with_meta(ByteDigest::Djb2("h2".into()), Some("c1"), Some("m2"), 1024);
        // byte_digest不一致 → matches = false（バイトレベルでは別物）
        assert!(!a.matches_within_location(&b));
    }

    #[test]
    fn precision_meta_level() {
        let fp = FileFingerprint {
            byte_digest: None,
            content_digest: None,
            meta_digest: Some(MetaDigest("m1".into())),
            size: 100,
            modified_at: None,
        };
        assert_eq!(fp.precision(), FingerprintPrecision::MetaLevel);
    }

    #[test]
    fn precision_semantic_trumps_meta() {
        let fp = FileFingerprint {
            byte_digest: None,
            content_digest: Some(ContentDigest("c1".into())),
            meta_digest: Some(MetaDigest("m1".into())),
            size: 100,
            modified_at: None,
        };
        assert_eq!(fp.precision(), FingerprintPrecision::Semantic);
    }

    #[test]
    fn precision_ordering() {
        assert!(FingerprintPrecision::SizeOnly < FingerprintPrecision::Metadata);
        assert!(FingerprintPrecision::Metadata < FingerprintPrecision::ByteLevel);
        assert!(FingerprintPrecision::ByteLevel < FingerprintPrecision::MetaLevel);
        assert!(FingerprintPrecision::MetaLevel < FingerprintPrecision::Semantic);
    }

    // =========================================================================
    // Entity model — Content = Identity
    // =========================================================================

    #[test]
    fn entity_model_meta_change_does_not_break_identity() {
        let before = FileFingerprint {
            byte_digest: None,
            content_digest: Some(ContentDigest("pixel_hash_abc".into())),
            meta_digest: Some(MetaDigest("meta_v1".into())),
            size: 10240,
            modified_at: None,
        };
        let after = FileFingerprint {
            byte_digest: None,
            content_digest: Some(ContentDigest("pixel_hash_abc".into())),
            meta_digest: Some(MetaDigest("meta_v2".into())),
            size: 10300,
            modified_at: None,
        };
        assert!(before.matches_within_location(&after));
    }

    #[test]
    fn entity_model_content_change_is_detected() {
        let v1 = FileFingerprint {
            byte_digest: None,
            content_digest: Some(ContentDigest("pixel_v1".into())),
            meta_digest: Some(MetaDigest("meta_v1".into())),
            size: 10240,
            modified_at: None,
        };
        let v2 = FileFingerprint {
            byte_digest: None,
            content_digest: Some(ContentDigest("pixel_v2".into())),
            meta_digest: Some(MetaDigest("meta_v1".into())),
            size: 10240,
            modified_at: None,
        };
        assert!(!v1.matches_within_location(&v2));
    }

    #[test]
    fn entity_model_reexport_with_ts_in_meta() {
        let original = hash_fp_with_meta(
            ByteDigest::Djb2("file_h1".into()),
            Some("pixel_abc"),
            Some("meta_ts1"),
            10240,
        );
        let reexport = hash_fp_with_meta(
            ByteDigest::Djb2("file_h2".into()),
            Some("pixel_abc"),
            Some("meta_ts2"),
            10300,
        );
        assert!(!original.matches_within_location(&reexport));
        assert_eq!(
            original.content_digest.as_ref().map(|cd| cd.as_str()),
            reexport.content_digest.as_ref().map(|cd| cd.as_str()),
        );
    }
}
