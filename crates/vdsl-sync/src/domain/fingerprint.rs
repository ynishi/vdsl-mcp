//! FileFingerprint — ファイル同一性判定の値オブジェクト。
//!
//! # 画像ファイル = Entity モデル
//!
//! Content（ピクセルデータ）がEntityのIdentity。メタデータやファイルサイズは
//! 副次的属性に過ぎない。メタ変更でsizeが変わっても同一Entityである。
//!
//! # ストレージ種別と取得可能情報
//!
//! - Local/SSH: file_hash + content_hash + meta_hash + size
//! - Cloud (B2/S3): size + modified_at のみ（hash取得不可）
//!
//! 比較ロジックを1箇所に集約し、精度レベルを型として可視化する。
//!
//! # precision() と matches() の関係
//!
//! この2つのメソッドは**異なる軸**を扱う：
//!
//! - [`FileFingerprint::precision()`] — 単体が**保持する**最高精度（情報量順）。
//!   content_hash > meta_hash > file_hash > modified_at > size の順。
//!
//! - [`FileFingerprint::matches()`] — 2者を比較する際の**信頼性**順。
//!   file_hash > content_hash > meta_hash > size+mtime > size の順。
//!   file_hashはバイト完全一致を保証するため、比較の確実性が最も高い。
//!   **content_hash/meta_hashはsize gateより前で判定** — メタ変更で
//!   sizeが変わっても同一Entityとして検出する。

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// ファイル同一性判定に使用するフィンガープリント。
///
/// [`matches()`](Self::matches) が利用可能な最高精度の情報で比較を行う。
/// 精度は [`precision()`](Self::precision) で確認可能。
///
/// # 精度の優先順位（情報量順）
///
/// 1. `content_hash` (Semantic) — フォーマット固有の意味的ハッシュ（ピクセルデータ等）
/// 2. `meta_hash` (MetaLevel) — 埋め込みメタデータのハッシュ（PNG tEXt, EXIF等）
/// 3. `file_hash` (ByteLevel) — ファイル全体のバイト列ハッシュ（一致=バイト同一）
/// 4. `size` + `modified_at` (Metadata) — メタデータ比較
/// 5. `size` のみ (SizeOnly) — 最低精度
///
/// # matches()の比較信頼性順
///
/// 1. `file_hash` — バイト完全一致（最も確実）
/// 2. `content_hash` — ピクセル同一性
/// 3. `meta_hash` — メタデータ同一性
/// 4. `size` + `modified_at`
/// 5. `size` のみ
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FileFingerprint {
    /// ファイル全体のハッシュ (DJB2, sha256 等)。
    /// Cloud Storageではダウンロードなしに取得不可のためNone。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub file_hash: Option<String>,
    /// フォーマット固有セマンティックハッシュ (PNG IHDR+IDAT 等のピクセルデータ)。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub content_hash: Option<String>,
    /// 埋め込みメタデータのハッシュ (PNG tEXt, EXIF等)。
    /// content_hashと合わせて「何が変わったか」を区別する:
    /// - content_hash一致 + meta_hash不一致 → メタデータだけ変更
    /// - content_hash不一致 → コンテンツ自体が変更
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub meta_hash: Option<String>,
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
/// **注意**: `matches()` の比較優先順序とは異なる。
/// `matches()` は信頼性順（file_hash > content_hash > meta_hash）で比較するが、
/// この enum は情報量順（Semantic > MetaLevel > ByteLevel）で序列化する。
/// 詳細はモジュールドキュメントを参照。
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum FingerprintPrecision {
    /// size のみ。同一サイズ・異内容のfalse positive リスクあり。
    SizeOnly = 0,
    /// size + mtime。ストレージのタイムスタンプ精度に依存。
    Metadata = 1,
    /// file_hash (DJB2/sha256)。バイト列完全一致。
    ByteLevel = 2,
    /// meta_hash (PNG tEXt, EXIF等)。埋め込みメタデータの同一性。
    MetaLevel = 3,
    /// content_hash (PNG IHDR+IDAT 等)。ピクセルデータの意味的同一性。
    Semantic = 4,
}

impl FileFingerprint {
    /// 利用可能な最も信頼性の高い情報で同一ファイルか判定する。
    ///
    /// # 画像ファイル = Entity モデル
    ///
    /// Content（ピクセルデータ）がEntityのIdentity。メタデータやファイルサイズは
    /// 副次的属性であり、メタ変更でsizeが変わっても同一Entityである。
    /// そのため content_hash / meta_hash 比較は **size gateより前** に実行し、
    /// hash一致時にsize不一致でも同一と判定する。
    ///
    /// # 信頼性フォールバック順
    ///
    /// 1. 双方に `file_hash` → バイト完全一致（最も確実）
    /// 2. 双方に `content_hash` → ピクセル同一性（size無関係で判定）
    /// 3. 双方に `meta_hash` → メタデータ同一性（size無関係で判定）
    /// 4. `size` gate → 上記hashが全て比較不可能な場合のみフォールバック
    /// 5. 双方に `modified_at` → mtime比較（size一致済み）
    /// 6. size一致のみ → 最低精度（false positiveリスクあり）
    pub fn matches(&self, other: &FileFingerprint) -> bool {
        // 1. ByteLevel: file_hash（バイト列全体のハッシュ。一致=ファイル同一）
        if let (Some(a), Some(b)) = (&self.file_hash, &other.file_hash) {
            return a == b;
        }
        // 2. Semantic: content_hash（ピクセルデータ等）
        //    sizeが異なってもcontent_hash一致なら同一Entity
        //    （メタデータ変更でファイルサイズが変わるケースに対応）
        if let (Some(a), Some(b)) = (&self.content_hash, &other.content_hash) {
            return a == b;
        }
        // 3. MetaLevel: meta_hash（埋め込みメタデータ）
        //    content_hashが双方にない場合のフォールバック
        if let (Some(a), Some(b)) = (&self.meta_hash, &other.meta_hash) {
            return a == b;
        }
        // 4. Size gate — hashが全て比較不可能な場合のフォールバック
        //    ※ content_hash/meta_hashがある場合はここに到達しない
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
    /// content_hash（Semantic）を最上位とする情報量の序列。
    /// `matches()` の比較信頼性順（file_hash最優先）とは独立した軸である。
    pub fn precision(&self) -> FingerprintPrecision {
        if self.content_hash.is_some() {
            FingerprintPrecision::Semantic
        } else if self.meta_hash.is_some() {
            FingerprintPrecision::MetaLevel
        } else if self.file_hash.is_some() {
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

    fn hash_fp(file_hash: &str, content_hash: Option<&str>, size: u64) -> FileFingerprint {
        FileFingerprint {
            file_hash: Some(file_hash.to_string()),
            content_hash: content_hash.map(|s| s.to_string()),
            meta_hash: None,
            size,
            modified_at: None,
        }
    }

    fn hash_fp_with_meta(
        file_hash: &str,
        content_hash: Option<&str>,
        meta_hash: Option<&str>,
        size: u64,
    ) -> FileFingerprint {
        FileFingerprint {
            file_hash: Some(file_hash.to_string()),
            content_hash: content_hash.map(|s| s.to_string()),
            meta_hash: meta_hash.map(|s| s.to_string()),
            size,
            modified_at: None,
        }
    }

    fn metadata_fp(size: u64, mtime: Option<DateTime<Utc>>) -> FileFingerprint {
        FileFingerprint {
            file_hash: None,
            content_hash: None,
            meta_hash: None,
            size,
            modified_at: mtime,
        }
    }

    // =========================================================================
    // matches() — file_hash 優先（バイト同一性が最も信頼性が高い）
    // =========================================================================

    #[test]
    fn matches_byte_level_trumps_semantic() {
        let a = hash_fp("h1", Some("c1"), 100);
        let b = hash_fp("h1", Some("c2"), 200); // file_hash一致 → content_hash/size無関係で同一
        assert!(a.matches(&b));
    }

    #[test]
    fn matches_byte_level_different_trumps_semantic() {
        let a = hash_fp("h1", Some("c1"), 100);
        let b = hash_fp("h2", Some("c1"), 100); // file_hash不一致 → content_hash一致でも異なる
        assert!(!a.matches(&b));
    }

    // =========================================================================
    // matches() — content_hash フォールバック（file_hashなし時）
    // =========================================================================

    #[test]
    fn matches_semantic_fallback_same() {
        // file_hashがNone → content_hashにフォールバック
        let a = FileFingerprint {
            file_hash: None,
            content_hash: Some("c1".to_string()),
            meta_hash: None,
            size: 100,
            modified_at: None,
        };
        let b = FileFingerprint {
            file_hash: None,
            content_hash: Some("c1".to_string()),
            meta_hash: None,
            size: 200,
            modified_at: None,
        };
        assert!(a.matches(&b));
    }

    #[test]
    fn matches_semantic_fallback_different() {
        let a = FileFingerprint {
            file_hash: None,
            content_hash: Some("c1".to_string()),
            meta_hash: None,
            size: 100,
            modified_at: None,
        };
        let b = FileFingerprint {
            file_hash: None,
            content_hash: Some("c2".to_string()),
            meta_hash: None,
            size: 100,
            modified_at: None,
        };
        assert!(!a.matches(&b));
    }

    // =========================================================================
    // matches() — metadata (size + mtime)
    // =========================================================================

    #[test]
    fn matches_metadata_same() {
        let t = Utc::now();
        let a = metadata_fp(1024, Some(t));
        let b = metadata_fp(1024, Some(t));
        assert!(a.matches(&b));
    }

    #[test]
    fn matches_metadata_size_differs() {
        let t = Utc::now();
        let a = metadata_fp(1024, Some(t));
        let b = metadata_fp(2048, Some(t));
        assert!(!a.matches(&b));
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
        assert!(!a.matches(&b));
    }

    // =========================================================================
    // matches() — size only (最低精度)
    // =========================================================================

    #[test]
    fn matches_size_only_same() {
        let a = metadata_fp(1024, None);
        let b = metadata_fp(1024, None);
        assert!(a.matches(&b)); // size一致 → true (false positive リスクあり)
    }

    #[test]
    fn matches_size_only_different() {
        let a = metadata_fp(1024, None);
        let b = metadata_fp(2048, None);
        assert!(!a.matches(&b));
    }

    // =========================================================================
    // matches() — 異精度の比較
    // =========================================================================

    #[test]
    fn matches_hash_vs_metadata_size_match() {
        // hash側にはfile_hashがあるが、metadata側にはない
        // → file_hash比較不可、size比較にフォールバック
        let a = hash_fp("h1", None, 1024);
        let b = metadata_fp(1024, None);
        assert!(a.matches(&b)); // size一致で true
    }

    #[test]
    fn matches_hash_vs_metadata_size_differs() {
        let a = hash_fp("h1", None, 1024);
        let b = metadata_fp(2048, None);
        assert!(!a.matches(&b));
    }

    // =========================================================================
    // precision()
    // =========================================================================

    #[test]
    fn precision_semantic() {
        let fp = hash_fp("h", Some("c"), 100);
        assert_eq!(fp.precision(), FingerprintPrecision::Semantic);
    }

    #[test]
    fn precision_byte_level() {
        let fp = hash_fp("h", None, 100);
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
        let hash = hash_fp("h", Some("c"), 100);
        let meta = metadata_fp(100, Some(Utc::now()));
        assert_eq!(
            hash.effective_precision(&meta),
            FingerprintPrecision::Metadata
        );
    }

    #[test]
    fn effective_precision_same_level() {
        let a = hash_fp("h1", None, 100);
        let b = hash_fp("h2", None, 200);
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
    // meta_hash — メタデータ同一性
    // =========================================================================

    #[test]
    fn matches_meta_hash_fallback_same() {
        // file_hash/content_hashがNone → meta_hashにフォールバック
        let a = FileFingerprint {
            file_hash: None,
            content_hash: None,
            meta_hash: Some("m1".to_string()),
            size: 100,
            modified_at: None,
        };
        let b = FileFingerprint {
            file_hash: None,
            content_hash: None,
            meta_hash: Some("m1".to_string()),
            size: 200,
            modified_at: None,
        };
        assert!(a.matches(&b));
    }

    #[test]
    fn matches_meta_hash_fallback_different() {
        let a = FileFingerprint {
            file_hash: None,
            content_hash: None,
            meta_hash: Some("m1".to_string()),
            size: 100,
            modified_at: None,
        };
        let b = FileFingerprint {
            file_hash: None,
            content_hash: None,
            meta_hash: Some("m2".to_string()),
            size: 100,
            modified_at: None,
        };
        assert!(!a.matches(&b));
    }

    #[test]
    fn content_same_meta_different_means_meta_only_change() {
        // ピクセル同一だがメタデータが異なるケース
        // file_hash不一致（バイト列は違う）、content_hash一致（ピクセル同一）
        let a = hash_fp_with_meta("h1", Some("c1"), Some("m1"), 1024);
        let b = hash_fp_with_meta("h2", Some("c1"), Some("m2"), 1024);
        // file_hash不一致 → matches = false（バイトレベルでは別物）
        assert!(!a.matches(&b));
        // しかしcontent_hashだけで比較すれば同一と判定できる
        // → rename検出やcanonical_hash用途ではcontent_hashを使う
    }

    #[test]
    fn precision_meta_level() {
        let fp = FileFingerprint {
            file_hash: None,
            content_hash: None,
            meta_hash: Some("m1".to_string()),
            size: 100,
            modified_at: None,
        };
        assert_eq!(fp.precision(), FingerprintPrecision::MetaLevel);
    }

    #[test]
    fn precision_semantic_trumps_meta() {
        // content_hashとmeta_hash両方ある場合 → Semantic
        let fp = FileFingerprint {
            file_hash: None,
            content_hash: Some("c1".to_string()),
            meta_hash: Some("m1".to_string()),
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
        // 画像ファイル = Entity。Content（ピクセルデータ）がIdentity。
        // メタデータ（tEXt/EXIF）にTSを埋め込むと、同一コンテンツでも
        // file_hash/size/meta_hashが全て変わるが、content_hashは不変。
        // → content_hash一致で同一Entityと判定。
        let before = FileFingerprint {
            file_hash: None, // file_hashは双方にないケース（Cloud等）
            content_hash: Some("pixel_hash_abc".to_string()),
            meta_hash: Some("meta_v1".to_string()),
            size: 10240, // メタ書き換え前のサイズ
            modified_at: None,
        };
        let after = FileFingerprint {
            file_hash: None,
            content_hash: Some("pixel_hash_abc".to_string()), // ピクセル同一
            meta_hash: Some("meta_v2".to_string()),           // メタ変化
            size: 10300,                                      // メタ追加でサイズ変化
            modified_at: None,
        };
        // content_hash一致 → 同一Entity（sizeの違いは無関係）
        assert!(before.matches(&after));
    }

    #[test]
    fn entity_model_content_change_is_detected() {
        // コンテンツ（ピクセルデータ）が変わった → 別Entity
        let v1 = FileFingerprint {
            file_hash: None,
            content_hash: Some("pixel_v1".to_string()),
            meta_hash: Some("meta_v1".to_string()),
            size: 10240,
            modified_at: None,
        };
        let v2 = FileFingerprint {
            file_hash: None,
            content_hash: Some("pixel_v2".to_string()), // ピクセル変化
            meta_hash: Some("meta_v1".to_string()),     // メタは同一
            size: 10240,                                // sizeも同一
            modified_at: None,
        };
        // content_hash不一致 → 別Entity
        assert!(!v1.matches(&v2));
    }

    #[test]
    fn entity_model_reexport_with_ts_in_meta() {
        // 同一画像を再書き出し（メタにTS埋め込み）→ 同一Entity
        // file_hashは変わる（バイト列が違う）がcontent_hashは同じ
        let original = hash_fp_with_meta("file_h1", Some("pixel_abc"), Some("meta_ts1"), 10240);
        let reexport = hash_fp_with_meta("file_h2", Some("pixel_abc"), Some("meta_ts2"), 10300);
        // file_hash不一致で即false — バイトレベルでは別物
        assert!(!original.matches(&reexport));
        // しかしcontent_hashだけ取り出せば同一Entity
        // → canonical_hash（content_hash）によるマッチングで同一と判定可能
        assert_eq!(
            original.content_hash.as_deref(),
            reexport.content_hash.as_deref()
        );
    }
}
