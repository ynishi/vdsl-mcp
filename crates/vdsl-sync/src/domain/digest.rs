//! Typed digest — ハッシュ値の型安全な表現。
//!
//! # 設計原則
//!
//! - **ByteDigest**: location固有のバイト列ダイジェスト（DJB2/SHA-256）。
//!   `PartialEq` 未実装 → `==` がコンパイルエラー。
//!   同一アルゴリズム同士のみ [`matches_same_algo()`](ByteDigest::matches_same_algo) で比較可能。
//!
//! - **ContentDigest**: ピクセルデータ等のセマンティックダイジェスト。
//!   location非依存。`PartialEq` 実装済み → cross-location比較可能。
//!
//! - **MetaDigest**: 埋め込みメタデータのダイジェスト。
//!   `PartialEq` 実装済み。
//!
//! - **CrossLocationIdentity**: cross-location比較専用の射影型。
//!   `ByteDigest` を構造的に持たない → 異アルゴリズム比較が不可能。
//!
//! # 型安全性の保証
//!
//! | 誤用パターン | 結果 |
//! |---|---|
//! | DJB2 == SHA-256 | コンパイルエラー（ByteDigestにPartialEqなし） |
//! | ByteDigest → canonical_digest混入 | コンパイルエラー（型不一致） |
//! | cross-locationでmatches_within_location() | メソッド名変更で旧呼び出し元がコンパイルエラー |

use serde::{Deserialize, Deserializer, Serialize, Serializer};

use super::error::DomainError;

// =============================================================================
// ByteDigest — location固有バイト列ダイジェスト
// =============================================================================

/// location固有のバイト列ダイジェスト。
///
/// **`PartialEq` 未実装** — `==` はコンパイルエラー。
/// 同一アルゴリズム同士のみ [`matches_same_algo()`](Self::matches_same_algo) で比較可能。
///
/// # Variants
///
/// - `Djb2`: DJB2ハッシュ（16文字hex）。Local/SSHスキャンで使用。
/// - `Sha256`: SHA-256ハッシュ（64文字hex）。Pod/Remoteスキャンで使用。
#[derive(Debug, Clone)]
pub enum ByteDigest {
    /// DJB2ハッシュ（16文字hex）。Local/SSHスキャン。
    Djb2(String),
    /// SHA-256ハッシュ（64文字hex）。Pod/Remoteスキャン。
    Sha256(String),
}

impl ByteDigest {
    /// 同一アルゴリズム同士の比較。
    ///
    /// # Errors
    ///
    /// 異なるアルゴリズム同士の比較は [`DomainError::DigestAlgorithmMismatch`]。
    pub fn matches_same_algo(&self, other: &ByteDigest) -> Result<bool, DomainError> {
        match (self, other) {
            (Self::Djb2(a), Self::Djb2(b)) => Ok(a == b),
            (Self::Sha256(a), Self::Sha256(b)) => Ok(a == b),
            _ => Err(DomainError::DigestAlgorithmMismatch {
                left: self.algo_name().to_string(),
                right: other.algo_name().to_string(),
            }),
        }
    }

    /// アルゴリズム名。
    pub fn algo_name(&self) -> &'static str {
        match self {
            Self::Djb2(_) => "djb2",
            Self::Sha256(_) => "sha256",
        }
    }

    /// 内部のハッシュ文字列への参照。
    pub fn as_str(&self) -> &str {
        match self {
            Self::Djb2(s) | Self::Sha256(s) => s,
        }
    }

    /// `"algo:value"` 形式の文字列にエンコード。DB保存用。
    pub fn to_prefixed_string(&self) -> String {
        match self {
            Self::Djb2(s) => format!("djb2:{s}"),
            Self::Sha256(s) => format!("sha256:{s}"),
        }
    }

    /// `"algo:value"` 形式またはレガシー（prefix無し）文字列からパース。
    ///
    /// prefix無しの場合、長さで推定:
    /// - 16文字以下 → DJB2
    /// - 17文字以上 → SHA-256
    pub fn parse(s: &str) -> Self {
        if let Some(rest) = s.strip_prefix("djb2:") {
            Self::Djb2(rest.to_string())
        } else if let Some(rest) = s.strip_prefix("sha256:") {
            Self::Sha256(rest.to_string())
        } else if s.len() <= 16 {
            Self::Djb2(s.to_string())
        } else {
            Self::Sha256(s.to_string())
        }
    }
}

impl std::fmt::Display for ByteDigest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Djb2(s) => write!(f, "djb2:{s}"),
            Self::Sha256(s) => write!(f, "sha256:{s}"),
        }
    }
}

/// Serde: `"algo:value"` 形式の文字列として直列化。
impl Serialize for ByteDigest {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&self.to_prefixed_string())
    }
}

/// Serde: `"algo:value"` 形式またはレガシー文字列からデシリアライズ。
impl<'de> Deserialize<'de> for ByteDigest {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let s = String::deserialize(deserializer)?;
        Ok(Self::parse(&s))
    }
}

// =============================================================================
// ContentDigest — ピクセルデータ等のセマンティックダイジェスト
// =============================================================================

/// ピクセルデータ等のセマンティックダイジェスト。
///
/// location非依存。`PartialEq` 実装済み → cross-location比較可能。
/// TopologyFile.canonical_digestの唯一の構築元。
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ContentDigest(pub String);

impl ContentDigest {
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for ContentDigest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

// =============================================================================
// MetaDigest — 埋め込みメタデータのダイジェスト
// =============================================================================

/// 埋め込みメタデータのダイジェスト（PNG tEXt, EXIF等）。
///
/// `PartialEq` 実装済み。
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct MetaDigest(pub String);

impl MetaDigest {
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for MetaDigest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

// =============================================================================
// CrossLocationIdentity — cross-location比較専用の射影型
// =============================================================================

/// cross-location比較で使用可能な情報のみ保持する射影型。
///
/// `ByteDigest` を構造的に持たない → 異アルゴリズム比較が不可能。
///
/// # 使用可能な情報
///
/// - `content_digest`: ピクセルデータのセマンティックハッシュ（location非依存）
/// - `size`: ファイルサイズ（location非依存）
///
/// # 比較ロジック
///
/// 1. 双方に `content_digest` → ContentDigest比較（最優先）
/// 2. フォールバック: `size` 比較（false positiveリスクあり）
#[derive(Debug, Clone)]
pub struct CrossLocationIdentity {
    pub content_digest: Option<ContentDigest>,
    pub size: u64,
}

impl CrossLocationIdentity {
    /// FileFingerprint から cross-location 比較可能な情報のみ抽出。
    ///
    /// ByteDigest は意図的に除外 — cross-location比較で使用不可。
    pub fn from_fingerprint(fp: &super::fingerprint::FileFingerprint) -> Self {
        Self {
            content_digest: fp.content_digest.clone(),
            size: fp.size,
        }
    }

    /// cross-location同一性判定。
    ///
    /// ContentDigest優先。双方Noneの場合はsize比較にフォールバック。
    pub fn matches(&self, other: &CrossLocationIdentity) -> bool {
        if let (Some(a), Some(b)) = (&self.content_digest, &other.content_digest) {
            return a == b;
        }
        // ContentDigestが片方または双方にない場合 → size比較
        self.size == other.size
    }
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    // =========================================================================
    // ByteDigest — matches_same_algo
    // =========================================================================

    #[test]
    fn djb2_same_value_matches() {
        let a = ByteDigest::Djb2("abc123".into());
        let b = ByteDigest::Djb2("abc123".into());
        assert_eq!(a.matches_same_algo(&b).unwrap(), true);
    }

    #[test]
    fn djb2_different_value_no_match() {
        let a = ByteDigest::Djb2("abc123".into());
        let b = ByteDigest::Djb2("def456".into());
        assert_eq!(a.matches_same_algo(&b).unwrap(), false);
    }

    #[test]
    fn sha256_same_value_matches() {
        let a = ByteDigest::Sha256("deadbeef".repeat(8));
        let b = ByteDigest::Sha256("deadbeef".repeat(8));
        assert_eq!(a.matches_same_algo(&b).unwrap(), true);
    }

    #[test]
    fn cross_algorithm_is_error() {
        let djb2 = ByteDigest::Djb2("abc123".into());
        let sha = ByteDigest::Sha256("deadbeef".repeat(8));
        let result = djb2.matches_same_algo(&sha);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.to_string().contains("djb2"));
        assert!(err.to_string().contains("sha256"));
    }

    // =========================================================================
    // ByteDigest — parse / to_prefixed_string roundtrip
    // =========================================================================

    #[test]
    fn parse_prefixed_djb2() {
        let d = ByteDigest::parse("djb2:abc123");
        assert_eq!(d.algo_name(), "djb2");
        assert_eq!(d.as_str(), "abc123");
    }

    #[test]
    fn parse_prefixed_sha256() {
        let d = ByteDigest::parse("sha256:deadbeef");
        assert_eq!(d.algo_name(), "sha256");
        assert_eq!(d.as_str(), "deadbeef");
    }

    #[test]
    fn parse_legacy_short_is_djb2() {
        let d = ByteDigest::parse("abc123");
        assert_eq!(d.algo_name(), "djb2");
        assert_eq!(d.as_str(), "abc123");
    }

    #[test]
    fn parse_legacy_long_is_sha256() {
        let long = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";
        let d = ByteDigest::parse(long);
        assert_eq!(d.algo_name(), "sha256");
        assert_eq!(d.as_str(), long);
    }

    #[test]
    fn roundtrip_prefixed() {
        let d = ByteDigest::Djb2("abc123".into());
        let s = d.to_prefixed_string();
        let d2 = ByteDigest::parse(&s);
        assert_eq!(d2.algo_name(), "djb2");
        assert_eq!(d2.as_str(), "abc123");
    }

    // =========================================================================
    // ByteDigest — serde
    // =========================================================================

    #[test]
    fn serde_roundtrip() {
        let d = ByteDigest::Sha256("deadbeef".into());
        let json = serde_json::to_string(&d).unwrap();
        assert_eq!(json, r#""sha256:deadbeef""#);
        let d2: ByteDigest = serde_json::from_str(&json).unwrap();
        assert_eq!(d2.algo_name(), "sha256");
        assert_eq!(d2.as_str(), "deadbeef");
    }

    // =========================================================================
    // ContentDigest — PartialEq
    // =========================================================================

    #[test]
    fn content_digest_eq() {
        let a = ContentDigest("pixel_abc".into());
        let b = ContentDigest("pixel_abc".into());
        assert_eq!(a, b);
    }

    #[test]
    fn content_digest_ne() {
        let a = ContentDigest("pixel_abc".into());
        let b = ContentDigest("pixel_xyz".into());
        assert_ne!(a, b);
    }

    // =========================================================================
    // MetaDigest — PartialEq
    // =========================================================================

    #[test]
    fn meta_digest_eq() {
        let a = MetaDigest("meta_v1".into());
        let b = MetaDigest("meta_v1".into());
        assert_eq!(a, b);
    }

    // =========================================================================
    // CrossLocationIdentity
    // =========================================================================

    #[test]
    fn cross_location_content_digest_match() {
        let a = CrossLocationIdentity {
            content_digest: Some(ContentDigest("pixel_abc".into())),
            size: 1024,
        };
        let b = CrossLocationIdentity {
            content_digest: Some(ContentDigest("pixel_abc".into())),
            size: 2048, // size differs but content_digest matches
        };
        assert!(a.matches(&b));
    }

    #[test]
    fn cross_location_content_digest_mismatch() {
        let a = CrossLocationIdentity {
            content_digest: Some(ContentDigest("pixel_abc".into())),
            size: 1024,
        };
        let b = CrossLocationIdentity {
            content_digest: Some(ContentDigest("pixel_xyz".into())),
            size: 1024,
        };
        assert!(!a.matches(&b));
    }

    #[test]
    fn cross_location_no_content_digest_falls_back_to_size() {
        let a = CrossLocationIdentity {
            content_digest: None,
            size: 1024,
        };
        let b = CrossLocationIdentity {
            content_digest: None,
            size: 1024,
        };
        assert!(a.matches(&b));
    }

    #[test]
    fn cross_location_no_content_digest_size_differs() {
        let a = CrossLocationIdentity {
            content_digest: None,
            size: 1024,
        };
        let b = CrossLocationIdentity {
            content_digest: None,
            size: 2048,
        };
        assert!(!a.matches(&b));
    }

    #[test]
    fn cross_location_one_has_content_digest_falls_back_to_size() {
        // 片方だけcontent_digestあり → size比較にフォールバック
        let a = CrossLocationIdentity {
            content_digest: Some(ContentDigest("pixel_abc".into())),
            size: 1024,
        };
        let b = CrossLocationIdentity {
            content_digest: None,
            size: 1024,
        };
        assert!(a.matches(&b));
    }
}
