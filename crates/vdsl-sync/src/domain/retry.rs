//! リトライポリシー — 配送失敗時のドメインルール。
//!
//! ファイル同期ドメインにおいて、転送失敗は「想定内」のイベント。
//! リトライポリシーは以下を定義する:
//!
//! - **最大試行回数** (`max_attempts`) — 何回まで再試行するか
//! - **エラー分類** ([`TransferErrorKind`]) — リトライ対象か否か
//!
//! # エラー分類
//!
//! - [`Transient`](TransferErrorKind::Transient) — 一時的エラー（ネットワーク障害等）。リトライ対象
//! - [`Permanent`](TransferErrorKind::Permanent) — 永続的エラー（ファイル消失等）。リトライ不要

use serde::{Deserialize, Serialize};
use std::fmt;

use super::error::DomainError;

/// 転送エラーの種別。
///
/// ドメインルール: `Transient` はリトライ対象、`Permanent` はリトライ不要。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TransferErrorKind {
    /// 一時的エラー: ネットワークタイムアウト、バックエンド一時障害、レート制限。
    /// リトライで回復が期待できる。
    Transient,
    /// 永続的エラー: ソースファイル消失、認証構成ミス、パス不正。
    /// リトライしても回復しない。
    Permanent,
}

impl TransferErrorKind {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Transient => "transient",
            Self::Permanent => "permanent",
        }
    }
}

impl fmt::Display for TransferErrorKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl std::str::FromStr for TransferErrorKind {
    type Err = DomainError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "transient" => Ok(Self::Transient),
            "permanent" => Ok(Self::Permanent),
            other => Err(DomainError::Validation {
                field: "transfer_error_kind".into(),
                reason: format!("unknown value: {other}"),
            }),
        }
    }
}

/// リトライポリシー (Value Object)。
///
/// 配送失敗時の再試行ルールを定義する。
/// `max_attempts` は初回を含む — 3なら初回+リトライ2回。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RetryPolicy {
    max_attempts: u32,
}

impl RetryPolicy {
    /// デフォルトの最大試行回数。
    pub const DEFAULT_MAX_ATTEMPTS: u32 = 3;

    /// 新しいリトライポリシーを作成。
    ///
    /// `max_attempts` は最低1にクランプされる（0回試行は無意味）。
    pub fn new(max_attempts: u32) -> Self {
        Self {
            max_attempts: max_attempts.max(1),
        }
    }

    pub fn max_attempts(&self) -> u32 {
        self.max_attempts
    }
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self::new(Self::DEFAULT_MAX_ATTEMPTS)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_policy() {
        let p = RetryPolicy::default();
        assert_eq!(p.max_attempts(), 3);
    }

    #[test]
    fn custom_policy() {
        let p = RetryPolicy::new(5);
        assert_eq!(p.max_attempts(), 5);
    }

    #[test]
    fn zero_clamped_to_one() {
        let p = RetryPolicy::new(0);
        assert_eq!(p.max_attempts(), 1);
    }

    #[test]
    fn error_kind_roundtrip() {
        for kind in [TransferErrorKind::Transient, TransferErrorKind::Permanent] {
            let s = kind.as_str();
            let parsed: TransferErrorKind = s.parse().unwrap();
            assert_eq!(parsed, kind);
        }
    }

    #[test]
    fn error_kind_invalid() {
        let result: Result<TransferErrorKind, _> = "unknown".parse();
        assert!(result.is_err());
    }
}
