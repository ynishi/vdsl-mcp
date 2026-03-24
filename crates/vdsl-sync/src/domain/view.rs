//! クエリ用ビュー型。
//!
//! Transfer/LocationFileから導出される読み取り専用のビュー。
//! ドメインエンティティではない。外部API (MCP, CLI, Lua bridge) への
//! レスポンス構築用。

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::fmt;

use super::location::LocationId;
use super::retry::RetryPolicy;
use super::transfer::{Transfer, TransferKind, TransferState};

/// 特定locationでのファイルの存在状態。
///
/// 最新のTransferから導出される。ドメインエンティティではない。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PresenceState {
    /// Completed Transferあり — ファイルが到達済み。
    Present,
    /// Queued Transferあり — 転送待ち。
    Pending,
    /// InFlight Transferあり — 転送中。
    Syncing,
    /// リトライ上限到達 or 永続的エラー — 手動介入が必要。
    Failed,
    /// ソースにファイルなしで失敗 — 再スキャンが必要。
    Absent,
}

impl PresenceState {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Present => "present",
            Self::Pending => "pending",
            Self::Syncing => "syncing",
            Self::Failed => "failed",
            Self::Absent => "absent",
        }
    }

    /// Priority for conflict resolution when a location appears as both
    /// src and dest for the same file. Higher value wins.
    ///
    /// Failed > Syncing > Pending > Present > Absent
    pub fn priority(&self) -> u8 {
        match self {
            Self::Absent => 0,
            Self::Present => 1,
            Self::Pending => 2,
            Self::Syncing => 3,
            Self::Failed => 4,
        }
    }

    /// Transfer + RetryPolicy からPresenceStateを導出。
    pub fn from_transfer(transfer: &Transfer, policy: &RetryPolicy) -> Self {
        match transfer.state() {
            TransferState::Blocked => Self::Pending,
            TransferState::Queued => Self::Pending,
            TransferState::InFlight => Self::Syncing,
            TransferState::Completed => Self::Present,
            TransferState::Failed => {
                if transfer.is_retryable(policy) {
                    Self::Pending
                } else {
                    Self::Failed
                }
            }
            TransferState::Cancelled => Self::Absent,
        }
    }
}

impl fmt::Display for PresenceState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// 特定locationでのファイルの存在状況ビュー。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PresenceView {
    pub location: LocationId,
    pub state: PresenceState,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub synced_at: Option<DateTime<Utc>>,
    pub attempt: u32,
}

/// 失敗したTransferの表示情報。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ErrorEntry {
    pub file_id: String,
    pub src: LocationId,
    pub dest: LocationId,
    pub error: String,
    pub attempts: u32,
}

impl ErrorEntry {
    pub(crate) fn from_transfer(t: &Transfer) -> Self {
        Self {
            file_id: t.file_id().to_string(),
            src: t.src().clone(),
            dest: t.dest().clone(),
            error: t.error().unwrap_or("unknown error").to_string(),
            attempts: t.attempt(),
        }
    }
}

/// 待機中Transferの表示情報。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PendingEntry {
    pub file_id: String,
    pub src: LocationId,
    pub dest: LocationId,
    pub kind: TransferKind,
}

impl PendingEntry {
    pub(crate) fn from_transfer(t: &Transfer) -> Self {
        Self {
            file_id: t.file_id().to_string(),
            src: t.src().clone(),
            dest: t.dest().clone(),
            kind: t.kind(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::retry::TransferErrorKind;

    fn loc(s: &str) -> LocationId {
        LocationId::new(s).unwrap()
    }

    fn make_transfer(
        state: TransferState,
        error_kind: Option<TransferErrorKind>,
        attempt: u32,
    ) -> Transfer {
        Transfer::reconstitute(
            "t-1".into(),
            "f-1".into(),
            loc("local"),
            loc("cloud"),
            TransferKind::Sync,
            state,
            if state == TransferState::Failed {
                Some("err".into())
            } else {
                None
            },
            error_kind,
            attempt,
            Utc::now(),
            if state != TransferState::Queued {
                Some(Utc::now())
            } else {
                None
            },
            if matches!(state, TransferState::Completed | TransferState::Failed) {
                Some(Utc::now())
            } else {
                None
            },
        )
    }

    #[test]
    fn presence_from_queued() {
        let t = make_transfer(TransferState::Queued, None, 1);
        assert_eq!(
            PresenceState::from_transfer(&t, &RetryPolicy::default()),
            PresenceState::Pending
        );
    }

    #[test]
    fn presence_from_completed() {
        let t = make_transfer(TransferState::Completed, None, 1);
        assert_eq!(
            PresenceState::from_transfer(&t, &RetryPolicy::default()),
            PresenceState::Present
        );
    }

    #[test]
    fn presence_from_in_flight() {
        let t = make_transfer(TransferState::InFlight, None, 1);
        assert_eq!(
            PresenceState::from_transfer(&t, &RetryPolicy::default()),
            PresenceState::Syncing
        );
    }

    #[test]
    fn presence_from_failed_transient_retryable() {
        let policy = RetryPolicy::new(3);
        let t = make_transfer(TransferState::Failed, Some(TransferErrorKind::Transient), 1);
        assert_eq!(
            PresenceState::from_transfer(&t, &policy),
            PresenceState::Pending
        );
    }

    #[test]
    fn presence_from_failed_transient_exhausted() {
        let policy = RetryPolicy::new(3);
        let t = make_transfer(TransferState::Failed, Some(TransferErrorKind::Transient), 3);
        assert_eq!(
            PresenceState::from_transfer(&t, &policy),
            PresenceState::Failed
        );
    }

    #[test]
    fn presence_from_failed_permanent() {
        let policy = RetryPolicy::new(10);
        let t = make_transfer(TransferState::Failed, Some(TransferErrorKind::Permanent), 1);
        assert_eq!(
            PresenceState::from_transfer(&t, &policy),
            PresenceState::Failed
        );
    }
}
