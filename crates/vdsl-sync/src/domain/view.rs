//! クエリ用ビュー型。
//!
//! TrackedFileとTransferから導出される読み取り専用のビュー。
//! ドメインエンティティではない。外部API (MCP, CLI, Lua bridge) への
//! レスポンス構築用。

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::fmt;

use super::location::LocationId;
use super::retry::RetryPolicy;
use super::tracked_file::TrackedFile;
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
    ///
    /// - Queued → Pending
    /// - InFlight → Syncing
    /// - Completed → Present
    /// - Failed + retryable → Pending（リトライ待ち）
    /// - Failed + exhausted → Failed（手動介入が必要）
    /// - Cancelled → Absent（転送は中断済み。ファイルは到達していない）
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
///
/// 最新のTransferから構築される。
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

/// TrackedFile + 各locationのPresence情報を結合したビュー。
///
/// `Store::get()` 等のクエリAPIが返す型。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileView {
    #[serde(flatten)]
    pub file: TrackedFile,
    pub presences: Vec<PresenceView>,
}

impl FileView {
    /// 特定locationの存在状態を取得。
    pub fn presence(&self, loc: &LocationId) -> Option<&PresenceView> {
        self.presences.iter().find(|p| &p.location == loc)
    }

    /// 特定locationのPresenceStateを取得。
    pub fn presence_state(&self, loc: &LocationId) -> Option<PresenceState> {
        self.presence(loc).map(|p| p.state)
    }

    /// Serialize to [`serde_json::Value`] for cross-boundary transport.
    pub fn to_value(&self) -> Result<serde_json::Value, serde_json::Error> {
        serde_json::to_value(self)
    }
}

// =============================================================================
// Status detail entries (Store::status() 用)
// =============================================================================

/// 失敗したTransferの表示情報。
///
/// Transfer本体を公開せず、Store利用者が必要な情報のみを提供する。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ErrorEntry {
    pub file_id: String,
    pub src: LocationId,
    pub dest: LocationId,
    pub error: String,
    pub attempts: u32,
}

impl ErrorEntry {
    /// Transferから表示用ErrorEntryを構築する。
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
///
/// Transfer本体を公開せず、Store利用者が必要な情報のみを提供する。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PendingEntry {
    pub file_id: String,
    pub src: LocationId,
    pub dest: LocationId,
    pub kind: TransferKind,
}

impl PendingEntry {
    /// Transferから表示用PendingEntryを構築する。
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
    use crate::domain::file_type::FileType;
    use crate::domain::retry::TransferErrorKind;
    use crate::domain::transfer::TransferKind;

    fn loc(s: &str) -> LocationId {
        LocationId::new(s).unwrap()
    }

    fn sample_view() -> FileView {
        FileView {
            file: TrackedFile::from_scan(
                "output/test.png".into(),
                FileType::Image,
                "hash123".into(),
                None,
                1024,
                None,
            )
            .expect("valid test data"),
            presences: vec![
                PresenceView {
                    location: loc("local"),
                    state: PresenceState::Present,
                    error: None,
                    synced_at: None,
                    attempt: 0,
                },
                PresenceView {
                    location: loc("cloud"),
                    state: PresenceState::Pending,
                    error: None,
                    synced_at: None,
                    attempt: 1,
                },
                PresenceView {
                    location: loc("pod"),
                    state: PresenceState::Present,
                    error: None,
                    synced_at: Some(Utc::now()),
                    attempt: 2,
                },
            ],
        }
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

    #[test]
    fn file_view_presence_lookup() {
        let v = sample_view();
        assert_eq!(
            v.presence_state(&loc("local")),
            Some(PresenceState::Present)
        );
        assert_eq!(
            v.presence_state(&loc("cloud")),
            Some(PresenceState::Pending)
        );
        assert_eq!(v.presence_state(&loc("nas")), None);
    }

    #[test]
    fn file_view_presence_detail() {
        let v = sample_view();
        let pod = v.presence(&loc("pod")).unwrap();
        assert_eq!(pod.attempt, 2);
        assert!(pod.synced_at.is_some());
    }

    #[test]
    fn serde_roundtrip() {
        let v = sample_view();
        let json = serde_json::to_value(&v).unwrap();

        // file fields are flattened
        assert!(json.get("relative_path").is_some());
        assert!(json.get("file_hash").is_some());
        // presences is an array
        assert!(json.get("presences").unwrap().is_array());

        let restored: FileView = serde_json::from_value(json).unwrap();
        assert_eq!(restored.file.relative_path(), "output/test.png");
        assert_eq!(restored.presences.len(), 3);
    }
}
