//! Transfer — 配送オブジェクト。
//!
//! 「1つのファイルを、あるlocationからあるlocationへ送る」という1回の配送行為。
//! 配送状況は自分自身が持つ。TrackedFileに問い合わせない。
//!
//! # 状態遷移
//!
//! ```text
//! Queued → InFlight → Completed
//!                   → Failed → (retry()で新Transferを生成)
//! ```
//!
//! Failedからの復帰は既存Transferの状態変更ではなく、
//! `retry()` で新しいTransfer (attempt +1) を生成する。
//! 失敗した記録は不変のまま履歴に残る。

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::fmt;

use super::error::SyncError;
use super::location::LocationId;
use super::retry::{RetryPolicy, TransferErrorKind};

/// 配送の状態。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TransferState {
    /// 転送待ち。配送指示が作成されたがまだ実行されていない。
    Queued,
    /// 転送中。バックエンドがファイルを送信している。
    InFlight,
    /// 転送完了。destにファイルが到達した。
    Completed,
    /// 転送失敗。エラー理由が`error`フィールドに記録される。
    Failed,
}

impl TransferState {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Queued => "queued",
            Self::InFlight => "in_flight",
            Self::Completed => "completed",
            Self::Failed => "failed",
        }
    }

    /// まだ配送が必要な状態か。
    pub fn is_actionable(&self) -> bool {
        matches!(self, Self::Queued)
    }
}

impl fmt::Display for TransferState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl std::str::FromStr for TransferState {
    type Err = SyncError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "queued" => Ok(Self::Queued),
            "in_flight" => Ok(Self::InFlight),
            "completed" => Ok(Self::Completed),
            "failed" => Ok(Self::Failed),
            other => Err(SyncError::InvalidTransferState(other.to_string())),
        }
    }
}

/// 1回の配送行為。
///
/// TrackedFileとは参照関係のみ (`file_id`)。
/// 配送状態・エラー・タイムスタンプは全てTransfer自身が保持する。
///
/// # 不変条件
///
/// - `src != dest` (自己転送は無意味)
/// - 状態遷移は `Queued → InFlight → Completed|Failed` の一方向のみ
/// - Failedからの復帰は `retry()` で新Transferを生成する
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Transfer {
    id: String,
    file_id: String,
    src: LocationId,
    dest: LocationId,
    state: TransferState,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    error: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    error_kind: Option<TransferErrorKind>,
    attempt: u32,
    created_at: DateTime<Utc>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    started_at: Option<DateTime<Utc>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    finished_at: Option<DateTime<Utc>>,
}

impl Transfer {
    // =========================================================================
    // Factory
    // =========================================================================

    /// 新しい配送指示を作成。state = Queued, attempt = 1。
    ///
    /// # Errors
    ///
    /// - `file_id` が空文字列の場合
    /// - `src == dest` の場合（自己転送は無意味）
    pub fn new(file_id: String, src: LocationId, dest: LocationId) -> Result<Self, SyncError> {
        if file_id.is_empty() {
            return Err(SyncError::Validation {
                field: "file_id".into(),
                reason: "must not be empty".into(),
            });
        }
        if src == dest {
            return Err(SyncError::Validation {
                field: "src/dest".into(),
                reason: format!("self-transfer is not allowed: {src}"),
            });
        }
        Ok(Self {
            id: uuid::Uuid::new_v4().to_string(),
            file_id,
            src,
            dest,
            state: TransferState::Queued,
            error: None,
            error_kind: None,
            attempt: 1,
            created_at: Utc::now(),
            started_at: None,
            finished_at: None,
        })
    }

    /// DB復元用。永続化済みデータからの再構成。
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn reconstitute(
        id: String,
        file_id: String,
        src: LocationId,
        dest: LocationId,
        state: TransferState,
        error: Option<String>,
        error_kind: Option<TransferErrorKind>,
        attempt: u32,
        created_at: DateTime<Utc>,
        started_at: Option<DateTime<Utc>>,
        finished_at: Option<DateTime<Utc>>,
    ) -> Self {
        Self {
            id,
            file_id,
            src,
            dest,
            state,
            error,
            error_kind,
            attempt,
            created_at,
            started_at,
            finished_at,
        }
    }

    // =========================================================================
    // State transitions
    // =========================================================================

    /// 転送開始。Queued → InFlight。
    ///
    /// Queued以外の状態から呼ばれた場合はErrを返す。
    pub fn start(&mut self) -> Result<(), SyncError> {
        if self.state != TransferState::Queued {
            return Err(SyncError::InvalidStateTransition {
                from: self.state.as_str().to_string(),
                to: "in_flight".to_string(),
            });
        }
        self.state = TransferState::InFlight;
        self.started_at = Some(Utc::now());
        Ok(())
    }

    /// 転送完了。InFlight → Completed。
    ///
    /// InFlight以外の状態から呼ばれた場合はErrを返す。
    pub fn complete(&mut self) -> Result<(), SyncError> {
        if self.state != TransferState::InFlight {
            return Err(SyncError::InvalidStateTransition {
                from: self.state.as_str().to_string(),
                to: "completed".to_string(),
            });
        }
        self.state = TransferState::Completed;
        self.finished_at = Some(Utc::now());
        Ok(())
    }

    /// 転送失敗。InFlight → Failed。
    ///
    /// `kind` でエラーの種別を分類する:
    /// - [`Transient`](TransferErrorKind::Transient) — リトライ対象
    /// - [`Permanent`](TransferErrorKind::Permanent) — リトライ不要
    ///
    /// InFlight以外の状態から呼ばれた場合はErrを返す。
    pub fn fail(&mut self, error: String, kind: TransferErrorKind) -> Result<(), SyncError> {
        if self.state != TransferState::InFlight {
            return Err(SyncError::InvalidStateTransition {
                from: self.state.as_str().to_string(),
                to: "failed".to_string(),
            });
        }
        self.state = TransferState::Failed;
        self.error = Some(error);
        self.error_kind = Some(kind);
        self.finished_at = Some(Utc::now());
        Ok(())
    }

    /// リトライ: この失敗Transferを元に新しいTransferを生成。
    ///
    /// attempt +1。元のTransferは不変のまま履歴に残る。
    /// Failed以外から呼ばれた場合はErrを返す。
    pub fn retry(&self) -> Result<Self, SyncError> {
        if self.state != TransferState::Failed {
            return Err(SyncError::InvalidStateTransition {
                from: self.state.as_str().to_string(),
                to: "queued (retry)".to_string(),
            });
        }
        Ok(Self {
            id: uuid::Uuid::new_v4().to_string(),
            file_id: self.file_id.clone(),
            src: self.src.clone(),
            dest: self.dest.clone(),
            state: TransferState::Queued,
            error: None,
            error_kind: None,
            attempt: self.attempt.saturating_add(1),
            created_at: Utc::now(),
            started_at: None,
            finished_at: None,
        })
    }

    // =========================================================================
    // Retry domain logic
    // =========================================================================

    /// このTransferはリトライ対象か（ドメインルール）。
    ///
    /// 以下の全条件を満たす場合にtrue:
    /// 1. 状態がFailed
    /// 2. エラー種別がTransient（一時的エラー）
    /// 3. 試行回数がmax_attempts未満
    pub fn is_retryable(&self, policy: &RetryPolicy) -> bool {
        self.state == TransferState::Failed
            && self.error_kind == Some(TransferErrorKind::Transient)
            && self.attempt < policy.max_attempts()
    }

    /// リトライ上限に到達したか。
    ///
    /// Failedかつ以下のいずれか:
    /// - Permanentエラー（リトライ不要）
    /// - 試行回数がmax_attempts以上（Transientでも上限到達）
    pub fn is_exhausted(&self, policy: &RetryPolicy) -> bool {
        self.state == TransferState::Failed
            && (self.error_kind == Some(TransferErrorKind::Permanent)
                || self.attempt >= policy.max_attempts())
    }

    // =========================================================================
    // Queries
    // =========================================================================

    pub fn id(&self) -> &str {
        &self.id
    }

    pub fn file_id(&self) -> &str {
        &self.file_id
    }

    pub fn src(&self) -> &LocationId {
        &self.src
    }

    pub fn dest(&self) -> &LocationId {
        &self.dest
    }

    pub fn state(&self) -> TransferState {
        self.state
    }

    pub fn error(&self) -> Option<&str> {
        self.error.as_deref()
    }

    pub fn error_kind(&self) -> Option<TransferErrorKind> {
        self.error_kind
    }

    pub fn attempt(&self) -> u32 {
        self.attempt
    }

    pub fn created_at(&self) -> DateTime<Utc> {
        self.created_at
    }

    pub fn started_at(&self) -> Option<DateTime<Utc>> {
        self.started_at
    }

    pub fn finished_at(&self) -> Option<DateTime<Utc>> {
        self.finished_at
    }

    /// Serialize to [`serde_json::Value`] for cross-boundary transport.
    pub fn to_value(&self) -> Result<serde_json::Value, SyncError> {
        serde_json::to_value(self).map_err(|e| SyncError::Serialization(format!("Transfer: {e}")))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn loc(s: &str) -> LocationId {
        LocationId::new(s).unwrap()
    }

    fn sample_transfer() -> Transfer {
        Transfer::new("file-1".into(), loc("local"), loc("cloud")).expect("valid test data")
    }

    fn failed_transient(attempt: u32) -> Transfer {
        Transfer::reconstitute(
            uuid::Uuid::new_v4().to_string(),
            "file-1".into(),
            loc("local"),
            loc("cloud"),
            TransferState::Failed,
            Some("timeout".into()),
            Some(TransferErrorKind::Transient),
            attempt,
            Utc::now(),
            Some(Utc::now()),
            Some(Utc::now()),
        )
    }

    fn failed_permanent() -> Transfer {
        Transfer::reconstitute(
            uuid::Uuid::new_v4().to_string(),
            "file-1".into(),
            loc("local"),
            loc("cloud"),
            TransferState::Failed,
            Some("file not found".into()),
            Some(TransferErrorKind::Permanent),
            1,
            Utc::now(),
            Some(Utc::now()),
            Some(Utc::now()),
        )
    }

    // --- Factory ---

    #[test]
    fn new_creates_queued_transfer() {
        let t = sample_transfer();
        assert_eq!(t.state(), TransferState::Queued);
        assert_eq!(t.attempt(), 1);
        assert_eq!(t.file_id(), "file-1");
        assert_eq!(t.src(), &loc("local"));
        assert_eq!(t.dest(), &loc("cloud"));
        assert!(t.error().is_none());
        assert!(t.error_kind().is_none());
        assert!(t.started_at().is_none());
        assert!(t.finished_at().is_none());
    }

    // --- Happy path: Queued → InFlight → Completed ---

    #[test]
    fn start_transitions_to_in_flight() {
        let mut t = sample_transfer();
        t.start().unwrap();
        assert_eq!(t.state(), TransferState::InFlight);
        assert!(t.started_at().is_some());
    }

    #[test]
    fn complete_transitions_to_completed() {
        let mut t = sample_transfer();
        t.start().unwrap();
        t.complete().unwrap();
        assert_eq!(t.state(), TransferState::Completed);
        assert!(t.finished_at().is_some());
    }

    // --- Failure path: Queued → InFlight → Failed ---

    #[test]
    fn fail_transient_transitions_to_failed() {
        let mut t = sample_transfer();
        t.start().unwrap();
        t.fail("B2 timeout".into(), TransferErrorKind::Transient)
            .unwrap();
        assert_eq!(t.state(), TransferState::Failed);
        assert_eq!(t.error(), Some("B2 timeout"));
        assert_eq!(t.error_kind(), Some(TransferErrorKind::Transient));
        assert!(t.finished_at().is_some());
    }

    #[test]
    fn fail_permanent_transitions_to_failed() {
        let mut t = sample_transfer();
        t.start().unwrap();
        t.fail("file not found".into(), TransferErrorKind::Permanent)
            .unwrap();
        assert_eq!(t.state(), TransferState::Failed);
        assert_eq!(t.error_kind(), Some(TransferErrorKind::Permanent));
    }

    // --- Retry domain logic ---

    #[test]
    fn transient_within_limit_is_retryable() {
        let policy = RetryPolicy::new(3);
        let t = failed_transient(1); // attempt 1 < max 3
        assert!(t.is_retryable(&policy));
        assert!(!t.is_exhausted(&policy));
    }

    #[test]
    fn transient_at_limit_is_exhausted() {
        let policy = RetryPolicy::new(3);
        let t = failed_transient(3); // attempt 3 >= max 3
        assert!(!t.is_retryable(&policy));
        assert!(t.is_exhausted(&policy));
    }

    #[test]
    fn permanent_is_never_retryable() {
        let policy = RetryPolicy::new(10);
        let t = failed_permanent(); // attempt 1, but Permanent
        assert!(!t.is_retryable(&policy));
        assert!(t.is_exhausted(&policy));
    }

    #[test]
    fn queued_is_not_retryable() {
        let policy = RetryPolicy::default();
        let t = sample_transfer();
        assert!(!t.is_retryable(&policy));
        assert!(!t.is_exhausted(&policy));
    }

    // --- Retry factory ---

    #[test]
    fn retry_creates_new_queued_transfer() {
        let mut t = sample_transfer();
        t.start().unwrap();
        t.fail("error".into(), TransferErrorKind::Transient)
            .unwrap();

        let t2 = t.retry().unwrap();
        assert_eq!(t2.state(), TransferState::Queued);
        assert_eq!(t2.attempt(), 2);
        assert_eq!(t2.file_id(), "file-1");
        assert_eq!(t2.src(), &loc("local"));
        assert_eq!(t2.dest(), &loc("cloud"));
        assert!(t2.error().is_none());
        assert!(t2.error_kind().is_none());
        assert_ne!(t2.id(), t.id(), "retry must generate new id");
    }

    #[test]
    fn retry_preserves_original() {
        let mut t = sample_transfer();
        t.start().unwrap();
        t.fail("error".into(), TransferErrorKind::Transient)
            .unwrap();
        let original_id = t.id().to_string();

        let _ = t.retry().unwrap();

        // Original is unchanged
        assert_eq!(t.id(), original_id);
        assert_eq!(t.state(), TransferState::Failed);
        assert_eq!(t.error(), Some("error"));
        assert_eq!(t.error_kind(), Some(TransferErrorKind::Transient));
    }

    // --- Invalid transitions ---

    #[test]
    fn start_from_non_queued_fails() {
        let mut t = sample_transfer();
        t.start().unwrap();
        assert!(t.start().is_err(), "InFlight → InFlight is invalid");
    }

    #[test]
    fn complete_from_queued_fails() {
        let mut t = sample_transfer();
        assert!(t.complete().is_err(), "Queued → Completed is invalid");
    }

    #[test]
    fn fail_from_queued_fails() {
        let mut t = sample_transfer();
        assert!(
            t.fail("err".into(), TransferErrorKind::Transient).is_err(),
            "Queued → Failed is invalid"
        );
    }

    #[test]
    fn retry_from_non_failed_fails() {
        let t = sample_transfer();
        assert!(t.retry().is_err(), "Queued → retry is invalid");
    }

    #[test]
    fn retry_from_completed_fails() {
        let mut t = sample_transfer();
        t.start().unwrap();
        t.complete().unwrap();
        assert!(t.retry().is_err(), "Completed → retry is invalid");
    }

    // --- TransferState ---

    #[test]
    fn state_roundtrip() {
        for state in [
            TransferState::Queued,
            TransferState::InFlight,
            TransferState::Completed,
            TransferState::Failed,
        ] {
            let s = state.as_str();
            let parsed: TransferState = s.parse().unwrap();
            assert_eq!(parsed, state);
        }
    }

    #[test]
    fn is_actionable() {
        assert!(TransferState::Queued.is_actionable());
        assert!(!TransferState::InFlight.is_actionable());
        assert!(!TransferState::Completed.is_actionable());
        assert!(!TransferState::Failed.is_actionable());
    }

    // --- Serde ---

    #[test]
    fn serde_roundtrip() {
        let mut t = sample_transfer();
        t.start().unwrap();
        t.complete().unwrap();
        let json = serde_json::to_value(&t).unwrap();
        let restored: Transfer = serde_json::from_value(json).unwrap();
        assert_eq!(restored.state(), TransferState::Completed);
        assert_eq!(restored.file_id(), "file-1");
    }

    #[test]
    fn serde_roundtrip_failed_with_error_kind() {
        let mut t = sample_transfer();
        t.start().unwrap();
        t.fail("net err".into(), TransferErrorKind::Transient)
            .unwrap();
        let json = serde_json::to_value(&t).unwrap();
        let restored: Transfer = serde_json::from_value(json).unwrap();
        assert_eq!(restored.state(), TransferState::Failed);
        assert_eq!(restored.error_kind(), Some(TransferErrorKind::Transient));
    }

    // --- Validation ---

    #[test]
    fn new_rejects_empty_file_id() {
        let result = Transfer::new("".into(), loc("local"), loc("cloud"));
        assert!(result.is_err());
    }

    #[test]
    fn new_rejects_self_transfer() {
        let result = Transfer::new("file-1".into(), loc("local"), loc("local"));
        assert!(result.is_err());
    }
}
