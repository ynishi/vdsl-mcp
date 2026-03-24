//! Domain error — ドメイン不変条件違反のみ。
//!
//! インフラ（DB, FS, ネットワーク）の詳細を一切含まない。
//! アプリケーション層の [`SyncError`](crate::application::error::SyncError) が
//! `#[from]` でこのエラーを包含する。

/// ドメイン不変条件違反。
#[derive(Debug, thiserror::Error)]
pub enum DomainError {
    #[error("invalid file type: {0}")]
    InvalidFileType(String),

    #[error("invalid location: {0}")]
    InvalidLocation(String),

    #[error("invalid transfer state: {0}")]
    InvalidTransferState(String),

    #[error("invalid state transition: {from} → {to}")]
    InvalidStateTransition { from: String, to: String },

    #[error("validation error: {field} — {reason}")]
    Validation { field: String, reason: String },
}
