//! SyncError — アプリケーション層の統合エラー。
//!
//! ドメインエラーとインフラエラーを `#[from]` で結合し、
//! アプリケーション固有のバリアントを追加する。
//!
//! infra trait (`FileStore`, `TransferStore`, `StorageBackend`) の返り値型。
//! 外部crate (vdsl-mcp) からもこのエラーが参照される。

use crate::domain::error::DomainError;
use crate::infra::error::InfraError;

/// Sync engine の統合エラー。
#[derive(Debug, thiserror::Error)]
pub enum SyncError {
    /// ドメイン不変条件違反。
    #[error(transparent)]
    Domain(#[from] DomainError),

    /// インフラストラクチャ障害。
    #[error(transparent)]
    Infra(#[from] InfraError),

    /// パスがsync root外。
    #[error("path is outside sync root: {path}")]
    OutsideSyncRoot { path: String },

    /// 重複ファイル検出。
    #[error("duplicate file: {path} is a duplicate of {duplicate_of}")]
    Duplicate { path: String, duplicate_of: String },

    /// ファイルがsync storeに未登録。
    #[error("file not registered in sync store: {0}")]
    NotRegistered(String),

    /// 初期化失敗（拠点到達不能、外部ツール確保失敗等）。
    #[error("initialization failed: {0}")]
    Init(String),

    /// バックエンド未設定。
    #[error("backend not configured for location: {0}")]
    NoBackend(String),

    /// ルートなし。
    #[error("no route available: {src} → {dest}, path={path}")]
    NoRouteAvailable {
        src: String,
        dest: String,
        path: String,
    },
}

// ---------------------------------------------------------------------------
// Convenience conversions: allow `?` from common infra error sources
// ---------------------------------------------------------------------------

impl From<std::io::Error> for SyncError {
    fn from(e: std::io::Error) -> Self {
        Self::Infra(InfraError::Io(e))
    }
}

impl From<serde_json::Error> for SyncError {
    fn from(e: serde_json::Error) -> Self {
        Self::Infra(InfraError::Serialization(e.to_string()))
    }
}
