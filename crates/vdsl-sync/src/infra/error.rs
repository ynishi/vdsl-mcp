//! Infrastructure error — 外部システム障害。
//!
//! DB操作、ファイルハッシュ計算、ストレージバックエンド転送、
//! シリアライズ、I/O等のインフラ固有エラー。
//!
//! アプリケーション層の [`SyncError`](crate::application::error::SyncError) が
//! `#[from]` でこのエラーを包含する。

use std::path::PathBuf;

/// インフラストラクチャ障害。
#[derive(Debug, thiserror::Error)]
pub enum InfraError {
    #[error("store error ({op}): {reason}")]
    Store { op: &'static str, reason: String },

    #[error("hash computation failed ({op}): {reason}")]
    Hash { op: &'static str, reason: String },

    #[error("transfer failed: {reason}")]
    Transfer { reason: String },

    #[error("file not found: {}", .0.display())]
    FileNotFound(PathBuf),

    #[error("serialization error: {0}")]
    Serialization(String),

    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
}
