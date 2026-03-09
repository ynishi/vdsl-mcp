//! Sync engine error types.

use std::path::PathBuf;

/// Errors produced by the sync engine.
#[derive(Debug, thiserror::Error)]
pub enum SyncError {
    #[error("invalid file type: {0}")]
    InvalidFileType(String),

    #[error("invalid location: {0}")]
    InvalidLocation(String),

    #[error("invalid location state: {0}")]
    InvalidLocationState(String),

    #[error("file not found: {}", .0.display())]
    FileNotFound(PathBuf),

    #[error("path is outside sync root: {path}")]
    OutsideSyncRoot { path: String },

    #[error("duplicate file: {path} is a duplicate of {duplicate_of}")]
    Duplicate { path: String, duplicate_of: String },

    #[error("file not registered in sync store: {0}")]
    NotRegistered(String),

    #[error("backend not configured for location: {0}")]
    NoBackend(String),

    #[error("no route available: dest={dest}, path={path}")]
    NoRouteAvailable { dest: String, path: String },

    #[error("transfer failed: {0}")]
    TransferFailed(String),

    #[error("store error: {0}")]
    Store(String),

    #[error("hash computation failed: {0}")]
    Hash(String),

    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
}
