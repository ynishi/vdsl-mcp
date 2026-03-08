//! vdsl-sync — N-location file synchronization engine.
//!
//! Tracks files across arbitrary remote locations (local, pod, cloud, NAS, S3, ...),
//! with pluggable storage backends and persistence stores.
//!
//! # Architecture
//!
//! - **Domain** — core entities and rules ([`SyncEntry`], [`LocationId`], [`FileType`])
//! - **Application** — use-case orchestration ([`SyncService`])
//! - **Infrastructure** — persistence and transfer ([`SyncStore`], [`StorageBackend`], [`ContentHasher`])
//!
//! # Design Principles
//!
//! - **N-location**: arbitrary number of remotes, not hardcoded 3
//! - **Store-agnostic**: `SyncStore` trait decouples from specific DB
//! - **Backend-agnostic**: `StorageBackend` trait decouples from transfer protocol
//! - **Local-first**: local operations complete immediately, sync is background

pub mod application;
pub mod domain;
pub mod fmt;
pub mod infra;

// Re-exports for convenience
pub use application::sync_service::{
    BatchResult, NotifyResult, RegisterOpts, RegisterResult, SyncService,
};
pub use domain::entry::SyncEntry;
pub use domain::error::SyncError;
pub use domain::file_type::FileType;
pub use domain::location::{LocationId, LocationState, LocationSummary, SyncSummary};
pub use infra::backend::{RemoteFile, StorageBackend};
pub use infra::hasher::{ContentHasher, HashResult};
pub use infra::store::{RemoteConfig, SyncStore};
