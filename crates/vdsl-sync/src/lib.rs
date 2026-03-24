//! vdsl-sync — N-location file synchronization engine.
//!
//! Tracks files across arbitrary remote locations (local, pod, cloud, NAS, S3, ...),
//! with pluggable storage backends and persistence stores.
//!
//! # Architecture
//!
//! - **Domain** — core entities and rules ([`TrackedFile`], [`Transfer`], [`LocationId`], [`FileType`])
//! - **Application** — use-case orchestration ([`SyncService`], [`TransferEngine`])
//! - **Infrastructure** — persistence and transfer ([`FileStore`], [`TransferStore`], [`StorageBackend`])
//!
//! # Design Principles
//!
//! - **N-location**: arbitrary number of remotes, not hardcoded 3
//! - **Store-agnostic**: `FileStore`/`TransferStore`/`RemoteStore` traits decouple from specific DB
//! - **Backend-agnostic**: `StorageBackend` trait decouples from transfer protocol
//! - **Local-first**: local operations complete immediately, sync is background

pub mod application;
pub mod domain;
pub mod fmt;
pub mod infra;

// Re-exports for convenience
pub use application::route::{TransferDirection, TransferRoute};
pub use application::sync_service::{
    ForceResult, NotifyResult, ScanError, SyncService, SyncServiceBuilder,
};
pub use application::transfer_engine::{BatchError, BatchResult, TransferEngine};
pub use domain::error::SyncError;
pub use domain::file_type::FileType;
pub use domain::graph::RouteGraph;
pub use domain::location::{LocationId, LocationSummary, SyncSummary};
pub use domain::retry::{RetryPolicy, TransferErrorKind};
pub use domain::tracked_file::TrackedFile;
pub use domain::transfer::{Transfer, TransferState};
pub use domain::view::{FileView, PresenceState, PresenceView};
pub use infra::backend::{RemoteFile, StorageBackend};
pub use infra::file_store::FileStore;
pub use infra::hasher::{ContentHasher, HashResult};
pub use infra::remote_store::RemoteStore;
pub use infra::shell::{LocalShell, RemoteShell, ShellOutput};
pub use infra::store::RemoteConfig;
pub use infra::transfer_store::TransferStore;
