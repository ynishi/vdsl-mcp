//! vdsl-sync — N-location file synchronization engine.
//!
//! Tracks files across arbitrary remote locations (local, pod, cloud, NAS, S3, ...),
//! with pluggable storage backends and persistence stores.
//!
//! # Architecture
//!
//! - **Domain** — core entities and rules ([`TrackedFile`], [`Transfer`], [`LocationId`], [`FileType`])
//! - **Application** — use-case orchestration ([`Store`], [`StoreBuilder`])
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

// Re-exports: Application (primary API)
pub use application::error::SyncError;
// Observer types: 旧Store/SyncFacade互換用に残存。新SDKパイプラインでは不使用。
pub use application::observer::{
    DeltaSummary, HashProgress, NullObserver, ProgressFnBridge, RecoveryProgress, SyncObserver,
    TransferProgress,
};
pub use application::route::{SrcFile, TransferDirection, TransferRoute};
pub use application::sdk::{
    PutReport, SyncReport, SyncReportConflict, SyncReportError, SyncStoreSdk,
};
pub use application::sdk_impl::{SdkImpl, SdkImplBuilder};
pub use application::store::{PutOptions, PutResult, ScanError, Store, StoreBuilder, SyncResult};
pub use application::sync_facade::{FacadeSyncResult, SyncFacade, SyncFacadeBuilder};
pub use application::task::{ProgressFn, TaskId, TaskStatus};
pub use application::topology_scanner::{ScanResult, TopologyScanError, TopologyScanner};
pub use application::topology_store::{
    TopologyFileView, TopologyPutResult, TopologyStore, TopologySyncResult,
};
pub use application::transfer_engine::{BatchError, BatchResult};
pub use domain::error::DomainError;
pub use domain::file_type::FileType;
pub use domain::fingerprint::{FileFingerprint, FingerprintPrecision};
pub use domain::graph::RouteGraph;
pub use domain::location::{LocationId, LocationSummary, SyncSummary};
pub use domain::plan::Topology;
pub use domain::retry::{RetryPolicy, TransferErrorKind};
pub use domain::scan::{ScanOutcome, ScanReport};
pub use domain::tracked_file::TrackedFile;
pub use domain::transfer::{Transfer, TransferKind, TransferState};
pub use domain::view::{ErrorEntry, FileView, PendingEntry, PresenceState, PresenceView};
pub use infra::backend::{RemoteFile, StorageBackend};
pub use infra::error::InfraError;
pub use infra::file_store::FileStore;
pub use infra::hasher::{ContentHasher, HashResult};
pub use infra::location::{CloudLocation, LocalLocation, Location, LocationKind, SshLocation};
pub use infra::remote_store::RemoteStore;
pub use infra::shell::{FileInspection, LocalShell, RemoteShell, ShellOutput};
pub use infra::store::RemoteConfig;
pub use infra::transfer_store::TransferStore;
