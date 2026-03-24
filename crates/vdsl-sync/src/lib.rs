//! vdsl-sync — N-location file synchronization engine.
//!
//! Topology-centric model: TopologyFile (identity) + LocationFile (per-location state)
//! + RouteGraph (DAG) による分散ファイルストレージの同期エンジン。
//!
//! # Architecture
//!
//! - **Domain** — core entities ([`TopologyFile`], [`LocationFile`], [`Transfer`], [`LocationId`])
//! - **Application** — use-case orchestration ([`SdkImpl`], [`TopologyStore`], [`TopologyScanner`])
//! - **Infrastructure** — persistence and transfer ([`TransferStore`], [`StorageBackend`])

pub mod application;
pub mod domain;
pub mod fmt;
pub mod infra;

// Re-exports: Application (primary API)
pub use application::error::SyncError;
pub use application::route::{SrcFile, TransferDirection, TransferRoute};
pub use application::sdk::{
    PutReport, SyncReport, SyncReportConflict, SyncReportError, SyncStoreSdk,
};
pub use application::sdk_impl::{SdkImpl, SdkImplBuilder};
pub use application::task::{TaskId, TaskStatus};
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
pub use domain::transfer::{Transfer, TransferKind, TransferState};
pub use domain::view::{ErrorEntry, PendingEntry, PresenceState, PresenceView};
pub use infra::backend::{RemoteFile, StorageBackend};
pub use infra::error::InfraError;
pub use infra::hasher::{ContentHasher, HashResult};
pub use infra::location::{CloudLocation, LocalLocation, Location, LocationKind, SshLocation};
pub use infra::shell::{FileInspection, LocalShell, RemoteShell, ShellOutput};
pub use infra::transfer_store::TransferStore;
