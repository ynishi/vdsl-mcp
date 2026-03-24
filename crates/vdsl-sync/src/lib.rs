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
pub mod infra;

// Re-exports: SDK public API face
//
// Interface layers (MCP, Lua) depend only on these types.
// Internal types (domain::*, infra::*) are accessible via direct module paths.

// --- SDK trait + result types ---
pub use application::error::SyncError;
pub use application::sdk::{
    PutReport, SyncReport, SyncReportConflict, SyncReportError, SyncStoreSdk,
};
pub use application::sdk_impl::{SdkImpl, SdkImplBuilder};
pub use application::task::{TaskId, TaskStatus};
pub use application::topology_store::TopologyFileView;

// --- Domain types used in SDK method signatures ---
pub use domain::file_type::FileType;
pub use domain::fingerprint::{FileFingerprint, FingerprintPrecision};
pub use domain::location::{LocationId, LocationSummary, SyncSummary};
pub use domain::view::{ErrorEntry, PendingEntry, PresenceState, PresenceView};

// --- Builder boundary types (SdkImplBuilder construction) ---
pub use infra::backend::StorageBackend;
pub use infra::error::InfraError;
pub use infra::hasher::{ContentHasher, Djb2Hasher};
pub use infra::location::{CloudLocation, LocalLocation, Location, SshLocation};
pub use infra::location_file_store::LocationFileStore;
pub use infra::rclone::RcloneBackend;
pub use infra::shell::{FileInspection, RemoteShell, ShellOutput};
pub use infra::topology_file_store::TopologyFileStore;
pub use infra::transfer_store::TransferStore;
