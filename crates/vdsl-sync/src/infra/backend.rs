//! Abstract file transfer backend.
//!
//! Each remote location has an associated [`StorageBackend`] that handles
//! push/pull/list/exists operations. vdsl-sync defines the trait;
//! consumers (e.g. vdsl-mcp) provide concrete implementations.

use std::path::Path;

use async_trait::async_trait;

use crate::domain::error::SyncError;

/// A file discovered on a remote location.
#[derive(Debug, Clone)]
pub struct RemoteFile {
    pub path: String,
    pub size: Option<u64>,
}

/// Abstract file transfer backend.
///
/// Implementations handle the actual data movement for a specific protocol.
/// The sync service routes operations to the correct backend based on location.
#[async_trait]
pub trait StorageBackend: Send + Sync {
    /// Push a local file to this remote.
    async fn push(&self, local_path: &Path, remote_path: &str) -> Result<(), SyncError>;

    /// Pull a file from this remote to a local path.
    async fn pull(&self, remote_path: &str, local_path: &Path) -> Result<(), SyncError>;

    /// List files at a remote path.
    async fn list(&self, remote_path: &str) -> Result<Vec<RemoteFile>, SyncError>;

    /// Check if a remote file exists.
    async fn exists(&self, remote_path: &str) -> Result<bool, SyncError>;

    /// Backend type name for display and config matching.
    fn backend_type(&self) -> &str;
}

/// In-memory backend for testing.
#[cfg(any(test, feature = "test-utils"))]
pub mod memory {
    use super::*;
    use std::collections::HashMap;
    use tokio::sync::Mutex;

    /// Records operations for test assertions.
    pub struct InMemoryBackend {
        pub log: Mutex<Vec<Op>>,
        pub fail_next: Mutex<bool>,
        pub files: Mutex<HashMap<String, Vec<u8>>>,
    }

    impl Default for InMemoryBackend {
        fn default() -> Self {
            Self {
                log: Mutex::new(Vec::new()),
                fail_next: Mutex::new(false),
                files: Mutex::new(HashMap::new()),
            }
        }
    }

    #[derive(Debug, Clone)]
    pub enum Op {
        Push { local: String, remote: String },
        Pull { remote: String, local: String },
        List { path: String },
        Exists { path: String },
    }

    #[async_trait]
    impl StorageBackend for InMemoryBackend {
        async fn push(&self, local_path: &Path, remote_path: &str) -> Result<(), SyncError> {
            self.log.lock().await.push(Op::Push {
                local: local_path.display().to_string(),
                remote: remote_path.into(),
            });
            let mut guard = self.fail_next.lock().await;
            if *guard {
                *guard = false;
                return Err(SyncError::TransferFailed("mock push error".into()));
            }
            Ok(())
        }

        async fn pull(&self, remote_path: &str, local_path: &Path) -> Result<(), SyncError> {
            self.log.lock().await.push(Op::Pull {
                remote: remote_path.into(),
                local: local_path.display().to_string(),
            });
            let mut guard = self.fail_next.lock().await;
            if *guard {
                *guard = false;
                return Err(SyncError::TransferFailed("mock pull error".into()));
            }
            Ok(())
        }

        async fn list(&self, remote_path: &str) -> Result<Vec<RemoteFile>, SyncError> {
            self.log.lock().await.push(Op::List {
                path: remote_path.into(),
            });
            Ok(vec![])
        }

        async fn exists(&self, remote_path: &str) -> Result<bool, SyncError> {
            self.log.lock().await.push(Op::Exists {
                path: remote_path.into(),
            });
            Ok(self.files.lock().await.contains_key(remote_path))
        }

        fn backend_type(&self) -> &str {
            "memory"
        }
    }
}
