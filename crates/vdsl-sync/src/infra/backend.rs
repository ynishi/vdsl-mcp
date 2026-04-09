//! Abstract file transfer backend.
//!
//! Each remote location has an associated [`StorageBackend`] that handles
//! push/pull/list/exists operations. vdsl-sync defines the trait;
//! consumers (e.g. vdsl-mcp) provide concrete implementations.

use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

use async_trait::async_trait;
use chrono::{DateTime, Utc};

use crate::infra::error::InfraError;

/// Progress callback for reporting transfer phase changes.
///
/// Called with a human-readable phase description:
/// - `"pushing to pod-xxx: chunk 5/22 (500/2111)"`
/// - `"pulling from cloud: 1200 files"`
/// - `"target cloud: 4383 queued"`
pub type ProgressFn = Arc<dyn Fn(&str) + Send + Sync>;

/// A file discovered on a remote location.
///
/// Metadata available depends on the storage backend:
/// - `size`: most backends provide this (rclone lsf `%s`)
/// - `modified_at`: available from rclone lsf `%t` (ISO 8601)
///
/// Used for metadata-based change detection on Cloud storage
/// where content hash computation requires downloading the file.
#[derive(Debug, Clone)]
pub struct RemoteFile {
    pub path: String,
    pub size: Option<u64>,
    /// Last modification time reported by the storage backend.
    pub modified_at: Option<DateTime<Utc>>,
}

/// Abstract file transfer backend.
///
/// Implementations handle the actual data movement for a specific protocol.
/// The sync service routes operations to the correct backend based on location.
#[async_trait]
pub trait StorageBackend: Send + Sync {
    /// Push a local file to this remote.
    async fn push(&self, local_path: &Path, remote_path: &str) -> Result<(), InfraError>;

    /// Pull a file from this remote to a local path.
    async fn pull(&self, remote_path: &str, local_path: &Path) -> Result<(), InfraError>;

    /// List files at a remote path.
    async fn list(&self, remote_path: &str) -> Result<Vec<RemoteFile>, InfraError>;

    /// Check if a remote file exists.
    async fn exists(&self, remote_path: &str) -> Result<bool, InfraError>;

    /// Delete a file on this remote.
    ///
    /// Returns `Ok(())` if the file was deleted or didn't exist.
    /// Default implementation returns `Err` — backends that support deletion
    /// must override this.
    async fn delete(&self, remote_path: &str) -> Result<(), InfraError> {
        Err(InfraError::Transfer {
            reason: format!(
                "delete not supported by {} backend for path: {remote_path}",
                self.backend_type()
            ),
        })
    }

    /// Move a file to an archive path (soft delete).
    ///
    /// Semantics: `src_remote_path` is moved to `archive_remote_path` atomically.
    /// Used by cold-storage backends (B2) to preserve deleted file revisions
    /// instead of hard-deleting them. The caller constructs the archive path
    /// (typically `{archive_root}/{ISO8601_ts}/{relative_path}`).
    ///
    /// Default implementation returns `Err` — backends that don't support
    /// archive-on-delete should leave this unimplemented; callers must check
    /// before invoking.
    async fn archive_move(
        &self,
        src_remote_path: &str,
        archive_remote_path: &str,
    ) -> Result<(), InfraError> {
        Err(InfraError::Transfer {
            reason: format!(
                "archive_move not supported by {} backend (src={src_remote_path}, dest={archive_remote_path})",
                self.backend_type()
            ),
        })
    }

    /// Batch archive-move: relocate multiple files from `src_root` to
    /// `archive_dest_root` preserving relative paths.
    ///
    /// Semantics: for each `relative_path`, moves
    /// `{src_root}/{relative_path}` → `{archive_dest_root}/{relative_path}`.
    ///
    /// Default implementation falls back to sequential `archive_move()`.
    async fn archive_move_batch(
        &self,
        src_root: &str,
        archive_dest_root: &str,
        relative_paths: &[String],
    ) -> HashMap<String, Result<(), InfraError>> {
        let mut results = HashMap::with_capacity(relative_paths.len());
        for rel in relative_paths {
            let src = if src_root.is_empty() {
                rel.clone()
            } else {
                format!("{src_root}/{rel}")
            };
            let dest = if archive_dest_root.is_empty() {
                rel.clone()
            } else {
                format!("{archive_dest_root}/{rel}")
            };
            let result = self.archive_move(&src, &dest).await;
            results.insert(rel.clone(), result);
        }
        results
    }

    /// Push multiple files in a single batch operation.
    ///
    /// `src_root` is the local base directory, `dest_root` is the remote base,
    /// and `relative_paths` are paths relative to both roots.
    ///
    /// Returns a map of relative_path → Ok/Err for per-file status tracking.
    /// Default implementation falls back to sequential `push()` calls.
    async fn push_batch(
        &self,
        src_root: &Path,
        dest_root: &str,
        relative_paths: &[String],
    ) -> HashMap<String, Result<(), InfraError>> {
        let mut results = HashMap::with_capacity(relative_paths.len());
        for rel in relative_paths {
            let local_path = src_root.join(rel);
            let remote_path = if dest_root.is_empty() {
                rel.clone()
            } else {
                format!("{dest_root}/{rel}")
            };
            let result = self.push(&local_path, &remote_path).await;
            results.insert(rel.clone(), result);
        }
        results
    }

    /// Pull multiple files in a single batch operation.
    ///
    /// `src_root` is the remote base, `dest_root` is the local base directory,
    /// and `relative_paths` are paths relative to both roots.
    ///
    /// Returns a map of relative_path → Ok/Err for per-file status tracking.
    /// Default implementation falls back to sequential `pull()` calls.
    async fn pull_batch(
        &self,
        src_root: &str,
        dest_root: &Path,
        relative_paths: &[String],
    ) -> HashMap<String, Result<(), InfraError>> {
        let mut results = HashMap::with_capacity(relative_paths.len());
        for rel in relative_paths {
            let remote_path = if src_root.is_empty() {
                rel.clone()
            } else {
                format!("{src_root}/{rel}")
            };
            let local_path = dest_root.join(rel);
            let result = self.pull(&remote_path, &local_path).await;
            results.insert(rel.clone(), result);
        }
        results
    }

    /// Delete multiple files in a single batch operation.
    ///
    /// `remote_root` is the remote base directory, `relative_paths` are paths
    /// relative to it. Uses `rclone delete --files-from` for rclone backends.
    ///
    /// Returns a map of relative_path → Ok/Err for per-file status tracking.
    /// Default implementation falls back to sequential `delete()` calls.
    async fn delete_batch(
        &self,
        remote_root: &str,
        relative_paths: &[String],
    ) -> HashMap<String, Result<(), InfraError>> {
        let mut results = HashMap::with_capacity(relative_paths.len());
        for rel in relative_paths {
            let remote_path = if remote_root.is_empty() {
                rel.clone()
            } else {
                format!("{remote_root}/{rel}")
            };
            let result = self.delete(&remote_path).await;
            results.insert(rel.clone(), result);
        }
        results
    }

    /// Whether this backend supports efficient batch push/pull.
    ///
    /// When true, callers should prefer `push_batch`/`pull_batch`/`delete_batch`
    /// over individual calls. Default: false (sequential fallback).
    fn supports_batch(&self) -> bool {
        false
    }

    /// Backend type name for display and config matching.
    fn backend_type(&self) -> &str;

    /// Set a progress callback for batch operations.
    ///
    /// Called by the sync engine before batch execution.
    /// Implementations that support chunked transfers (e.g. RcloneBackend)
    /// should call this callback on chunk completion.
    /// Default: no-op (callback is ignored).
    fn set_progress_callback(&self, _callback: Option<ProgressFn>) {}

    /// 外部ツールの到達確認 + 確保。
    ///
    /// - rclone: バイナリ存在確認 → なければインストール → 接続テスト
    /// - memory: 常にOk
    ///
    /// デフォルト実装: `list("")` で接続テスト（バイナリが存在しなければここで失敗する）。
    async fn ensure(&self) -> Result<(), InfraError> {
        self.list("").await.map(|_| ())
    }
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
        Delete { path: String },
    }

    #[async_trait]
    impl StorageBackend for InMemoryBackend {
        async fn push(&self, local_path: &Path, remote_path: &str) -> Result<(), InfraError> {
            self.log.lock().await.push(Op::Push {
                local: local_path.display().to_string(),
                remote: remote_path.into(),
            });
            let mut guard = self.fail_next.lock().await;
            if *guard {
                *guard = false;
                return Err(InfraError::Transfer {
                    reason: "mock push error".into(),
                }
                .into());
            }
            Ok(())
        }

        async fn pull(&self, remote_path: &str, local_path: &Path) -> Result<(), InfraError> {
            self.log.lock().await.push(Op::Pull {
                remote: remote_path.into(),
                local: local_path.display().to_string(),
            });
            let mut guard = self.fail_next.lock().await;
            if *guard {
                *guard = false;
                return Err(InfraError::Transfer {
                    reason: "mock pull error".into(),
                }
                .into());
            }
            Ok(())
        }

        async fn list(&self, remote_path: &str) -> Result<Vec<RemoteFile>, InfraError> {
            self.log.lock().await.push(Op::List {
                path: remote_path.into(),
            });
            let files = self.files.lock().await;
            Ok(files
                .iter()
                .map(|(path, data)| RemoteFile {
                    path: path.clone(),
                    size: Some(data.len() as u64),
                    modified_at: None,
                })
                .collect())
        }

        async fn exists(&self, remote_path: &str) -> Result<bool, InfraError> {
            self.log.lock().await.push(Op::Exists {
                path: remote_path.into(),
            });
            Ok(self.files.lock().await.contains_key(remote_path))
        }

        async fn delete(&self, remote_path: &str) -> Result<(), InfraError> {
            self.log.lock().await.push(Op::Delete {
                path: remote_path.into(),
            });
            let mut guard = self.fail_next.lock().await;
            if *guard {
                *guard = false;
                return Err(InfraError::Transfer {
                    reason: "mock delete error".into(),
                }
                .into());
            }
            self.files.lock().await.remove(remote_path);
            Ok(())
        }

        fn backend_type(&self) -> &str {
            "memory"
        }
    }

    /// Blanket impl so `Arc<InMemoryBackend>` can be used as a `StorageBackend`.
    ///
    /// Avoids orphan-rule workarounds (newtype wrapper) in every test module.
    #[async_trait]
    impl StorageBackend for std::sync::Arc<InMemoryBackend> {
        async fn push(&self, local_path: &Path, remote_path: &str) -> Result<(), InfraError> {
            (**self).push(local_path, remote_path).await
        }
        async fn pull(&self, remote_path: &str, local_path: &Path) -> Result<(), InfraError> {
            (**self).pull(remote_path, local_path).await
        }
        async fn list(&self, remote_path: &str) -> Result<Vec<RemoteFile>, InfraError> {
            (**self).list(remote_path).await
        }
        async fn exists(&self, remote_path: &str) -> Result<bool, InfraError> {
            (**self).exists(remote_path).await
        }
        async fn delete(&self, remote_path: &str) -> Result<(), InfraError> {
            (**self).delete(remote_path).await
        }
        async fn push_batch(
            &self,
            src_root: &Path,
            dest_root: &str,
            relative_paths: &[String],
        ) -> HashMap<String, Result<(), InfraError>> {
            (**self)
                .push_batch(src_root, dest_root, relative_paths)
                .await
        }
        async fn delete_batch(
            &self,
            remote_root: &str,
            relative_paths: &[String],
        ) -> HashMap<String, Result<(), InfraError>> {
            (**self).delete_batch(remote_root, relative_paths).await
        }
        fn supports_batch(&self) -> bool {
            (**self).supports_batch()
        }
        fn backend_type(&self) -> &str {
            (**self).backend_type()
        }
        async fn ensure(&self) -> Result<(), InfraError> {
            (**self).ensure().await
        }
        fn set_progress_callback(&self, callback: Option<ProgressFn>) {
            (**self).set_progress_callback(callback);
        }
    }
}
