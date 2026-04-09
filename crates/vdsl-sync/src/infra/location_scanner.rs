//! LocationScanner — Locationごとのファイルスキャン抽象。
//!
//! 「このLocationに何があるか」を問い合わせるinfraトレイト。
//! Local/SSH/Cloudの3種を多態で扱う。
//!
//! TransferRouteは「AからBへどう運ぶか」を知る構造体であり、
//! 「Aに何があるか」を知る責務ではない。この分離がLocationScannerの存在理由。
//!
//! # フロー
//!
//! ```text
//! LocationScanner.scan() → ScannedFile[]
//!     ↓
//! Domain純粋関数 compute_deltas(TopologyFile[], ScannedFile[]) → TopologyDelta[]
//! ```

use std::path::{Path, PathBuf};
use std::sync::Arc;

use async_trait::async_trait;

use crate::domain::file_type::FileType;
use crate::domain::fingerprint::FileFingerprint;
use crate::domain::location::LocationId;
use crate::infra::error::InfraError;
use crate::infra::hasher::{ContentHasher, HashResult};
use crate::infra::shell::RemoteShell;

use super::backend::StorageBackend;

// =============================================================================
// ScannedFile — scan出力型
// =============================================================================

/// スキャンで検出された1ファイル。
///
/// LocationScanner.scan() の出力単位。
/// Domain層のTopologyFile/LocationFileとは独立した、infra由来の生データ。
#[derive(Debug, Clone)]
pub struct ScannedFile {
    pub relative_path: String,
    pub file_type: FileType,
    pub fingerprint: FileFingerprint,
    pub origin: LocationId,
    pub embedded_id: Option<String>,
}

/// スキャン中の非致命的エラー（個別ファイル単位）。
#[derive(Debug, Clone)]
pub struct ScanError {
    pub path: String,
    pub error: String,
}

/// スキャン結果。
pub struct LocationScanResult {
    pub files: Vec<ScannedFile>,
    pub errors: Vec<ScanError>,
}

// =============================================================================
// LocationScanner trait
// =============================================================================

/// Locationのファイル一覧を取得するトレイト。
///
/// 各Locationは自分のスキャン方法を知っている:
/// - Local: walkdir + ContentHasher
/// - SSH: RemoteShell + batch_inspect
/// - Cloud: StorageBackend.list() (metadata only)
#[async_trait]
pub trait LocationScanner: Send + Sync {
    /// このスキャナーが担当するLocationId。
    fn location_id(&self) -> &LocationId;

    /// ファイルスキャンを実行する。
    ///
    /// excludes: スキャン除外globパターン。
    async fn scan(&self, excludes: &[glob::Pattern]) -> Result<LocationScanResult, InfraError>;
}

// =============================================================================
// LocalScanner
// =============================================================================

/// ローカルファイルシステムのスキャナー。
///
/// walkdir + ContentHasherでファイルリスト + ハッシュを取得する。
/// incremental scan: (size, mtime) 不変ならDB上のhashを再利用可能
/// （呼び出し側がLocationFileStoreとの突合で判断）。
pub struct LocalScanner {
    location_id: LocationId,
    root: PathBuf,
    hasher: Arc<dyn ContentHasher>,
}

impl LocalScanner {
    pub fn new(location_id: LocationId, root: PathBuf, hasher: Arc<dyn ContentHasher>) -> Self {
        Self {
            location_id,
            root,
            hasher,
        }
    }
}

#[async_trait]
impl LocationScanner for LocalScanner {
    fn location_id(&self) -> &LocationId {
        &self.location_id
    }

    async fn scan(&self, excludes: &[glob::Pattern]) -> Result<LocationScanResult, InfraError> {
        if !self.root.is_dir() {
            tracing::warn!(
                location = %self.location_id,
                root = %self.root.display(),
                "local_scan: root is not a directory"
            );
            return Ok(LocationScanResult {
                files: Vec::new(),
                errors: Vec::new(),
            });
        }

        tracing::debug!(
            location = %self.location_id,
            root = %self.root.display(),
            excludes = excludes.len(),
            "local_scan: listing files"
        );
        let files = list_local_files(&self.root).await?;
        tracing::debug!(
            location = %self.location_id,
            raw_files = files.len(),
            "local_scan: walkdir done"
        );

        let mut scanned = Vec::new();
        let mut errors = Vec::new();
        let mut excluded = 0usize;

        for (relative_path, size, modified_at) in files {
            if excludes.iter().any(|p| p.matches(&relative_path)) {
                excluded += 1;
                tracing::trace!(
                    path = %relative_path,
                    location = %self.location_id,
                    "local_scan: excluded by pattern"
                );
                continue;
            }

            let file_type = infer_file_type(&relative_path);
            let abs_path = self.root.join(&relative_path);

            match hash_local_file(&self.hasher, &abs_path).await {
                Ok((hash_result, file_size)) => {
                    scanned.push(ScannedFile {
                        relative_path,
                        file_type,
                        fingerprint: FileFingerprint {
                            byte_digest: Some(crate::domain::digest::ByteDigest::Djb2(
                                hash_result.file_hash,
                            )),
                            content_digest: hash_result
                                .content_hash
                                .map(crate::domain::digest::ContentDigest),
                            meta_digest: None,
                            size: file_size.unwrap_or(size),
                            modified_at,
                        },
                        origin: self.location_id.clone(),
                        embedded_id: None,
                    });
                }
                Err(e) => {
                    tracing::warn!(
                        path = %abs_path.display(),
                        location = %self.location_id,
                        error = %e,
                        "local_scan: hash failed"
                    );
                    errors.push(ScanError {
                        path: abs_path.display().to_string(),
                        error: format!("hash failed: {e}"),
                    });
                }
            }
        }

        tracing::info!(
            location = %self.location_id,
            scanned = scanned.len(),
            excluded = excluded,
            errors = errors.len(),
            "local_scan: complete"
        );

        Ok(LocationScanResult {
            files: scanned,
            errors,
        })
    }
}

// =============================================================================
// SshScanner
// =============================================================================

/// SSH経由リモートホストのスキャナー。
///
/// RemoteShell.batch_inspect() でファイルリスト + SHA256ハッシュを一括取得する。
pub struct SshScanner {
    location_id: LocationId,
    root: PathBuf,
    shell: Arc<dyn RemoteShell>,
}

impl SshScanner {
    pub fn new(location_id: LocationId, root: PathBuf, shell: Arc<dyn RemoteShell>) -> Self {
        Self {
            location_id,
            root,
            shell,
        }
    }
}

#[async_trait]
impl LocationScanner for SshScanner {
    fn location_id(&self) -> &LocationId {
        &self.location_id
    }

    async fn scan(&self, excludes: &[glob::Pattern]) -> Result<LocationScanResult, InfraError> {
        let root_str = self.root.to_str().ok_or_else(|| InfraError::Transfer {
            reason: format!(
                "ssh scan root is not valid UTF-8: {}",
                self.root.to_string_lossy()
            ),
        })?;

        // Phase 1: find でファイルリスト取得
        // -L: symlinkをfollow（pod上の output/ が symlink の場合に対応）
        let find_cmd = format!(
            "find -L '{}' -type f -not -name '.*'",
            root_str.replace('\'', "'\\''")
        );
        let output = self
            .shell
            .exec(&["bash", "-c", &find_cmd], Some(60))
            .await
            .map_err(|e| InfraError::Transfer {
                reason: format!("ssh exec failed: {e}"),
            })?;
        if !output.success {
            return Err(InfraError::Transfer {
                reason: format!(
                    "ssh find failed (exit {:?}): {}",
                    output.exit_code, output.stderr
                ),
            });
        }

        let root_prefix = if root_str.ends_with('/') {
            root_str.to_string()
        } else {
            format!("{}/", root_str)
        };

        let relative_paths: Vec<String> = output
            .stdout
            .lines()
            .filter_map(|line| {
                let line = line.trim();
                if line.is_empty() {
                    return None;
                }
                let rel = line.strip_prefix(&root_prefix).unwrap_or(line);
                if excludes.iter().any(|p| p.matches(rel)) {
                    return None;
                }
                Some(rel.to_string())
            })
            .collect();

        if relative_paths.is_empty() {
            return Ok(LocationScanResult {
                files: Vec::new(),
                errors: Vec::new(),
            });
        }

        // Phase 2: batch_inspect でハッシュ + サイズ一括取得
        let inspections = self
            .shell
            .batch_inspect(root_str, &relative_paths)
            .await
            .map_err(|e| InfraError::Transfer {
                reason: format!("ssh batch_inspect failed: {e}"),
            })?;

        let files: Vec<ScannedFile> = inspections
            .into_iter()
            .map(|fi| ScannedFile {
                file_type: infer_file_type(&fi.relative_path),
                fingerprint: FileFingerprint {
                    byte_digest: Some(crate::domain::digest::ByteDigest::Sha256(fi.sha256)),
                    content_digest: None,
                    meta_digest: None,
                    size: fi.size,
                    modified_at: None,
                },
                relative_path: fi.relative_path,
                origin: self.location_id.clone(),
                embedded_id: None,
            })
            .collect();

        Ok(LocationScanResult {
            files,
            errors: Vec::new(),
        })
    }
}

// =============================================================================
// CloudScanner
// =============================================================================

/// Cloud storage (rclone remote) のスキャナー。
///
/// StorageBackend.list() でメタデータのみ取得する。
/// コンテンツハッシュはダウンロードが必要なため取得しない。
pub struct CloudScanner {
    location_id: LocationId,
    root: PathBuf,
    backend: Arc<dyn StorageBackend>,
}

impl CloudScanner {
    pub fn new(location_id: LocationId, root: PathBuf, backend: Arc<dyn StorageBackend>) -> Self {
        Self {
            location_id,
            root,
            backend,
        }
    }
}

#[async_trait]
impl LocationScanner for CloudScanner {
    fn location_id(&self) -> &LocationId {
        &self.location_id
    }

    async fn scan(&self, excludes: &[glob::Pattern]) -> Result<LocationScanResult, InfraError> {
        let root_str = self.root.to_str().ok_or_else(|| InfraError::Transfer {
            reason: format!(
                "cloud scan root is not valid UTF-8: {}",
                self.root.to_string_lossy()
            ),
        })?;

        let remote_files = self
            .backend
            .list(root_str)
            .await
            .map_err(|e| InfraError::Transfer {
                reason: format!("cloud list failed: {e}"),
            })?;

        let files: Vec<ScannedFile> = remote_files
            .into_iter()
            .filter(|f| !excludes.iter().any(|p| p.matches(&f.path)))
            .map(|f| ScannedFile {
                file_type: infer_file_type(&f.path),
                fingerprint: FileFingerprint {
                    byte_digest: None,
                    content_digest: None,
                    meta_digest: None,
                    size: f.size.unwrap_or(0),
                    modified_at: f.modified_at,
                },
                relative_path: f.path,
                origin: self.location_id.clone(),
                embedded_id: None,
            })
            .collect();

        Ok(LocationScanResult {
            files,
            errors: Vec::new(),
        })
    }
}

// =============================================================================
// Helpers
// =============================================================================

/// 拡張子からFileTypeを推定する。
fn infer_file_type(relative_path: &str) -> FileType {
    Path::new(relative_path)
        .extension()
        .and_then(|e| e.to_str())
        .map(FileType::from_extension)
        .unwrap_or(FileType::Asset)
}

/// ローカルファイルのハッシュ計算（blocking task）。
async fn hash_local_file(
    hasher: &Arc<dyn ContentHasher>,
    path: &Path,
) -> Result<(HashResult, Option<u64>), InfraError> {
    let hasher = Arc::clone(hasher);
    let hash_path = path.to_path_buf();
    let hash_result = tokio::task::spawn_blocking(move || hasher.hash_file(&hash_path))
        .await
        .map_err(|e| InfraError::Hash {
            op: "hasher",
            reason: format!("spawn_blocking join failed: {e}"),
        })?
        .map_err(|e| InfraError::Hash {
            op: "hasher",
            reason: format!("hash_file failed: {e}"),
        })?;
    let file_size = tokio::fs::metadata(path)
        .await
        .map(|m| Some(m.len()))
        .unwrap_or(None);
    Ok((hash_result, file_size))
}

/// ローカルディレクトリの再帰ファイルリスト取得。
///
/// 返り値: (relative_path, size, modified_at) のタプル。
async fn list_local_files(
    root: &Path,
) -> Result<Vec<(String, u64, Option<chrono::DateTime<chrono::Utc>>)>, InfraError> {
    use chrono::{DateTime, Utc};

    let root = root.to_path_buf();
    tokio::task::spawn_blocking(move || {
        let mut result = Vec::new();
        let walker = walkdir::WalkDir::new(&root)
            .follow_links(false)
            .into_iter()
            .filter_map(|e| e.ok());

        for entry in walker {
            if !entry.file_type().is_file() {
                continue;
            }
            // 隠しファイル除外
            if entry
                .file_name()
                .to_str()
                .is_some_and(|n| n.starts_with('.'))
            {
                continue;
            }

            let abs_path = entry.path();
            let relative = abs_path
                .strip_prefix(&root)
                .map(|p| p.to_string_lossy().to_string())
                .unwrap_or_default();

            if relative.is_empty() {
                continue;
            }

            let metadata = entry.metadata().ok();
            let size = metadata.as_ref().map_or(0, |m| m.len());
            let modified_at: Option<DateTime<Utc>> = metadata
                .and_then(|m| m.modified().ok())
                .map(DateTime::<Utc>::from);

            result.push((relative, size, modified_at));
        }
        Ok(result)
    })
    .await
    .map_err(|e| InfraError::Hash {
        op: "list_local_files",
        reason: format!("spawn_blocking join failed: {e}"),
    })?
}
