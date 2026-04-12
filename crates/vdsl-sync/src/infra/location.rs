//! Location — 拠点の多態抽象。
//!
//! 各拠点は「何があるか」(scan) と「どこにファイルがあるか」(file_root) を知っている。
//! Local/SSH/Cloud で処理内容が根本的に異なるため、trait による多態で実装を切り替える。
//!
//! # 層配置
//!
//! `Location` trait は infra層に配置する。
//! 理由: 実装が RemoteShell, StorageBackend, ContentHasher 等の infra型に依存するため。
//! Domain層の `LocationId` は値オブジェクト（識別子のみ）として残る。
//!
//! # 責務
//!
//! - `id()` → この拠点の識別子
//! - `kind()` → 拠点の物理的分類（コスト推定に使用）
//! - `file_root()` → ファイルのベースパス
//! - `scanner()` → この拠点のスキャン能力（LocationScanner）
//! - `ensure()` → 到達確認 + 外部ツールの確保（rclone等）

use std::path::{Path, PathBuf};
use std::sync::Arc;

use async_trait::async_trait;

use crate::domain::location::LocationId;
use crate::infra::error::InfraError;
use crate::infra::location_scanner::LocationScanner;

/// 拠点の物理的分類。
///
/// `SdkImplBuilder::build()` でルートコストを自動推定するために使用する。
/// 2拠点間の転送コストは、双方の `LocationKind` の組み合わせで決まる:
///
/// | src → dest | コスト | 根拠 |
/// |---|---|---|
/// | Local → Remote | 1.0 | LAN/SSH、低レイテンシ |
/// | Remote → Cloud | 2.0 | DC帯域、中速 |
/// | Local → Cloud | 5.0 | 家庭回線アップロード、低速 |
/// | Cloud → Remote | 2.0 | DC帯域、中速 |
/// | Cloud → Local | 5.0 | 家庭回線ダウンロード |
/// | Remote → Local | 1.0 | LAN/SSH |
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum LocationKind {
    /// ローカルファイルシステム（開発マシン等）。
    Local,
    /// SSH経由リモートホスト（GPU Pod, NAS等）。データセンター帯域。
    Remote,
    /// クラウドストレージ（B2, S3等）。オブジェクトストア。
    Cloud,
}

/// 拠点の多態抽象。
///
/// 各拠点は自分のスキャン方法を知っている:
/// - Local: walkdir + ContentHasher
/// - SSH: RemoteShell + batch_inspect
/// - Cloud: StorageBackend.list() (metadata only)
///
/// `Location` trait 実装を `SdkImplBuilder::location()` に渡すことで、
/// Scanner と Route の整合性が保証される。
/// `kind()` は `SdkImplBuilder::build()` でルートコストの自動推定に使用される。
#[async_trait]
pub trait Location: Send + Sync {
    /// この拠点の識別子。
    fn id(&self) -> &LocationId;

    /// 拠点の物理的分類。
    ///
    /// ルート間コスト推定に使用される。
    fn kind(&self) -> LocationKind;

    /// ファイルのベースパス。
    ///
    /// Local: `/Users/.../output`
    /// Pod: `/workspace/comfyui/output`
    /// Cloud: `vdsl/output`
    fn file_root(&self) -> &Path;

    /// この拠点のスキャナーを返す。
    ///
    /// 各実装が自分のスキャン方法に応じたLocationScannerを構築して返す。
    fn scanner(&self) -> Arc<dyn LocationScanner>;

    /// 拠点の到達可能性を検証し、必要な外部ツールを確保する。
    ///
    /// sync開始前に全Locationに対して呼ばれる。
    /// - Local: file_rootの存在確認（なければ作成）
    /// - SSH: SSH接続テスト
    /// - Cloud: rcloneバイナリ確認 + バケット接続テスト
    ///
    /// 失敗時は早期エラーで、数分かかるscanを無駄にしない。
    async fn ensure(&self) -> Result<(), InfraError>;
}

// =============================================================================
// LocalLocation
// =============================================================================

use crate::infra::hasher::ContentHasher;
use crate::infra::location_scanner::LocalScanner;

/// ローカルファイルシステムの拠点。
///
/// walkdir + ContentHasher でスキャンする。
pub struct LocalLocation {
    id: LocationId,
    root: PathBuf,
    hasher: Arc<dyn ContentHasher>,
}

impl LocalLocation {
    pub fn new(root: PathBuf, hasher: Arc<dyn ContentHasher>) -> Self {
        Self {
            id: LocationId::local(),
            root,
            hasher,
        }
    }
}

#[async_trait]
impl Location for LocalLocation {
    fn id(&self) -> &LocationId {
        &self.id
    }

    fn kind(&self) -> LocationKind {
        LocationKind::Local
    }

    fn file_root(&self) -> &Path {
        &self.root
    }

    fn scanner(&self) -> Arc<dyn LocationScanner> {
        Arc::new(LocalScanner::new(
            self.id.clone(),
            self.root.clone(),
            self.hasher.clone(),
        ))
    }

    async fn ensure(&self) -> Result<(), InfraError> {
        if !self.root.exists() {
            std::fs::create_dir_all(&self.root).map_err(|e| {
                InfraError::Init(format!(
                    "local file_root '{}' does not exist and could not be created: {e}",
                    self.root.display()
                ))
            })?;
        }
        if !self.root.is_dir() {
            return Err(InfraError::Init(format!(
                "local file_root '{}' exists but is not a directory",
                self.root.display()
            )));
        }
        Ok(())
    }
}

// =============================================================================
// SshLocation
// =============================================================================

use crate::infra::location_scanner::SshScanner;
use crate::infra::shell::RemoteShell;

/// SSH経由リモートホストの拠点。
///
/// RemoteShell.batch_inspect() でスキャンする。
pub struct SshLocation {
    id: LocationId,
    root: PathBuf,
    shell: Arc<dyn RemoteShell>,
}

impl SshLocation {
    pub fn new(id: LocationId, root: PathBuf, shell: Arc<dyn RemoteShell>) -> Self {
        Self { id, root, shell }
    }
}

#[async_trait]
impl Location for SshLocation {
    fn id(&self) -> &LocationId {
        &self.id
    }

    fn kind(&self) -> LocationKind {
        LocationKind::Remote
    }

    fn file_root(&self) -> &Path {
        &self.root
    }

    fn scanner(&self) -> Arc<dyn LocationScanner> {
        Arc::new(SshScanner::new(
            self.id.clone(),
            self.root.clone(),
            self.shell.clone(),
        ))
    }

    async fn ensure(&self) -> Result<(), InfraError> {
        let output = self.shell.exec(&["echo", "pong"], Some(30)).await?;
        if !output.success {
            return Err(InfraError::Init(format!(
                "SSH location '{}' unreachable (exit {}): {}",
                self.id,
                output.exit_code.unwrap_or(-1),
                output.stderr.trim()
            )));
        }
        Ok(())
    }
}

// =============================================================================
// CloudLocation
// =============================================================================

use crate::infra::backend::StorageBackend;
use crate::infra::location_scanner::CloudScanner;

/// Cloud storage の拠点。
///
/// StorageBackend.list() でメタデータのみ取得する。
/// コンテンツハッシュはダウンロードが必要なため取得しない。
pub struct CloudLocation {
    id: LocationId,
    root: PathBuf,
    backend: Arc<dyn StorageBackend>,
}

impl CloudLocation {
    pub fn new(id: LocationId, root: PathBuf, backend: Arc<dyn StorageBackend>) -> Self {
        Self { id, root, backend }
    }
}

#[async_trait]
impl Location for CloudLocation {
    fn id(&self) -> &LocationId {
        &self.id
    }

    fn kind(&self) -> LocationKind {
        LocationKind::Cloud
    }

    fn file_root(&self) -> &Path {
        &self.root
    }

    fn scanner(&self) -> Arc<dyn LocationScanner> {
        Arc::new(CloudScanner::new(
            self.id.clone(),
            self.root.clone(),
            self.backend.clone(),
        ))
    }

    async fn ensure(&self) -> Result<(), InfraError> {
        self.backend.ensure().await.map_err(|e| {
            InfraError::Init(format!("cloud location '{}' ensure failed: {e}", self.id))
        })
    }
}
