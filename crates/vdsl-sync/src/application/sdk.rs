//! SyncStore SDK — vdsl-syncの外部向けAPI面。
//!
//! MCP・Lua Bridge等のインターフェース層はこのtraitのみに依存する。
//! scan→delta→plan→execute等の内部パイプラインは一切漏れない。
//!
//! # 設計方針
//!
//! - **UseCase完結**: `sync()`一発で全工程が内部完結
//! - **CRUD + Query**: Firestore的なput/get/list/delete + status/errors/pending
//! - **進捗**: `status()` でDB SELECTベースのサマリーを取得（Observer不要）
//! - **型安全**: 戻り型は全てSDK専用型。旧Store/SyncFacadeの型に依存しない

use std::path::Path;

use async_trait::async_trait;

use super::topology_store::TopologyFileView;
use crate::application::error::SyncError;
use crate::domain::file_type::FileType;
use crate::domain::fingerprint::FileFingerprint;
use crate::domain::location::{LocationId, SyncSummary};
use crate::domain::view::{ErrorEntry, PendingEntry};

// =============================================================================
// SDK Result types
// =============================================================================

/// sync/sync_route/force_rewrite の結果。
///
/// 旧BatchResult/SyncResult/FacadeSyncResultを統合した単一型。
/// Serialize対応でMCP層がそのまま返せる。
#[derive(Debug, Clone, Default, serde::Serialize)]
pub struct SyncReport {
    /// スキャンで検出されたファイル数。
    pub scanned: usize,
    /// スキャン時の非致命的エラー（個別ファイル読み取り失敗等）。
    pub scan_errors: Vec<SyncReportError>,
    /// 計画で作成されたTransfer数。
    pub transfers_created: usize,
    /// 実行で成功した転送数。
    pub transferred: usize,
    /// 実行で失敗した転送数。
    pub failed: usize,
    /// 転送失敗詳細。
    pub errors: Vec<SyncReportError>,
    /// 検出されたコンフリクト。
    ///
    /// 複数Locationで同一ファイルが異なる内容に更新された場合に報告される。
    /// コンフリクトのあるファイルのUpdate転送は実行されない。
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub conflicts: Vec<SyncReportConflict>,
}

/// コンフリクト報告。SDK面の型。
///
/// domain::topology_delta::ConflictEntry から変換して使用する。
#[derive(Debug, Clone, serde::Serialize)]
pub struct SyncReportConflict {
    pub file_id: String,
    pub path: String,
    /// コンフリクトしているLocation群。
    pub locations: Vec<String>,
}

/// 転送失敗の詳細。
#[derive(Debug, Clone, serde::Serialize)]
pub struct SyncReportError {
    pub path: String,
    pub error: String,
}

/// put() の結果。
///
/// TopologyPutResultをre-exportせず、SDK面として安定させる。
#[derive(Debug, serde::Serialize)]
pub struct PutReport {
    /// 登録/更新されたファイルID。
    pub file_id: String,
    /// 新規登録 = true、更新 = false。
    pub is_new: bool,
    /// 作成されたTransfer数。
    pub transfers_created: usize,
}

// =============================================================================
// SDK Trait
// =============================================================================

/// vdsl-syncの外部向けAPI。
///
/// インターフェース層（MCP, Lua Bridge, CLI）はこのtraitのみに依存する。
/// 内部実装（TopologyStore, TransferEngine, scan, delta等）は一切公開しない。
#[async_trait]
pub trait SyncStoreSdk: Send + Sync {
    // =========================================================================
    // UseCase — 同期操作
    // =========================================================================

    /// 全体同期: scan→delta→plan→execute 一括。
    async fn sync(&self) -> Result<SyncReport, SyncError>;

    /// 単一ルート同期。
    async fn sync_route(
        &self,
        src: &LocationId,
        dest: &LocationId,
    ) -> Result<SyncReport, SyncError>;

    /// 全件再転送（メンテナンス操作）。
    ///
    /// ターゲットを空にして通常syncで代替可能。コンフリクト検出機構の導入後に除去予定。
    #[deprecated(note = "use sync after clearing target — force_rewrite semantics are incomplete")]
    async fn force_rewrite(&self) -> Result<SyncReport, SyncError>;

    // =========================================================================
    // Command — ファイル操作
    // =========================================================================

    /// ファイル登録。
    async fn put(
        &self,
        path: &str,
        file_type: FileType,
        fingerprint: FileFingerprint,
        origin: &LocationId,
        embedded_id: Option<String>,
    ) -> Result<PutReport, SyncError>;

    /// ファイル削除。削除されたTransfer数を返す。
    async fn delete(&self, path: &str) -> Result<usize, SyncError>;

    // =========================================================================
    // Query — 読み取り
    // =========================================================================

    /// ファイル取得。
    async fn get(&self, path: &str) -> Result<Option<TopologyFileView>, SyncError>;

    /// ファイル一覧。
    async fn list(
        &self,
        file_type: Option<FileType>,
        limit: Option<usize>,
    ) -> Result<Vec<TopologyFileView>, SyncError>;

    /// ロケーション別同期サマリー。
    async fn status(&self) -> Result<SyncSummary, SyncError>;

    /// エラー一覧。
    async fn errors(&self) -> Result<Vec<ErrorEntry>, SyncError>;

    /// 転送待ち一覧。
    async fn pending(&self, dest: &LocationId) -> Result<Vec<PendingEntry>, SyncError>;

    // =========================================================================
    // Topology — 読み取り専用
    // =========================================================================

    /// 登録済みロケーション一覧。
    fn locations(&self) -> Vec<LocationId>;

    /// 全エッジ `(src, dest)` 一覧。
    fn all_edges(&self) -> Vec<(LocationId, LocationId)>;

    /// ローカルファイルルート。
    fn local_root(&self) -> Option<&Path>;
}
