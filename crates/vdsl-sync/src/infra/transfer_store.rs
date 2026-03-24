//! TransferStore — Transfer永続化トレイト。
//!
//! 配送記録の永続化を抽象化する。
//! Transfer CRUD + クエリ + 集計 + pruning。

use async_trait::async_trait;
use chrono::{DateTime, Utc};

use crate::domain::error::SyncError;
use crate::domain::location::LocationId;
use crate::domain::transfer::Transfer;

/// Transfer永続化。
///
/// 実装: [`super::sqlite::SqliteSyncStore`] (feature = "sqlite")
#[async_trait]
pub trait TransferStore: Send + Sync {
    /// Transferを保存。
    async fn insert_transfer(&self, transfer: &Transfer) -> Result<(), SyncError>;

    /// Transfer状態を更新（start/complete/fail後に呼ぶ）。
    async fn update_transfer(&self, transfer: &Transfer) -> Result<(), SyncError>;

    /// 特定destのQueued Transfer一覧。
    ///
    /// 各file_idの最新Transferのみ返す（古いリトライは除外）。
    async fn queued_transfers(&self, dest: &LocationId) -> Result<Vec<Transfer>, SyncError>;

    /// 特定ファイルの最新Transfer（dest別）。
    ///
    /// 各destについて最新のTransferのみ返す。
    /// FileView構築時にPresenceView導出に使用。
    async fn latest_transfers_by_file(&self, file_id: &str) -> Result<Vec<Transfer>, SyncError>;

    /// Failed Transfer一覧（最新のみ）。
    async fn failed_transfers(&self) -> Result<Vec<Transfer>, SyncError>;

    /// 完了済み古いTransferを削除。削除件数を返す。
    ///
    /// 履歴肥大化防止用。各file_id×destの最新は保持される。
    async fn prune_completed(&self, before: DateTime<Utc>) -> Result<usize, SyncError>;
}
