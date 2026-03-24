//! TransferStore — Transfer永続化トレイト。
//!
//! 配送記録の永続化を抽象化する。
//! Transfer CRUD + クエリ + 集計 + pruning。

use async_trait::async_trait;
use chrono::{DateTime, Utc};

use crate::domain::location::LocationId;
use crate::domain::transfer::{Transfer, TransferState};
use crate::infra::error::InfraError;

/// DB集約クエリの1行。dest×state別のファイル数カウント。
#[derive(Debug, Clone)]
pub struct TransferStatRow {
    pub src: LocationId,
    pub dest: LocationId,
    pub state: TransferState,
    pub error_kind: Option<String>,
    pub attempt: u32,
    pub file_count: usize,
}

/// Transfer永続化。
///
/// 実装: [`super::sqlite::SqliteSyncStore`] (feature = "sqlite")
#[async_trait]
pub trait TransferStore: Send + Sync {
    /// Transferを保存。
    async fn insert_transfer(&self, transfer: &Transfer) -> Result<(), InfraError>;

    /// Transfer状態を更新（start/complete/fail後に呼ぶ）。
    async fn update_transfer(&self, transfer: &Transfer) -> Result<(), InfraError>;

    /// 特定destのQueued Transfer一覧。
    ///
    /// 各file_idの最新Transferのみ返す（古いリトライは除外）。
    async fn queued_transfers(&self, dest: &LocationId) -> Result<Vec<Transfer>, InfraError>;

    /// 特定ファイルの最新Transfer（dest別）。
    ///
    /// 各destについて最新のTransferのみ返す。
    /// FileView構築時にPresenceView導出に使用。
    async fn latest_transfers_by_file(&self, file_id: &str) -> Result<Vec<Transfer>, InfraError>;

    /// Failed Transfer一覧（最新のみ）。
    async fn failed_transfers(&self) -> Result<Vec<Transfer>, InfraError>;

    /// 完了済み古いTransferを削除。削除件数を返す。
    ///
    /// 履歴肥大化防止用。各file_id×destの最新は保持される。
    async fn prune_completed(&self, before: DateTime<Utc>) -> Result<usize, InfraError>;

    /// Queued状態のTransfer件数を返す。進捗表示用。
    async fn count_queued(&self) -> Result<usize, InfraError>;

    /// InFlight状態のTransferをCancelledに遷移。
    ///
    /// プロセス再起動時にInFlightのまま残った孤児transferを終端状態にする。
    /// Cancelledは終端状態 — 再実行されない。必要であれば新しいTransferを作成する。
    /// 返り値はキャンセルした件数。
    async fn cancel_orphaned_inflight(&self) -> Result<usize, InfraError>;

    /// 指定Transfer IDに依存するBlocked TransferをQueuedに遷移させる。
    ///
    /// Transfer完了時に呼び出し、依存チェーンの次ホップを実行可能にする。
    /// 返り値はunblockした件数。
    async fn unblock_dependents(&self, completed_transfer_id: &str) -> Result<usize, InfraError>;

    /// 全destのQueued/Blocked Transfer一覧（最新のみ）。
    ///
    /// `status()` のpending_entries構築用。dest指定なしで全pendingを返す。
    async fn all_pending_transfers(&self) -> Result<Vec<Transfer>, InfraError>;

    /// Transfer状態の集約統計。
    ///
    /// 最新Transfer（file_id×dest別）をsrc, dest, state, error_kind, attemptでGROUP BYし、
    /// ファイル数カウントを返す。`status()`のN+1クエリ問題を解消するための集約メソッド。
    async fn transfer_stats(&self) -> Result<Vec<TransferStatRow>, InfraError>;

    /// Location別のPresent file数。
    ///
    /// 各locationについて、srcとして送出した or completedとして到達したdistinct file数を返す。
    /// UNIONでsrc/completed-destを統合し、同一location×file_idの重複を排除する。
    async fn present_counts_by_location(
        &self,
    ) -> Result<std::collections::HashMap<LocationId, usize>, InfraError>;
}
