//! LocationFileStore — LocationFile永続化トレイト。
//!
//! LocationFile（各Locationでのファイル実体情報）のCRUD。
//! TopologyFile（身元 / inode）は [`TopologyFileStore`] が管理する。

use async_trait::async_trait;

use crate::application::error::SyncError;
use crate::domain::location::LocationId;
use crate::domain::location_file::LocationFile;

/// LocationFile永続化。
///
/// `(file_id, location_id)` が一意キー。
/// file_idはTopologyFile.idに対応する。
#[async_trait]
pub trait LocationFileStore: Send + Sync {
    /// LocationFileを保存（新規 or 更新）。
    ///
    /// `(file_id, location_id)` が既存ならUPDATE、なければINSERT。
    async fn upsert(&self, file: &LocationFile) -> Result<(), SyncError>;

    /// `(file_id, location_id)` でLocationFileを取得。
    async fn get(
        &self,
        file_id: &str,
        location_id: &LocationId,
    ) -> Result<Option<LocationFile>, SyncError>;

    /// あるファイルの全Location分のLocationFileを取得。
    ///
    /// distribute_actions()の入力用: file_id → Vec<LocationFile>。
    async fn list_by_file(&self, file_id: &str) -> Result<Vec<LocationFile>, SyncError>;

    /// あるLocationの全LocationFileを取得。
    ///
    /// スキャン時の比較用: location_id → Vec<LocationFile>。
    async fn list_by_location(
        &self,
        location_id: &LocationId,
    ) -> Result<Vec<LocationFile>, SyncError>;

    /// 複数ファイルの全LocationFileを一括取得。
    ///
    /// file_id → Vec<LocationFile> のマップを返す。
    /// distribute_actions()のバッチ入力用。N+1回避。
    async fn list_by_files(
        &self,
        file_ids: &[&str],
    ) -> Result<std::collections::HashMap<String, Vec<LocationFile>>, SyncError>;

    /// LocationFileを削除。削除した場合true。
    async fn delete(&self, file_id: &str, location_id: &LocationId) -> Result<bool, SyncError>;

    /// あるLocationのLocationFile数。
    async fn count_by_location(&self, location_id: &LocationId) -> Result<usize, SyncError>;
}
