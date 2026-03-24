//! RemoteStore — リモート設定永続化トレイト。
//!
//! 旧SyncStoreのリモート部分を分離したもの。

use async_trait::async_trait;

use crate::domain::error::SyncError;
use crate::domain::location::LocationId;
use crate::infra::store::RemoteConfig;

/// リモートエンドポイント設定の永続化。
///
/// 実装: [`super::sqlite::SqliteSyncStore`] (feature = "sqlite")
#[async_trait]
pub trait RemoteStore: Send + Sync {
    /// リモートを登録（UPSERT）。
    async fn register_remote(&self, remote: &RemoteConfig) -> Result<(), SyncError>;

    /// location_idでリモートを取得。
    async fn get_remote(&self, location_id: &LocationId)
        -> Result<Option<RemoteConfig>, SyncError>;

    /// 全リモート一覧。
    async fn list_remotes(&self) -> Result<Vec<RemoteConfig>, SyncError>;

    /// リモートを削除。存在していた場合true。
    async fn remove_remote(&self, location_id: &LocationId) -> Result<bool, SyncError>;
}
