//! TopologyFileStore — TopologyFile永続化トレイト。
//!
//! TopologyFile（ファイル身元 / inode）のCRUD + ハッシュ検索。

use async_trait::async_trait;

use crate::application::error::SyncError;
use crate::domain::file_type::FileType;
use crate::domain::topology_file::TopologyFile;

/// TopologyFile永続化。
///
/// Topology上のファイル身元情報（canonical_hash, relative_path, file_type等）を管理する。
/// LocationFile（各Locationでの実体情報）は [`LocationFileStore`] が管理する。
#[async_trait]
pub trait TopologyFileStore: Send + Sync {
    /// TopologyFileを保存（新規 or 更新）。
    ///
    /// idが既存の場合はUPDATE、なければINSERT。
    async fn upsert(&self, file: &TopologyFile) -> Result<(), SyncError>;

    /// IDでTopologyFileを取得。
    async fn get_by_id(&self, id: &str) -> Result<Option<TopologyFile>, SyncError>;

    /// relative_pathでTopologyFileを取得（deleted除外）。
    async fn get_by_path(&self, relative_path: &str) -> Result<Option<TopologyFile>, SyncError>;

    /// canonical_hashでTopologyFileを検索（deleted除外）。
    ///
    /// rename検出で使用: スキャン結果のcontent_hashがTopologyFileのcanonical_hashに一致
    /// すれば同一Entity。
    async fn find_by_canonical_hash(&self, hash: &str) -> Result<Option<TopologyFile>, SyncError>;

    /// 生存中（deleted_at IS NULL）のTopologyFile一覧。
    async fn list_active(
        &self,
        file_type: Option<FileType>,
        limit: Option<usize>,
    ) -> Result<Vec<TopologyFile>, SyncError>;

    /// 削除済み（deleted_at IS NOT NULL）のTopologyFile一覧。
    ///
    /// distribute_delete_actions()で使用: 削除済みファイルのLocationFileを掃除する。
    async fn list_deleted(&self) -> Result<Vec<TopologyFile>, SyncError>;

    /// 生存中ファイル数。
    async fn count_active(&self) -> Result<usize, SyncError>;

    /// 全TopologyFileのrelative_path一覧（deleted除外）。
    ///
    /// スキャン結果との差分でVanished検出に使用。
    async fn list_active_paths(&self) -> Result<Vec<String>, SyncError>;
}
