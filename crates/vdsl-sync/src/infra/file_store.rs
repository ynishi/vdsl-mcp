//! FileStore — TrackedFile永続化トレイト。
//!
//! ファイル身元情報の永続化を抽象化する。
//! TrackedFileのCRUD + 重複検出。

use async_trait::async_trait;

use crate::domain::error::SyncError;
use crate::domain::file_type::FileType;
use crate::domain::tracked_file::TrackedFile;

/// TrackedFile永続化。
///
/// 実装: [`super::sqlite::SqliteSyncStore`] (feature = "sqlite")
#[async_trait]
pub trait FileStore: Send + Sync {
    /// TrackedFileを保存（新規 or 更新）。
    ///
    /// relative_pathが既存の場合はUPDATE、なければINSERT。
    async fn upsert_file(&self, file: &TrackedFile) -> Result<(), SyncError>;

    /// IDでTrackedFileを取得。
    ///
    /// Transfer実行時にfile_idからrelative_pathを引くために使用。
    async fn get_file_by_id(&self, id: &str) -> Result<Option<TrackedFile>, SyncError>;

    /// relative_pathでTrackedFileを取得。
    async fn get_file_by_path(&self, relative_path: &str)
        -> Result<Option<TrackedFile>, SyncError>;

    /// 重複検出。content_hash優先 → file_hashフォールバック。
    ///
    /// exclude_pathは自分自身を除外するため。
    async fn find_duplicate_file(
        &self,
        file_hash: &str,
        content_hash: Option<&str>,
        exclude_path: &str,
    ) -> Result<Option<TrackedFile>, SyncError>;

    /// TrackedFile一覧。file_type/limitでフィルタ可能。
    async fn list_files(
        &self,
        file_type: Option<FileType>,
        limit: Option<usize>,
    ) -> Result<Vec<TrackedFile>, SyncError>;

    /// relative_pathで削除。削除した場合true。
    async fn delete_file(&self, relative_path: &str) -> Result<bool, SyncError>;
}
