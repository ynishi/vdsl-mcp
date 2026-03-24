//! SQLite implementation of file/transfer/remote stores.
//!
//! Uses normalized schema: `tracked_files` + `transfers` + `sync_remotes`.
//! Designed for single-writer (sync engine), concurrent readers OK.
//!
//! Uses `tokio-rusqlite` for non-blocking async access — each connection
//! runs on a dedicated background thread with mpsc channel dispatch.

mod mapping;
mod schema;

use std::path::Path;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use rusqlite::params;

use crate::domain::error::SyncError;
use crate::domain::file_type::FileType;
use crate::domain::location::LocationId;
use crate::domain::tracked_file::TrackedFile;
use crate::domain::transfer::Transfer;
use crate::infra::file_store::FileStore;
use crate::infra::remote_store::RemoteStore;
use crate::infra::store::RemoteConfig;
use crate::infra::transfer_store::TransferStore;

use mapping::{
    query_tracked_files, query_transfers, row_to_remote_tuple, ts_to_string, tuple_to_remote_config,
};

/// SQLite-backed sync store.
///
/// Uses `tokio_rusqlite::Connection` — a handle that dispatches closures
/// to a dedicated background thread via mpsc channel. Does not block
/// the async runtime.
pub struct SqliteSyncStore {
    conn: tokio_rusqlite::Connection,
}

impl SqliteSyncStore {
    /// Open (or create) a sync database at the given path.
    pub async fn open(path: &Path) -> Result<Self, SyncError> {
        let path = path.to_path_buf();
        let conn = tokio_rusqlite::Connection::open(&path)
            .await
            .map_err(|e| SyncError::Store(format!("open failed: {e}")))?;
        conn.call(schema::init_connection)
            .await
            .map_err(map_call_err)?;
        Ok(Self { conn })
    }

    /// Open an in-memory database (for testing).
    pub async fn open_in_memory() -> Result<Self, SyncError> {
        let conn = tokio_rusqlite::Connection::open_in_memory()
            .await
            .map_err(|e| SyncError::Store(format!("open_in_memory failed: {e}")))?;
        conn.call(schema::init_connection)
            .await
            .map_err(map_call_err)?;
        Ok(Self { conn })
    }
}

// =============================================================================
// Error mapping
// =============================================================================

/// Convert `tokio_rusqlite::Error<SyncError>` → `SyncError`.
fn map_call_err(e: tokio_rusqlite::Error<SyncError>) -> SyncError {
    match e {
        tokio_rusqlite::Error::Error(sync_err) => sync_err,
        tokio_rusqlite::Error::ConnectionClosed => {
            SyncError::Store("sqlite connection closed".into())
        }
        tokio_rusqlite::Error::Close((_, e)) => {
            SyncError::Store(format!("sqlite close error: {e}"))
        }
        other => SyncError::Store(format!("tokio-rusqlite: {other:?}")),
    }
}

// =============================================================================
// FileStore trait implementation
// =============================================================================

#[async_trait]
impl FileStore for SqliteSyncStore {
    async fn upsert_file(&self, file: &TrackedFile) -> Result<(), SyncError> {
        let file = file.clone();
        self.conn
            .call(move |conn| {
                let file_size_i64 = i64::try_from(file.file_size()).map_err(|_| {
                    SyncError::Store(format!(
                        "file_size exceeds i64::MAX: {} (file {})",
                        file.file_size(),
                        file.id()
                    ))
                })?;
                let registered_at_str = ts_to_string(file.registered_at());
                let updated_at_str = ts_to_string(file.updated_at());
                conn.execute(
                    "INSERT INTO tracked_files (id, relative_path, file_type, file_hash, content_hash, file_size, embedded_id, registered_at, updated_at)
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)
                     ON CONFLICT (relative_path) DO UPDATE SET
                         file_type = excluded.file_type,
                         file_hash = excluded.file_hash,
                         content_hash = excluded.content_hash,
                         file_size = excluded.file_size,
                         embedded_id = excluded.embedded_id,
                         updated_at = excluded.updated_at",
                    params![
                        file.id(),
                        file.relative_path(),
                        file.file_type().as_str(),
                        file.file_hash(),
                        file.content_hash(),
                        file_size_i64,
                        file.embedded_id(),
                        registered_at_str,
                        updated_at_str,
                    ],
                )
                .map_err(|e| SyncError::Store(format!("upsert_file failed: {e}")))?;
                Ok(())
            })
            .await
            .map_err(map_call_err)
    }

    async fn get_file_by_path(
        &self,
        relative_path: &str,
    ) -> Result<Option<TrackedFile>, SyncError> {
        let path = relative_path.to_string();
        self.conn
            .call(move |conn| {
                let files = query_tracked_files(
                    conn,
                    "SELECT * FROM tracked_files WHERE relative_path = ?",
                    &[&path as &dyn rusqlite::types::ToSql],
                )?;
                Ok(files.into_iter().next())
            })
            .await
            .map_err(map_call_err)
    }

    async fn get_file_by_id(&self, id: &str) -> Result<Option<TrackedFile>, SyncError> {
        let id = id.to_string();
        self.conn
            .call(move |conn| {
                let files = query_tracked_files(
                    conn,
                    "SELECT * FROM tracked_files WHERE id = ?",
                    &[&id as &dyn rusqlite::types::ToSql],
                )?;
                Ok(files.into_iter().next())
            })
            .await
            .map_err(map_call_err)
    }

    async fn find_duplicate_file(
        &self,
        file_hash: &str,
        content_hash: Option<&str>,
        exclude_path: &str,
    ) -> Result<Option<TrackedFile>, SyncError> {
        let file_hash = file_hash.to_string();
        let content_hash = content_hash.map(|s| s.to_string());
        let exclude_path = exclude_path.to_string();
        self.conn
            .call(move |conn| {
                if let Some(ref ch) = content_hash {
                    let files = query_tracked_files(
                        conn,
                        "SELECT * FROM tracked_files WHERE content_hash = ? AND relative_path != ?",
                        &[
                            ch as &dyn rusqlite::types::ToSql,
                            &exclude_path as &dyn rusqlite::types::ToSql,
                        ],
                    )?;
                    if let Some(f) = files.into_iter().next() {
                        return Ok(Some(f));
                    }
                }
                let files = query_tracked_files(
                    conn,
                    "SELECT * FROM tracked_files WHERE file_hash = ? AND relative_path != ?",
                    &[
                        &file_hash as &dyn rusqlite::types::ToSql,
                        &exclude_path as &dyn rusqlite::types::ToSql,
                    ],
                )?;
                Ok(files.into_iter().next())
            })
            .await
            .map_err(map_call_err)
    }

    async fn delete_file(&self, relative_path: &str) -> Result<bool, SyncError> {
        let path = relative_path.to_string();
        self.conn
            .call(move |conn| {
                let changes = conn
                    .execute(
                        "DELETE FROM tracked_files WHERE relative_path = ?",
                        params![path],
                    )
                    .map_err(|e| SyncError::Store(format!("{e}")))?;
                Ok(changes > 0)
            })
            .await
            .map_err(map_call_err)
    }

    async fn list_files(
        &self,
        file_type: Option<FileType>,
        limit: Option<usize>,
    ) -> Result<Vec<TrackedFile>, SyncError> {
        self.conn
            .call(move |conn| {
                let mut sql = String::from("SELECT * FROM tracked_files");
                let mut param_values: Vec<Box<dyn rusqlite::types::ToSql>> = Vec::new();

                if let Some(ft) = file_type {
                    sql.push_str(" WHERE file_type = ?");
                    param_values.push(Box::new(ft.as_str().to_string()));
                }
                sql.push_str(" ORDER BY updated_at DESC");
                if let Some(n) = limit {
                    sql.push_str(" LIMIT ?");
                    let n_i64 = i64::try_from(n)
                        .map_err(|_| SyncError::Store(format!("limit exceeds i64::MAX: {n}")))?;
                    param_values.push(Box::new(n_i64));
                }

                let refs: Vec<&dyn rusqlite::types::ToSql> =
                    param_values.iter().map(|p| p.as_ref()).collect();
                query_tracked_files(conn, &sql, &refs)
            })
            .await
            .map_err(map_call_err)
    }
}

// =============================================================================
// TransferStore trait implementation
// =============================================================================

#[async_trait]
impl TransferStore for SqliteSyncStore {
    async fn insert_transfer(&self, transfer: &Transfer) -> Result<(), SyncError> {
        let t = transfer.clone();
        self.conn
            .call(move |conn| {
                let attempt_i64 = i64::from(t.attempt());
                let created_at_str = ts_to_string(t.created_at());
                let started_at_str = t.started_at().map(ts_to_string);
                let finished_at_str = t.finished_at().map(ts_to_string);
                let error_kind_str = t.error_kind().map(|k| k.to_string());
                conn.execute(
                    "INSERT INTO transfers (id, file_id, src, dest, state, error, error_kind, attempt, created_at, started_at, finished_at)
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)",
                    params![
                        t.id(),
                        t.file_id(),
                        t.src().as_str(),
                        t.dest().as_str(),
                        t.state().as_str(),
                        t.error(),
                        error_kind_str,
                        attempt_i64,
                        created_at_str,
                        started_at_str,
                        finished_at_str,
                    ],
                )
                .map_err(|e| SyncError::Store(format!("insert_transfer failed: {e}")))?;
                Ok(())
            })
            .await
            .map_err(map_call_err)
    }

    async fn update_transfer(&self, transfer: &Transfer) -> Result<(), SyncError> {
        let t = transfer.clone();
        self.conn
            .call(move |conn| {
                let started_at_str = t.started_at().map(ts_to_string);
                let finished_at_str = t.finished_at().map(ts_to_string);
                let error_kind_str = t.error_kind().map(|k| k.to_string());
                conn.execute(
                    "UPDATE transfers SET state = ?, error = ?, error_kind = ?, started_at = ?, finished_at = ?, attempt = ?
                     WHERE id = ?",
                    params![
                        t.state().as_str(),
                        t.error(),
                        error_kind_str,
                        started_at_str,
                        finished_at_str,
                        i64::from(t.attempt()),
                        t.id(),
                    ],
                )
                .map_err(|e| SyncError::Store(format!("update_transfer failed: {e}")))?;
                Ok(())
            })
            .await
            .map_err(map_call_err)
    }

    async fn queued_transfers(&self, dest: &LocationId) -> Result<Vec<Transfer>, SyncError> {
        let dest_str = dest.as_str().to_string();
        self.conn
            .call(move |conn| {
                query_transfers(
                    conn,
                    "SELECT t.* FROM transfers t
                     WHERE t.dest = ? AND t.state = 'queued'
                       AND NOT EXISTS (
                           SELECT 1 FROM transfers t2
                           WHERE t2.file_id = t.file_id
                             AND t2.dest = t.dest
                             AND t2.ROWID > t.ROWID
                       )
                     ORDER BY t.created_at",
                    &[&dest_str as &dyn rusqlite::types::ToSql],
                )
            })
            .await
            .map_err(map_call_err)
    }

    async fn latest_transfers_by_file(&self, file_id: &str) -> Result<Vec<Transfer>, SyncError> {
        let file_id = file_id.to_string();
        self.conn
            .call(move |conn| {
                query_transfers(
                    conn,
                    "SELECT t.* FROM transfers t
                     WHERE t.file_id = ?
                       AND NOT EXISTS (
                           SELECT 1 FROM transfers t2
                           WHERE t2.file_id = t.file_id
                             AND t2.dest = t.dest
                             AND t2.ROWID > t.ROWID
                       )",
                    &[&file_id as &dyn rusqlite::types::ToSql],
                )
            })
            .await
            .map_err(map_call_err)
    }

    async fn failed_transfers(&self) -> Result<Vec<Transfer>, SyncError> {
        self.conn
            .call(|conn| {
                query_transfers(
                    conn,
                    "SELECT * FROM transfers WHERE state = 'failed' ORDER BY finished_at DESC",
                    &[],
                )
            })
            .await
            .map_err(map_call_err)
    }

    async fn prune_completed(&self, before: DateTime<Utc>) -> Result<usize, SyncError> {
        let before_str = ts_to_string(before);
        self.conn
            .call(move |conn| {
                // 各 file_id × dest の最新Transferは保持し、それより古い completed を削除
                let deleted = conn
                    .execute(
                        "DELETE FROM transfers
                         WHERE state = 'completed'
                           AND finished_at < ?1
                           AND id NOT IN (
                               SELECT t.id FROM transfers t
                               INNER JOIN (
                                   SELECT file_id, dest, MAX(created_at) as max_created
                                   FROM transfers
                                   GROUP BY file_id, dest
                               ) latest ON t.file_id = latest.file_id
                                           AND t.dest = latest.dest
                                           AND t.created_at = latest.max_created
                           )",
                        params![before_str],
                    )
                    .map_err(|e| SyncError::Store(format!("prune_completed failed: {e}")))?;
                Ok(deleted)
            })
            .await
            .map_err(map_call_err)
    }
}

// =============================================================================
// RemoteStore trait implementation
// =============================================================================

#[async_trait]
impl RemoteStore for SqliteSyncStore {
    async fn register_remote(&self, remote: &RemoteConfig) -> Result<(), SyncError> {
        let remote = remote.clone();
        self.conn
            .call(move |conn| {
                let config_json = serde_json::to_string(&remote.config)
                    .map_err(|e| SyncError::Store(format!("config serialize: {e}")))?;
                let created_at_str = ts_to_string(remote.created_at);
                conn.execute(
                    "INSERT INTO sync_remotes (location_id, backend, config, created_at)
                     VALUES (?, ?, ?, ?)
                     ON CONFLICT (location_id) DO UPDATE SET backend = excluded.backend, config = excluded.config",
                    params![
                        remote.location_id.as_str(),
                        remote.backend,
                        config_json,
                        created_at_str,
                    ],
                )
                .map_err(|e| SyncError::Store(format!("{e}")))?;
                Ok(())
            })
            .await
            .map_err(map_call_err)
    }

    async fn get_remote(
        &self,
        location_id: &LocationId,
    ) -> Result<Option<RemoteConfig>, SyncError> {
        let loc_str = location_id.as_str().to_string();
        self.conn
            .call(move |conn| {
                let mut stmt = conn
                    .prepare(
                        "SELECT location_id, backend, config, created_at
                         FROM sync_remotes WHERE location_id = ?",
                    )
                    .map_err(|e| SyncError::Store(format!("{e}")))?;
                let mut rows = stmt
                    .query_map(params![loc_str], row_to_remote_tuple)
                    .map_err(|e| SyncError::Store(format!("{e}")))?;
                match rows.next() {
                    Some(row) => {
                        let (loc, backend, config_str, created_at_str) =
                            row.map_err(|e| SyncError::Store(format!("{e}")))?;
                        Ok(Some(tuple_to_remote_config(
                            loc,
                            backend,
                            config_str,
                            created_at_str,
                        )?))
                    }
                    None => Ok(None),
                }
            })
            .await
            .map_err(map_call_err)
    }

    async fn list_remotes(&self) -> Result<Vec<RemoteConfig>, SyncError> {
        self.conn
            .call(|conn| {
                let mut stmt = conn
                    .prepare("SELECT location_id, backend, config, created_at FROM sync_remotes ORDER BY location_id")
                    .map_err(|e| SyncError::Store(format!("{e}")))?;
                let rows = stmt
                    .query_map([], row_to_remote_tuple)
                    .map_err(|e| SyncError::Store(format!("{e}")))?;

                let mut remotes = Vec::new();
                for row in rows {
                    let (loc, backend, config_str, created_at_str) =
                        row.map_err(|e| SyncError::Store(format!("{e}")))?;
                    remotes.push(tuple_to_remote_config(
                        loc,
                        backend,
                        config_str,
                        created_at_str,
                    )?);
                }
                Ok(remotes)
            })
            .await
            .map_err(map_call_err)
    }

    async fn remove_remote(&self, location_id: &LocationId) -> Result<bool, SyncError> {
        let loc_str = location_id.as_str().to_string();
        self.conn
            .call(move |conn| {
                let changes = conn
                    .execute(
                        "DELETE FROM sync_remotes WHERE location_id = ?",
                        params![loc_str],
                    )
                    .map_err(|e| SyncError::Store(format!("{e}")))?;
                Ok(changes > 0)
            })
            .await
            .map_err(map_call_err)
    }
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    use crate::domain::tracked_file::TrackedFile;
    use crate::domain::transfer::Transfer;
    use crate::infra::file_store::FileStore;
    use crate::infra::transfer_store::TransferStore;

    fn loc(s: &str) -> LocationId {
        LocationId::new(s).expect("valid test location")
    }

    fn sample_tracked_file(path: &str) -> TrackedFile {
        TrackedFile::from_scan(
            path.into(),
            FileType::Image,
            format!("fh_{}", path.replace('/', "_")),
            None,
            1024,
            None,
        )
        .expect("valid test data")
    }

    // =========================================================================
    // FileStore tests
    // =========================================================================

    #[tokio::test]
    async fn upsert_and_get_file() {
        let store = SqliteSyncStore::open_in_memory().await.expect("open");
        let file = sample_tracked_file("output/test.png");

        store.upsert_file(&file).await.expect("upsert");
        let got = store
            .get_file_by_path("output/test.png")
            .await
            .expect("get")
            .expect("found");

        assert_eq!(got.relative_path(), "output/test.png");
        assert_eq!(got.file_type(), FileType::Image);
        assert_eq!(got.file_size(), 1024);
    }

    #[tokio::test]
    async fn upsert_updates_existing() {
        let store = SqliteSyncStore::open_in_memory().await.expect("open");
        let mut file = sample_tracked_file("output/up.png");
        store.upsert_file(&file).await.expect("insert");

        file.update_from_scan(
            FileType::Image,
            "new_hash".into(),
            Some("ch".into()),
            2048,
            None,
        );
        store.upsert_file(&file).await.expect("upsert update");

        let got = store
            .get_file_by_path("output/up.png")
            .await
            .expect("get")
            .expect("found");
        assert_eq!(got.file_hash(), "new_hash");
        assert_eq!(got.content_hash(), Some("ch"));
        assert_eq!(got.file_size(), 2048);
    }

    #[tokio::test]
    async fn get_nonexistent() {
        let store = SqliteSyncStore::open_in_memory().await.expect("open");
        let got = store.get_file_by_path("no/such/file").await.expect("get");
        assert!(got.is_none());
    }

    #[tokio::test]
    async fn delete_file() {
        let store = SqliteSyncStore::open_in_memory().await.expect("open");
        let file = sample_tracked_file("output/del.png");
        store.upsert_file(&file).await.expect("insert");

        assert!(store.delete_file("output/del.png").await.expect("delete"));
        assert!(!store.delete_file("output/del.png").await.expect("delete2"));
        assert!(store
            .get_file_by_path("output/del.png")
            .await
            .expect("get")
            .is_none());
    }

    #[tokio::test]
    async fn find_duplicate_by_file_hash() {
        let store = SqliteSyncStore::open_in_memory().await.expect("open");
        let file = TrackedFile::from_scan(
            "output/a.png".into(),
            FileType::Image,
            "deadbeef".into(),
            None,
            512,
            None,
        )
        .expect("valid test data");
        store.upsert_file(&file).await.expect("insert");

        let dup = store
            .find_duplicate_file("deadbeef", None, "output/b.png")
            .await
            .expect("find");
        assert_eq!(dup.expect("dup").relative_path(), "output/a.png");

        let no_dup = store
            .find_duplicate_file("deadbeef", None, "output/a.png")
            .await
            .expect("find");
        assert!(no_dup.is_none());
    }

    #[tokio::test]
    async fn find_duplicate_content_hash_priority() {
        let store = SqliteSyncStore::open_in_memory().await.expect("open");
        let file = TrackedFile::from_scan(
            "output/a.png".into(),
            FileType::Image,
            "file_aaa".into(),
            Some("content_xxx".into()),
            512,
            None,
        )
        .expect("valid test data");
        store.upsert_file(&file).await.expect("insert");

        let dup = store
            .find_duplicate_file("file_bbb", Some("content_xxx"), "output/b.png")
            .await
            .expect("find");
        assert_eq!(dup.expect("dup").relative_path(), "output/a.png");
    }

    #[tokio::test]
    async fn list_files_with_filter() {
        let store = SqliteSyncStore::open_in_memory().await.expect("open");
        store
            .upsert_file(&sample_tracked_file("a.png"))
            .await
            .expect("insert");
        store
            .upsert_file(
                &TrackedFile::from_scan(
                    "b.json".into(),
                    FileType::Recipe,
                    "fh_b".into(),
                    None,
                    64,
                    None,
                )
                .expect("valid test data"),
            )
            .await
            .expect("insert");
        store
            .upsert_file(&sample_tracked_file("c.png"))
            .await
            .expect("insert");

        let all = store.list_files(None, None).await.expect("list");
        assert_eq!(all.len(), 3);

        let images = store
            .list_files(Some(FileType::Image), None)
            .await
            .expect("list images");
        assert_eq!(images.len(), 2);

        let limited = store.list_files(None, Some(1)).await.expect("limited");
        assert_eq!(limited.len(), 1);
    }

    // =========================================================================
    // TransferStore tests
    // =========================================================================

    #[tokio::test]
    async fn insert_and_query_transfer() {
        let store = SqliteSyncStore::open_in_memory().await.expect("open");
        let file = sample_tracked_file("output/t.png");
        store.upsert_file(&file).await.expect("insert file");

        let transfer =
            Transfer::new(file.id().to_string(), loc("local"), loc("cloud")).expect("valid");
        store
            .insert_transfer(&transfer)
            .await
            .expect("insert transfer");

        let queued = store.queued_transfers(&loc("cloud")).await.expect("queued");
        assert_eq!(queued.len(), 1);
        assert_eq!(queued[0].file_id(), file.id());
        assert_eq!(queued[0].dest(), &loc("cloud"));
    }

    #[tokio::test]
    async fn update_transfer_state() {
        let store = SqliteSyncStore::open_in_memory().await.expect("open");
        let file = sample_tracked_file("output/s.png");
        store.upsert_file(&file).await.expect("insert file");

        let mut transfer =
            Transfer::new(file.id().to_string(), loc("local"), loc("cloud")).expect("valid");
        store
            .insert_transfer(&transfer)
            .await
            .expect("insert transfer");

        transfer.start().expect("start");
        store
            .update_transfer(&transfer)
            .await
            .expect("update transfer");

        let queued = store.queued_transfers(&loc("cloud")).await.expect("queued");
        assert_eq!(queued.len(), 0);

        transfer.complete().expect("complete");
        store
            .update_transfer(&transfer)
            .await
            .expect("update transfer");

        let latest = store
            .latest_transfers_by_file(file.id())
            .await
            .expect("latest");
        assert_eq!(latest.len(), 1);
        assert_eq!(
            latest[0].state(),
            crate::domain::transfer::TransferState::Completed
        );
    }

    #[tokio::test]
    async fn failed_transfers_query() {
        let store = SqliteSyncStore::open_in_memory().await.expect("open");
        let file = sample_tracked_file("output/f.png");
        store.upsert_file(&file).await.expect("insert file");

        let mut transfer =
            Transfer::new(file.id().to_string(), loc("local"), loc("cloud")).expect("valid");
        transfer.start().expect("start");
        transfer
            .fail(
                "timeout".into(),
                crate::domain::retry::TransferErrorKind::Transient,
            )
            .expect("fail");
        store
            .insert_transfer(&transfer)
            .await
            .expect("insert transfer");

        let failed = store.failed_transfers().await.expect("failed");
        assert_eq!(failed.len(), 1);
        assert_eq!(failed[0].error(), Some("timeout"));
        assert_eq!(
            failed[0].error_kind(),
            Some(crate::domain::retry::TransferErrorKind::Transient)
        );
    }

    #[tokio::test]
    async fn latest_transfers_by_file_returns_latest_per_dest() {
        let store = SqliteSyncStore::open_in_memory().await.expect("open");
        let file = sample_tracked_file("output/r.png");
        store.upsert_file(&file).await.expect("insert file");

        let mut t1 =
            Transfer::new(file.id().to_string(), loc("local"), loc("cloud")).expect("valid");
        t1.start().expect("start");
        t1.fail(
            "err".into(),
            crate::domain::retry::TransferErrorKind::Transient,
        )
        .expect("fail");
        store.insert_transfer(&t1).await.expect("insert t1");

        let t2 = t1.retry().expect("retry");
        store.insert_transfer(&t2).await.expect("insert t2");

        let mut t3 = Transfer::new(file.id().to_string(), loc("local"), loc("pod")).expect("valid");
        t3.start().expect("start");
        t3.complete().expect("complete");
        store.insert_transfer(&t3).await.expect("insert t3");

        let latest = store
            .latest_transfers_by_file(file.id())
            .await
            .expect("latest");
        assert_eq!(latest.len(), 2);

        let cloud = latest
            .iter()
            .find(|t| t.dest() == &loc("cloud"))
            .expect("cloud");
        assert_eq!(
            cloud.state(),
            crate::domain::transfer::TransferState::Queued
        );
        assert_eq!(cloud.attempt(), 2);

        let pod = latest
            .iter()
            .find(|t| t.dest() == &loc("pod"))
            .expect("pod");
        assert_eq!(
            pod.state(),
            crate::domain::transfer::TransferState::Completed
        );
    }

    #[tokio::test]
    async fn cascade_delete_transfers() {
        let store = SqliteSyncStore::open_in_memory().await.expect("open");
        let file = sample_tracked_file("output/cas.png");
        let file_id = file.id().to_string();
        store.upsert_file(&file).await.expect("insert file");

        let transfer = Transfer::new(file_id.clone(), loc("local"), loc("cloud")).expect("valid");
        store
            .insert_transfer(&transfer)
            .await
            .expect("insert transfer");

        store
            .delete_file("output/cas.png")
            .await
            .expect("delete file");

        let count: usize = store
            .conn
            .call(move |conn| {
                conn.query_row(
                    "SELECT COUNT(*) FROM transfers WHERE file_id = ?",
                    params![file_id],
                    |row| row.get(0),
                )
                .map_err(|e| SyncError::Store(format!("{e}")))
            })
            .await
            .expect("count");
        assert_eq!(count, 0);
    }

    #[tokio::test]
    async fn queued_returns_only_latest_per_file_dest() {
        let store = SqliteSyncStore::open_in_memory().await.expect("open");
        let file = sample_tracked_file("output/q.png");
        store.upsert_file(&file).await.expect("insert file");

        let mut t1 =
            Transfer::new(file.id().to_string(), loc("local"), loc("cloud")).expect("valid");
        t1.start().expect("start");
        t1.fail(
            "err".into(),
            crate::domain::retry::TransferErrorKind::Transient,
        )
        .expect("fail");
        store.insert_transfer(&t1).await.expect("insert t1");

        let t2 = t1.retry().expect("retry");
        store.insert_transfer(&t2).await.expect("insert t2");

        let queued = store.queued_transfers(&loc("cloud")).await.expect("queued");
        assert_eq!(queued.len(), 1);
        assert_eq!(queued[0].attempt(), 2);
    }

    // =========================================================================
    // RemoteStore tests
    // =========================================================================

    #[tokio::test]
    async fn remote_management() {
        let store = SqliteSyncStore::open_in_memory().await.expect("open");

        let remote = RemoteConfig {
            location_id: LocationId::new("cloud").expect("valid"),
            backend: "rclone".into(),
            config: serde_json::json!({"bucket": "my-bucket"}),
            created_at: chrono::Utc::now(),
        };
        store.register_remote(&remote).await.expect("register");

        let remotes = store.list_remotes().await.expect("list");
        assert_eq!(remotes.len(), 1);
        assert_eq!(remotes[0].backend, "rclone");

        let removed = store
            .remove_remote(&LocationId::new("cloud").expect("valid"))
            .await
            .expect("remove");
        assert!(removed);

        let remotes2 = store.list_remotes().await.expect("list2");
        assert!(remotes2.is_empty());
    }
}
