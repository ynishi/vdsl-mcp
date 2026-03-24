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

use crate::application::error::SyncError;
use crate::domain::file_type::FileType;
use crate::domain::location::LocationId;
use crate::domain::tracked_file::TrackedFile;
use crate::domain::transfer::Transfer;
use crate::infra::error::InfraError;
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
        let conn =
            tokio_rusqlite::Connection::open(&path)
                .await
                .map_err(|e| InfraError::Store {
                    op: "sqlite",
                    reason: format!("open failed: {e}"),
                })?;
        conn.call(schema::init_connection)
            .await
            .map_err(map_call_err)?;
        Ok(Self { conn })
    }

    /// Open an in-memory database (for testing).
    pub async fn open_in_memory() -> Result<Self, SyncError> {
        let conn = tokio_rusqlite::Connection::open_in_memory()
            .await
            .map_err(|e| InfraError::Store {
                op: "sqlite",
                reason: format!("open_in_memory failed: {e}"),
            })?;
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
        tokio_rusqlite::Error::ConnectionClosed => InfraError::Store {
            op: "sqlite",
            reason: "sqlite connection closed".into(),
        }
        .into(),
        tokio_rusqlite::Error::Close((_, e)) => InfraError::Store {
            op: "sqlite",
            reason: format!("sqlite close error: {e}"),
        }
        .into(),
        other => InfraError::Store {
            op: "sqlite",
            reason: format!("tokio-rusqlite: {other:?}"),
        }
        .into(),
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
                    InfraError::Store { op: "sqlite", reason: format!(
                        "file_size exceeds i64::MAX: {} (file {})",
                        file.file_size(),
                        file.id()
                    ) }
                })?;
                let modified_at_str = file.modified_at().map(ts_to_string);
                let registered_at_str = ts_to_string(file.registered_at());
                let updated_at_str = ts_to_string(file.updated_at());
                let deleted_at_str = file.deleted_at().map(ts_to_string);
                conn.execute(
                    "INSERT INTO tracked_files (id, relative_path, file_type, file_hash, content_hash, file_size, embedded_id, modified_at, registered_at, updated_at, deleted_at)
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)
                     ON CONFLICT (relative_path) DO UPDATE SET
                         file_type = excluded.file_type,
                         file_hash = excluded.file_hash,
                         content_hash = excluded.content_hash,
                         file_size = excluded.file_size,
                         embedded_id = excluded.embedded_id,
                         modified_at = excluded.modified_at,
                         updated_at = excluded.updated_at,
                         deleted_at = excluded.deleted_at",
                    params![
                        file.id(),
                        file.relative_path(),
                        file.file_type().as_str(),
                        file.file_hash(),
                        file.content_hash(),
                        file_size_i64,
                        file.embedded_id(),
                        modified_at_str,
                        registered_at_str,
                        updated_at_str,
                        deleted_at_str,
                    ],
                )
                .map_err(|e| InfraError::Store { op: "sqlite", reason: format!("upsert_file failed: {e}") })?;
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
                    .map_err(|e| InfraError::Store {
                        op: "sqlite",
                        reason: format!("{e}"),
                    })?;
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
                    let n_i64 = i64::try_from(n).map_err(|_| InfraError::Store {
                        op: "sqlite",
                        reason: format!("limit exceeds i64::MAX: {n}"),
                    })?;
                    param_values.push(Box::new(n_i64));
                }

                let refs: Vec<&dyn rusqlite::types::ToSql> =
                    param_values.iter().map(|p| p.as_ref()).collect();
                query_tracked_files(conn, &sql, &refs)
            })
            .await
            .map_err(map_call_err)
    }

    async fn count_files(&self) -> Result<usize, SyncError> {
        self.conn
            .call(|conn| {
                let count: usize = conn
                    .query_row(
                        "SELECT COUNT(*) FROM tracked_files WHERE deleted_at IS NULL",
                        [],
                        |row| row.get(0),
                    )
                    .map_err(|e| InfraError::Store {
                        op: "sqlite",
                        reason: format!("count_files failed: {e}"),
                    })?;
                Ok(count)
            })
            .await
            .map_err(map_call_err)
    }

    async fn list_all_paths(&self) -> Result<Vec<String>, SyncError> {
        self.conn
            .call(|conn| {
                let mut stmt = conn
                    .prepare(
                        "SELECT relative_path FROM tracked_files WHERE deleted_at IS NULL ORDER BY relative_path",
                    )
                    .map_err(|e| InfraError::Store { op: "sqlite", reason: format!("{e}") })?;
                let rows = stmt
                    .query_map([], |row| row.get::<_, String>(0))
                    .map_err(|e| InfraError::Store { op: "sqlite", reason: format!("{e}") })?;
                let mut paths = Vec::new();
                for row in rows {
                    paths.push(row.map_err(|e| InfraError::Store { op: "sqlite", reason: format!("{e}") })?);
                }
                Ok(paths)
            })
            .await
            .map_err(map_call_err)
    }

    async fn list_all_ids(&self) -> Result<Vec<String>, SyncError> {
        self.conn
            .call(|conn| {
                let mut stmt = conn
                    .prepare("SELECT id FROM tracked_files WHERE deleted_at IS NULL ORDER BY id")
                    .map_err(|e| InfraError::Store {
                        op: "sqlite",
                        reason: format!("{e}"),
                    })?;
                let rows = stmt
                    .query_map([], |row| row.get::<_, String>(0))
                    .map_err(|e| InfraError::Store {
                        op: "sqlite",
                        reason: format!("{e}"),
                    })?;
                let mut ids = Vec::new();
                for row in rows {
                    ids.push(row.map_err(|e| InfraError::Store {
                        op: "sqlite",
                        reason: format!("{e}"),
                    })?);
                }
                Ok(ids)
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
                let depends_on_str = t.depends_on().map(|s| s.to_string());
                conn.execute(
                    "INSERT INTO transfers (id, file_id, src, dest, kind, state, error, error_kind, attempt, created_at, started_at, finished_at, depends_on)
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13)",
                    params![
                        t.id(),
                        t.file_id(),
                        t.src().as_str(),
                        t.dest().as_str(),
                        t.kind().as_str(),
                        t.state().as_str(),
                        t.error(),
                        error_kind_str,
                        attempt_i64,
                        created_at_str,
                        started_at_str,
                        finished_at_str,
                        depends_on_str,
                    ],
                )
                .map_err(|e| InfraError::Store { op: "sqlite", reason: format!("insert_transfer failed: {e}") })?;
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
                .map_err(|e| InfraError::Store { op: "sqlite", reason: format!("update_transfer failed: {e}") })?;
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
                    "SELECT t.* FROM transfers t
                     WHERE t.state = 'failed'
                       AND NOT EXISTS (
                           SELECT 1 FROM transfers t2
                           WHERE t2.file_id = t.file_id
                             AND t2.dest = t.dest
                             AND t2.ROWID > t.ROWID
                       )
                     ORDER BY t.finished_at DESC",
                    &[],
                )
            })
            .await
            .map_err(map_call_err)
    }

    async fn all_pending_transfers(&self) -> Result<Vec<Transfer>, SyncError> {
        self.conn
            .call(|conn| {
                query_transfers(
                    conn,
                    "SELECT t.* FROM transfers t
                     WHERE t.state IN ('queued', 'blocked')
                       AND NOT EXISTS (
                           SELECT 1 FROM transfers t2
                           WHERE t2.file_id = t.file_id
                             AND t2.dest = t.dest
                             AND t2.ROWID > t.ROWID
                       )
                     ORDER BY t.created_at",
                    &[],
                )
            })
            .await
            .map_err(map_call_err)
    }

    async fn transfer_stats(
        &self,
    ) -> Result<Vec<crate::infra::transfer_store::TransferStatRow>, SyncError> {
        use crate::infra::transfer_store::TransferStatRow;

        self.conn
            .call(|conn| {
                // 最新Transfer（file_id×dest別）をGROUP BYして集約
                let mut stmt = conn
                    .prepare(
                        "SELECT src, dest, state, error_kind, attempt, COUNT(DISTINCT file_id) as file_count
                         FROM (
                             SELECT t.src, t.dest, t.state, t.error_kind, t.attempt, t.file_id
                             FROM transfers t
                             WHERE NOT EXISTS (
                                 SELECT 1 FROM transfers t2
                                 WHERE t2.file_id = t.file_id
                                   AND t2.dest = t.dest
                                   AND t2.ROWID > t.ROWID
                             )
                         )
                         GROUP BY src, dest, state, error_kind, attempt",
                    )
                    .map_err(|e| InfraError::Store {
                        op: "sqlite",
                        reason: format!("transfer_stats prepare failed: {e}"),
                    })?;

                let rows = stmt
                    .query_map([], |row| {
                        let src_str: String = row.get(0)?;
                        let dest_str: String = row.get(1)?;
                        let state: String = row.get(2)?;
                        let error_kind: Option<String> = row.get(3)?;
                        let attempt: u32 = row.get(4)?;
                        let file_count: usize = row.get(5)?;
                        Ok((src_str, dest_str, state, error_kind, attempt, file_count))
                    })
                    .map_err(|e| InfraError::Store {
                        op: "sqlite",
                        reason: format!("transfer_stats query failed: {e}"),
                    })?;

                let mut result = Vec::new();
                for row in rows {
                    let (src_str, dest_str, state_str, error_kind, attempt, file_count) =
                        row.map_err(|e| InfraError::Store {
                            op: "sqlite",
                            reason: format!("transfer_stats row failed: {e}"),
                        })?;
                    let src = LocationId::new(src_str).map_err(|e| InfraError::Store {
                        op: "sqlite",
                        reason: format!("invalid src location: {e}"),
                    })?;
                    let dest = LocationId::new(dest_str).map_err(|e| InfraError::Store {
                        op: "sqlite",
                        reason: format!("invalid dest location: {e}"),
                    })?;
                    let state: crate::domain::transfer::TransferState =
                        state_str.parse().map_err(|e| InfraError::Store {
                            op: "sqlite",
                            reason: format!("invalid transfer state: {e}"),
                        })?;
                    result.push(TransferStatRow {
                        src,
                        dest,
                        state,
                        error_kind,
                        attempt,
                        file_count,
                    });
                }
                Ok(result)
            })
            .await
            .map_err(map_call_err)
    }

    async fn present_counts_by_location(
        &self,
    ) -> Result<std::collections::HashMap<LocationId, usize>, SyncError> {
        self.conn
            .call(|conn| {
                // src（送出元＝ファイル存在）と completed dest を UNION し、
                // location × file_id の重複を排除してカウント
                let mut stmt = conn
                    .prepare(
                        "WITH latest AS (
                             SELECT t.src, t.dest, t.state, t.file_id
                             FROM transfers t
                             WHERE NOT EXISTS (
                                 SELECT 1 FROM transfers t2
                                 WHERE t2.file_id = t.file_id
                                   AND t2.dest = t.dest
                                   AND t2.ROWID > t.ROWID
                             )
                         )
                         SELECT location, COUNT(DISTINCT file_id) as file_count
                         FROM (
                             SELECT src AS location, file_id FROM latest
                             UNION
                             SELECT dest AS location, file_id FROM latest WHERE state = 'completed'
                         )
                         GROUP BY location",
                    )
                    .map_err(|e| InfraError::Store {
                        op: "sqlite",
                        reason: format!("present_counts_by_location prepare failed: {e}"),
                    })?;

                let rows = stmt
                    .query_map([], |row| {
                        let loc: String = row.get(0)?;
                        let count: usize = row.get(1)?;
                        Ok((loc, count))
                    })
                    .map_err(|e| InfraError::Store {
                        op: "sqlite",
                        reason: format!("present_counts_by_location query failed: {e}"),
                    })?;

                let mut result = std::collections::HashMap::new();
                for row in rows {
                    let (loc_str, count) = row.map_err(|e| InfraError::Store {
                        op: "sqlite",
                        reason: format!("present_counts_by_location row failed: {e}"),
                    })?;
                    let loc = LocationId::new(loc_str).map_err(|e| InfraError::Store {
                        op: "sqlite",
                        reason: format!("invalid location: {e}"),
                    })?;
                    result.insert(loc, count);
                }
                Ok(result)
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
                    .map_err(|e| InfraError::Store {
                        op: "sqlite",
                        reason: format!("prune_completed failed: {e}"),
                    })?;
                Ok(deleted)
            })
            .await
            .map_err(map_call_err)
    }

    async fn count_queued(&self) -> Result<usize, SyncError> {
        self.conn
            .call(|conn| {
                let count: i64 = conn
                    .query_row(
                        "SELECT COUNT(*) FROM transfers WHERE state = 'queued'",
                        [],
                        |row| row.get(0),
                    )
                    .map_err(|e| InfraError::Store {
                        op: "sqlite",
                        reason: format!("count_queued failed: {e}"),
                    })?;
                Ok(count as usize)
            })
            .await
            .map_err(map_call_err)
    }

    async fn cancel_orphaned_inflight(&self) -> Result<usize, SyncError> {
        self.conn
            .call(|conn| {
                let count = conn
                    .execute(
                        "UPDATE transfers SET state = 'cancelled', finished_at = strftime('%Y-%m-%dT%H:%M:%SZ', 'now') \
                         WHERE state = 'in_flight'",
                        [],
                    )
                    .map_err(|e| InfraError::Store {
                        op: "sqlite",
                        reason: format!("cancel_orphaned_inflight failed: {e}"),
                    })?;
                Ok(count)
            })
            .await
            .map_err(map_call_err)
    }

    async fn unblock_dependents(&self, completed_transfer_id: &str) -> Result<usize, SyncError> {
        let id = completed_transfer_id.to_string();
        self.conn
            .call(move |conn| {
                let count = conn
                    .execute(
                        "UPDATE transfers SET state = 'queued' WHERE depends_on = ? AND state = 'blocked'",
                        params![id],
                    )
                    .map_err(|e| InfraError::Store {
                        op: "sqlite",
                        reason: format!("unblock_dependents failed: {e}"),
                    })?;
                Ok(count)
            })
            .await
            .map_err(map_call_err)
    }

    async fn requeue_all(
        &self,
        file_ids: &[String],
        routes: &[(LocationId, LocationId)],
    ) -> Result<usize, SyncError> {
        // Build route pairs as owned strings for the closure.
        let route_pairs: Vec<(String, String)> = routes
            .iter()
            .map(|(s, d)| (s.as_str().to_string(), d.as_str().to_string()))
            .collect();
        let file_ids = file_ids.to_vec();

        self.conn
            .call(move |conn| {
                let tx = conn
                    .transaction()
                    .map_err(|e| InfraError::Store { op: "sqlite", reason: format!("requeue_all tx: {e}") })?;

                let mut inserted: usize = 0;
                for file_id in &file_ids {
                    for (src, dest) in &route_pairs {
                        // Skip if a completed transfer already exists for this file×route.
                        let has_completed: bool = tx
                            .query_row(
                                "SELECT EXISTS(
                                    SELECT 1 FROM transfers
                                    WHERE file_id = ?1 AND src = ?2 AND dest = ?3
                                      AND state = 'completed'
                                )",
                                params![file_id, src, dest],
                                |row| row.get(0),
                            )
                            .map_err(|e| InfraError::Store {
                                op: "sqlite",
                                reason: format!("requeue_all check: {e}"),
                            })?;

                        if has_completed {
                            continue;
                        }

                        let src_loc = LocationId::new(src).map_err(|e| InfraError::Store {
                            op: "sqlite",
                            reason: format!("requeue_all: {e}"),
                        })?;
                        let dest_loc = LocationId::new(dest).map_err(|e| InfraError::Store {
                            op: "sqlite",
                            reason: format!("requeue_all: {e}"),
                        })?;
                        let t = Transfer::new(file_id.clone(), src_loc, dest_loc).map_err(|e| {
                            InfraError::Store {
                                op: "sqlite",
                                reason: format!("requeue_all: {e}"),
                            }
                        })?;
                        let created_at_str = ts_to_string(t.created_at());
                        tx.execute(
                            "INSERT INTO transfers (id, file_id, src, dest, kind, state, error, error_kind, attempt, created_at, started_at, finished_at)
                             VALUES (?1, ?2, ?3, ?4, ?5, ?6, NULL, NULL, 0, ?7, NULL, NULL)",
                            params![
                                t.id(),
                                t.file_id(),
                                t.src().as_str(),
                                t.dest().as_str(),
                                t.kind().as_str(),
                                t.state().as_str(),
                                created_at_str,
                            ],
                        )
                        .map_err(|e| InfraError::Store { op: "sqlite", reason: format!("requeue_all insert: {e}") })?;
                        inserted += 1;
                    }
                }

                tx.commit()
                    .map_err(|e| InfraError::Store { op: "sqlite", reason: format!("requeue_all commit: {e}") })?;
                Ok(inserted)
            })
            .await
            .map_err(map_call_err)
    }

    async fn purge_non_completed(&self) -> Result<usize, SyncError> {
        self.conn
            .call(|conn| {
                let deleted = conn
                    .execute(
                        "DELETE FROM transfers WHERE state NOT IN ('completed', 'cancelled')",
                        [],
                    )
                    .map_err(|e| InfraError::Store {
                        op: "sqlite",
                        reason: format!("purge_non_completed: {e}"),
                    })?;
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
                    .map_err(|e| InfraError::Store { op: "sqlite", reason: format!("config serialize: {e}") })?;
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
                .map_err(|e| InfraError::Store { op: "sqlite", reason: format!("{e}") })?;
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
                    .map_err(|e| InfraError::Store {
                        op: "sqlite",
                        reason: format!("{e}"),
                    })?;
                let mut rows = stmt
                    .query_map(params![loc_str], row_to_remote_tuple)
                    .map_err(|e| InfraError::Store {
                        op: "sqlite",
                        reason: format!("{e}"),
                    })?;
                match rows.next() {
                    Some(row) => {
                        let (loc, backend, config_str, created_at_str) =
                            row.map_err(|e| InfraError::Store {
                                op: "sqlite",
                                reason: format!("{e}"),
                            })?;
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
                    .map_err(|e| InfraError::Store { op: "sqlite", reason: format!("{e}") })?;
                let rows = stmt
                    .query_map([], row_to_remote_tuple)
                    .map_err(|e| InfraError::Store { op: "sqlite", reason: format!("{e}") })?;

                let mut remotes = Vec::new();
                for row in rows {
                    let (loc, backend, config_str, created_at_str) =
                        row.map_err(|e| InfraError::Store { op: "sqlite", reason: format!("{e}") })?;
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
                    .map_err(|e| InfraError::Store {
                        op: "sqlite",
                        reason: format!("{e}"),
                    })?;
                Ok(changes > 0)
            })
            .await
            .map_err(map_call_err)
    }
}

// =============================================================================
// TopologyFileStore trait implementation
// =============================================================================

use crate::domain::topology_file::TopologyFile;
use crate::infra::topology_file_store::TopologyFileStore;

use mapping::query_topology_files;

#[async_trait]
impl TopologyFileStore for SqliteSyncStore {
    async fn upsert(&self, file: &TopologyFile) -> Result<(), SyncError> {
        let file = file.clone();
        self.conn
            .call(move |conn| {
                let registered_at_str = ts_to_string(file.registered_at());
                let deleted_at_str = file.deleted_at().map(ts_to_string);
                conn.execute(
                    "INSERT INTO topology_files (id, relative_path, canonical_hash, file_type, registered_at, deleted_at)
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6)
                     ON CONFLICT (id) DO UPDATE SET
                         relative_path = excluded.relative_path,
                         canonical_hash = excluded.canonical_hash,
                         file_type = excluded.file_type,
                         registered_at = excluded.registered_at,
                         deleted_at = excluded.deleted_at",
                    params![
                        file.id(),
                        file.relative_path(),
                        file.canonical_hash(),
                        file.file_type().as_str(),
                        registered_at_str,
                        deleted_at_str,
                    ],
                )
                .map_err(|e| InfraError::Store {
                    op: "sqlite",
                    reason: format!("upsert topology_file failed: {e}"),
                })?;
                Ok(())
            })
            .await
            .map_err(map_call_err)
    }

    async fn get_by_id(&self, id: &str) -> Result<Option<TopologyFile>, SyncError> {
        let id = id.to_string();
        self.conn
            .call(move |conn| {
                let files = query_topology_files(
                    conn,
                    "SELECT * FROM topology_files WHERE id = ?",
                    &[&id as &dyn rusqlite::types::ToSql],
                )?;
                Ok(files.into_iter().next())
            })
            .await
            .map_err(map_call_err)
    }

    async fn get_by_path(&self, relative_path: &str) -> Result<Option<TopologyFile>, SyncError> {
        let path = relative_path.to_string();
        self.conn
            .call(move |conn| {
                let files = query_topology_files(
                    conn,
                    "SELECT * FROM topology_files WHERE relative_path = ? AND deleted_at IS NULL",
                    &[&path as &dyn rusqlite::types::ToSql],
                )?;
                Ok(files.into_iter().next())
            })
            .await
            .map_err(map_call_err)
    }

    async fn find_by_canonical_hash(&self, hash: &str) -> Result<Option<TopologyFile>, SyncError> {
        let hash = hash.to_string();
        self.conn
            .call(move |conn| {
                let files = query_topology_files(
                    conn,
                    "SELECT * FROM topology_files WHERE canonical_hash = ? AND deleted_at IS NULL",
                    &[&hash as &dyn rusqlite::types::ToSql],
                )?;
                Ok(files.into_iter().next())
            })
            .await
            .map_err(map_call_err)
    }

    async fn list_active(
        &self,
        file_type: Option<FileType>,
        limit: Option<usize>,
    ) -> Result<Vec<TopologyFile>, SyncError> {
        self.conn
            .call(move |conn| {
                let mut sql = String::from("SELECT * FROM topology_files WHERE deleted_at IS NULL");
                let mut param_values: Vec<Box<dyn rusqlite::types::ToSql>> = Vec::new();

                if let Some(ft) = file_type {
                    sql.push_str(" AND file_type = ?");
                    param_values.push(Box::new(ft.as_str().to_string()));
                }
                sql.push_str(" ORDER BY registered_at DESC");
                if let Some(n) = limit {
                    sql.push_str(" LIMIT ?");
                    let n_i64 = i64::try_from(n).map_err(|_| InfraError::Store {
                        op: "sqlite",
                        reason: format!("limit exceeds i64::MAX: {n}"),
                    })?;
                    param_values.push(Box::new(n_i64));
                }

                let refs: Vec<&dyn rusqlite::types::ToSql> =
                    param_values.iter().map(|p| p.as_ref()).collect();
                query_topology_files(conn, &sql, &refs)
            })
            .await
            .map_err(map_call_err)
    }

    async fn list_deleted(&self) -> Result<Vec<TopologyFile>, SyncError> {
        self.conn
            .call(|conn| {
                query_topology_files(
                    conn,
                    "SELECT * FROM topology_files WHERE deleted_at IS NOT NULL ORDER BY deleted_at DESC",
                    &[],
                )
            })
            .await
            .map_err(map_call_err)
    }

    async fn count_active(&self) -> Result<usize, SyncError> {
        self.conn
            .call(|conn| {
                let count: usize = conn
                    .query_row(
                        "SELECT COUNT(*) FROM topology_files WHERE deleted_at IS NULL",
                        [],
                        |row| row.get(0),
                    )
                    .map_err(|e| InfraError::Store {
                        op: "sqlite",
                        reason: format!("count_active topology_files failed: {e}"),
                    })?;
                Ok(count)
            })
            .await
            .map_err(map_call_err)
    }

    async fn list_active_paths(&self) -> Result<Vec<String>, SyncError> {
        self.conn
            .call(|conn| {
                let mut stmt = conn
                    .prepare(
                        "SELECT relative_path FROM topology_files WHERE deleted_at IS NULL ORDER BY relative_path",
                    )
                    .map_err(|e| InfraError::Store {
                        op: "sqlite",
                        reason: format!("{e}"),
                    })?;
                let rows = stmt
                    .query_map([], |row| row.get::<_, String>(0))
                    .map_err(|e| InfraError::Store {
                        op: "sqlite",
                        reason: format!("{e}"),
                    })?;
                let mut paths = Vec::new();
                for row in rows {
                    paths.push(row.map_err(|e| InfraError::Store {
                        op: "sqlite",
                        reason: format!("{e}"),
                    })?);
                }
                Ok(paths)
            })
            .await
            .map_err(map_call_err)
    }
}

// =============================================================================
// LocationFileStore trait implementation
// =============================================================================

use crate::domain::location_file::LocationFile;
use crate::infra::location_file_store::LocationFileStore;

use mapping::query_location_files;

#[async_trait]
impl LocationFileStore for SqliteSyncStore {
    async fn upsert(&self, file: &LocationFile) -> Result<(), SyncError> {
        let file = file.clone();
        self.conn
            .call(move |conn| {
                let size_i64 = i64::try_from(file.fingerprint().size).map_err(|_| {
                    InfraError::Store {
                        op: "sqlite",
                        reason: format!(
                            "size exceeds i64::MAX: {} (file_id {})",
                            file.fingerprint().size,
                            file.file_id()
                        ),
                    }
                })?;
                let modified_at_str = file.fingerprint().modified_at.map(ts_to_string);
                let updated_at_str = ts_to_string(file.updated_at());
                conn.execute(
                    "INSERT INTO location_files (file_id, location_id, relative_path, file_hash, content_hash, meta_hash, size, modified_at, state, embedded_id, updated_at)
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)
                     ON CONFLICT (file_id, location_id) DO UPDATE SET
                         relative_path = excluded.relative_path,
                         file_hash = excluded.file_hash,
                         content_hash = excluded.content_hash,
                         meta_hash = excluded.meta_hash,
                         size = excluded.size,
                         modified_at = excluded.modified_at,
                         state = excluded.state,
                         embedded_id = excluded.embedded_id,
                         updated_at = excluded.updated_at",
                    params![
                        file.file_id(),
                        file.location_id().as_str(),
                        file.relative_path(),
                        file.fingerprint().file_hash,
                        file.fingerprint().content_hash,
                        file.fingerprint().meta_hash,
                        size_i64,
                        modified_at_str,
                        file.state().as_str(),
                        file.embedded_id(),
                        updated_at_str,
                    ],
                )
                .map_err(|e| InfraError::Store {
                    op: "sqlite",
                    reason: format!("upsert location_file failed: {e}"),
                })?;
                Ok(())
            })
            .await
            .map_err(map_call_err)
    }

    async fn get(
        &self,
        file_id: &str,
        location_id: &LocationId,
    ) -> Result<Option<LocationFile>, SyncError> {
        let file_id = file_id.to_string();
        let loc_str = location_id.as_str().to_string();
        self.conn
            .call(move |conn| {
                let files = query_location_files(
                    conn,
                    "SELECT * FROM location_files WHERE file_id = ? AND location_id = ?",
                    &[
                        &file_id as &dyn rusqlite::types::ToSql,
                        &loc_str as &dyn rusqlite::types::ToSql,
                    ],
                )?;
                Ok(files.into_iter().next())
            })
            .await
            .map_err(map_call_err)
    }

    async fn list_by_file(&self, file_id: &str) -> Result<Vec<LocationFile>, SyncError> {
        let file_id = file_id.to_string();
        self.conn
            .call(move |conn| {
                query_location_files(
                    conn,
                    "SELECT * FROM location_files WHERE file_id = ? ORDER BY location_id",
                    &[&file_id as &dyn rusqlite::types::ToSql],
                )
            })
            .await
            .map_err(map_call_err)
    }

    async fn list_by_location(
        &self,
        location_id: &LocationId,
    ) -> Result<Vec<LocationFile>, SyncError> {
        let loc_str = location_id.as_str().to_string();
        self.conn
            .call(move |conn| {
                query_location_files(
                    conn,
                    "SELECT * FROM location_files WHERE location_id = ? ORDER BY relative_path",
                    &[&loc_str as &dyn rusqlite::types::ToSql],
                )
            })
            .await
            .map_err(map_call_err)
    }

    async fn list_by_files(
        &self,
        file_ids: &[&str],
    ) -> Result<std::collections::HashMap<String, Vec<LocationFile>>, SyncError> {
        let file_ids: Vec<String> = file_ids.iter().map(|s| s.to_string()).collect();
        self.conn
            .call(move |conn| {
                let mut result: std::collections::HashMap<String, Vec<LocationFile>> =
                    std::collections::HashMap::new();
                // バッチサイズ999（SQLiteパラメータ制限）
                for chunk in file_ids.chunks(999) {
                    let placeholders: Vec<&str> =
                        chunk.iter().map(|_| "?").collect();
                    let sql = format!(
                        "SELECT * FROM location_files WHERE file_id IN ({}) ORDER BY file_id, location_id",
                        placeholders.join(",")
                    );
                    let params: Vec<&dyn rusqlite::types::ToSql> =
                        chunk.iter().map(|s| s as &dyn rusqlite::types::ToSql).collect();
                    let files = query_location_files(conn, &sql, &params)?;
                    for file in files {
                        result
                            .entry(file.file_id().to_string())
                            .or_default()
                            .push(file);
                    }
                }
                Ok(result)
            })
            .await
            .map_err(map_call_err)
    }

    async fn delete(&self, file_id: &str, location_id: &LocationId) -> Result<bool, SyncError> {
        let file_id = file_id.to_string();
        let loc_str = location_id.as_str().to_string();
        self.conn
            .call(move |conn| {
                let changes = conn
                    .execute(
                        "DELETE FROM location_files WHERE file_id = ? AND location_id = ?",
                        params![file_id, loc_str],
                    )
                    .map_err(|e| InfraError::Store {
                        op: "sqlite",
                        reason: format!("delete location_file failed: {e}"),
                    })?;
                Ok(changes > 0)
            })
            .await
            .map_err(map_call_err)
    }

    async fn count_by_location(&self, location_id: &LocationId) -> Result<usize, SyncError> {
        let loc_str = location_id.as_str().to_string();
        self.conn
            .call(move |conn| {
                let count: usize = conn
                    .query_row(
                        "SELECT COUNT(*) FROM location_files WHERE location_id = ?",
                        params![loc_str],
                        |row| row.get(0),
                    )
                    .map_err(|e| InfraError::Store {
                        op: "sqlite",
                        reason: format!("count_by_location failed: {e}"),
                    })?;
                Ok(count)
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
                    FileType::Asset,
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
    async fn failed_transfers_excludes_retried() {
        let store = SqliteSyncStore::open_in_memory().await.expect("open");
        let file = sample_tracked_file("output/retry.png");
        store.upsert_file(&file).await.expect("insert file");

        // T1: Failed (attempt=1)
        let mut t1 =
            Transfer::new(file.id().to_string(), loc("local"), loc("cloud")).expect("valid");
        t1.start().expect("start");
        t1.fail(
            "net error".into(),
            crate::domain::retry::TransferErrorKind::Transient,
        )
        .expect("fail");
        store.insert_transfer(&t1).await.expect("insert t1");

        // T2: retry of T1 → Queued (attempt=2), then fails again
        let mut t2 = t1.retry().expect("retry");
        t2.start().expect("start");
        t2.fail(
            "net error again".into(),
            crate::domain::retry::TransferErrorKind::Transient,
        )
        .expect("fail");
        store.insert_transfer(&t2).await.expect("insert t2");

        // failed_transfers should return only T2 (latest), not T1
        let failed = store.failed_transfers().await.expect("failed");
        assert_eq!(
            failed.len(),
            1,
            "should return only the latest failed transfer"
        );
        assert_eq!(failed[0].error(), Some("net error again"));
        assert_eq!(failed[0].attempt(), 2);
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
                .map_err(|e| InfraError::Store {
                    op: "sqlite",
                    reason: format!("{e}"),
                })
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

    // =========================================================================
    // unblock_dependents tests
    // =========================================================================

    #[tokio::test]
    async fn unblock_dependents_transitions_blocked_to_queued() {
        use crate::domain::transfer::TransferKind;

        let store = SqliteSyncStore::open_in_memory().await.expect("open");
        let file = sample_tracked_file("output/chain.png");
        store.upsert_file(&file).await.expect("insert file");

        // T1: local→cloud (Queued — 先行transfer)
        let t1 =
            Transfer::new(file.id().to_string(), loc("local"), loc("cloud")).expect("valid t1");
        let t1_id = t1.id().to_string();
        store.insert_transfer(&t1).await.expect("insert t1");

        // T2: cloud→pod (Blocked, depends_on=T1)
        let t2 = Transfer::with_dependency(
            file.id().to_string(),
            loc("cloud"),
            loc("pod"),
            TransferKind::Sync,
            t1_id.clone(),
        )
        .expect("valid t2");
        let t2_id = t2.id().to_string();
        store.insert_transfer(&t2).await.expect("insert t2");

        // Before unblock: T2 should NOT appear in queued_transfers
        let queued_before = store.queued_transfers(&loc("pod")).await.expect("queued");
        assert_eq!(
            queued_before.len(),
            0,
            "blocked transfer must not appear in queued"
        );

        // Simulate T1 completion → unblock dependents
        let unblocked = store.unblock_dependents(&t1_id).await.expect("unblock");
        assert_eq!(unblocked, 1, "exactly one transfer should be unblocked");

        // After unblock: T2 should appear in queued_transfers
        let queued_after = store.queued_transfers(&loc("pod")).await.expect("queued");
        assert_eq!(
            queued_after.len(),
            1,
            "unblocked transfer must appear in queued"
        );
        assert_eq!(queued_after[0].id(), t2_id);
    }

    #[tokio::test]
    async fn unblock_dependents_ignores_non_blocked_state() {
        use crate::domain::transfer::TransferKind;

        let store = SqliteSyncStore::open_in_memory().await.expect("open");
        let file = sample_tracked_file("output/nonblock.png");
        store.upsert_file(&file).await.expect("insert file");

        // T1: local→cloud (Queued)
        let t1 =
            Transfer::new(file.id().to_string(), loc("local"), loc("cloud")).expect("valid t1");
        let t1_id = t1.id().to_string();
        store.insert_transfer(&t1).await.expect("insert t1");

        // T2: depends on T1, but manually set to in_flight (not blocked)
        let t2 = Transfer::with_dependency(
            file.id().to_string(),
            loc("cloud"),
            loc("pod"),
            TransferKind::Sync,
            t1_id.clone(),
        )
        .expect("valid t2");
        // with_dependency creates Blocked. Insert as-is, then manually
        // update via SQL to simulate a non-blocked state (race condition).
        store.insert_transfer(&t2).await.expect("insert t2");

        // Manually update T2 to in_flight via SQL (simulating a race)
        let t2_id_clone = t2.id().to_string();
        store
            .conn
            .call(move |conn| {
                conn.execute(
                    "UPDATE transfers SET state = 'in_flight' WHERE id = ?",
                    params![t2_id_clone],
                )
                .map_err(|e| InfraError::Store {
                    op: "sqlite",
                    reason: format!("{e}"),
                })
            })
            .await
            .expect("manual update");

        // unblock should NOT touch in_flight transfers
        let unblocked = store.unblock_dependents(&t1_id).await.expect("unblock");
        assert_eq!(unblocked, 0, "in_flight transfer must not be unblocked");
    }

    #[tokio::test]
    async fn unblock_dependents_multiple_dependents() {
        use crate::domain::transfer::TransferKind;

        let store = SqliteSyncStore::open_in_memory().await.expect("open");
        let file_a = sample_tracked_file("output/multi_a.png");
        let file_b = sample_tracked_file("output/multi_b.png");
        store.upsert_file(&file_a).await.expect("insert a");
        store.upsert_file(&file_b).await.expect("insert b");

        // T1: local→cloud (shared dependency)
        let t1 =
            Transfer::new(file_a.id().to_string(), loc("local"), loc("cloud")).expect("valid t1");
        let t1_id = t1.id().to_string();
        store.insert_transfer(&t1).await.expect("insert t1");

        // T2: cloud→pod for file_a (Blocked, depends_on=T1)
        let t2 = Transfer::with_dependency(
            file_a.id().to_string(),
            loc("cloud"),
            loc("pod"),
            TransferKind::Sync,
            t1_id.clone(),
        )
        .expect("valid t2");
        store.insert_transfer(&t2).await.expect("insert t2");

        // T3: cloud→nas for file_b (Blocked, depends_on=T1)
        let t3 = Transfer::with_dependency(
            file_b.id().to_string(),
            loc("cloud"),
            loc("nas"),
            TransferKind::Sync,
            t1_id.clone(),
        )
        .expect("valid t3");
        store.insert_transfer(&t3).await.expect("insert t3");

        // Unblock both at once
        let unblocked = store.unblock_dependents(&t1_id).await.expect("unblock");
        assert_eq!(unblocked, 2, "both blocked transfers should be unblocked");

        // Verify both are now queued
        let pod_queued = store.queued_transfers(&loc("pod")).await.expect("pod");
        assert_eq!(pod_queued.len(), 1);
        let nas_queued = store.queued_transfers(&loc("nas")).await.expect("nas");
        assert_eq!(nas_queued.len(), 1);
    }

    #[tokio::test]
    async fn unblock_dependents_no_dependents_returns_zero() {
        let store = SqliteSyncStore::open_in_memory().await.expect("open");

        // No transfers at all — should return 0 without error
        let unblocked = store
            .unblock_dependents("nonexistent-id")
            .await
            .expect("unblock");
        assert_eq!(unblocked, 0);
    }
}
