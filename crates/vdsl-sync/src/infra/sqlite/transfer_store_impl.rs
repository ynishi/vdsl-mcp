//! TransferStore trait implementation for SqliteSyncStore.

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use rusqlite::params;

use crate::domain::location::LocationId;
use crate::domain::transfer::Transfer;
use crate::infra::error::InfraError;
use crate::infra::transfer_store::{TransferStatRow, TransferStore};

use super::mapping::{query_transfers, ts_to_string};
use super::{map_call_err, SqliteSyncStore};

#[async_trait]
impl TransferStore for SqliteSyncStore {
    async fn insert_transfer(&self, transfer: &Transfer) -> Result<(), InfraError> {
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

    async fn update_transfer(&self, transfer: &Transfer) -> Result<(), InfraError> {
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

    async fn queued_transfers(&self, dest: &LocationId) -> Result<Vec<Transfer>, InfraError> {
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

    async fn latest_transfers_by_file(&self, file_id: &str) -> Result<Vec<Transfer>, InfraError> {
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

    async fn failed_transfers(&self) -> Result<Vec<Transfer>, InfraError> {
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

    async fn all_pending_transfers(&self) -> Result<Vec<Transfer>, InfraError> {
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

    async fn transfer_stats(&self) -> Result<Vec<TransferStatRow>, InfraError> {
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
    ) -> Result<std::collections::HashMap<LocationId, usize>, InfraError> {
        self.conn
            .call(|conn| {
                // location_filesのactive件数をlocation別にカウント
                let mut stmt = conn
                    .prepare(
                        "SELECT location_id, COUNT(DISTINCT file_id) as file_count
                         FROM location_files
                         WHERE state = 'active'
                         GROUP BY location_id",
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

    async fn prune_completed(&self, before: DateTime<Utc>) -> Result<usize, InfraError> {
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

    async fn count_queued(&self) -> Result<usize, InfraError> {
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

    async fn cancel_orphaned_inflight(&self) -> Result<usize, InfraError> {
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

    async fn unblock_dependents(&self, completed_transfer_id: &str) -> Result<usize, InfraError> {
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
}
