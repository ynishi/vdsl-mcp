//! SQLite persistence for sync task status.
//!
//! Persists `TaskStatus<SyncReport>` so that poll() survives
//! MCP server session restarts. On startup, `recover_stale_running()`
//! marks any `running` tasks as `failed` (process crashed).

use chrono::Utc;
use rusqlite::params;
use rusqlite::OptionalExtension;

use crate::application::sdk::SyncReport;
use crate::application::task::{TaskId, TaskStatus};
use crate::infra::error::InfraError;

use super::{map_call_err, SqliteSyncStore};

impl SqliteSyncStore {
    /// Insert a new task as Pending.
    pub async fn insert_task(&self, id: &TaskId) -> Result<(), InfraError> {
        let id_str = id.as_str().to_string();
        let now = Utc::now().to_rfc3339();
        self.conn
            .call(move |conn| {
                conn.execute(
                    "INSERT OR REPLACE INTO sync_tasks \
                     (task_id, status, phase, created_at, updated_at) \
                     VALUES (?1, 'pending', '', ?2, ?2)",
                    params![id_str, now],
                )
                .map(|_| ())
                .map_err(|e| InfraError::Store {
                    op: "insert_task",
                    reason: format!("{e}"),
                })
            })
            .await
            .map_err(map_call_err)
    }

    /// Update task status to Running with a phase description.
    pub async fn update_task_running(&self, id: &TaskId, phase: &str) -> Result<(), InfraError> {
        let id_str = id.as_str().to_string();
        let phase = phase.to_string();
        let now = Utc::now().to_rfc3339();
        self.conn
            .call(move |conn| {
                conn.execute(
                    "UPDATE sync_tasks SET status = 'running', phase = ?1, updated_at = ?2 \
                     WHERE task_id = ?3",
                    params![phase, now, id_str],
                )
                .map(|_| ())
                .map_err(|e| InfraError::Store {
                    op: "update_task_running",
                    reason: format!("{e}"),
                })
            })
            .await
            .map_err(map_call_err)
    }

    /// Update task status to Completed with a serialized SyncReport.
    pub async fn update_task_completed(
        &self,
        id: &TaskId,
        report: &SyncReport,
    ) -> Result<(), InfraError> {
        let id_str = id.as_str().to_string();
        let json = serde_json::to_string(report).map_err(|e| InfraError::Store {
            op: "update_task_completed",
            reason: format!("serialize SyncReport: {e}"),
        })?;
        let now = Utc::now().to_rfc3339();
        self.conn
            .call(move |conn| {
                conn.execute(
                    "UPDATE sync_tasks SET status = 'completed', result_json = ?1, updated_at = ?2 \
                     WHERE task_id = ?3",
                    params![json, now, id_str],
                )
                .map(|_| ())
                .map_err(|e| InfraError::Store {
                    op: "update_task_completed",
                    reason: format!("{e}"),
                })
            })
            .await
            .map_err(map_call_err)
    }

    /// Update task status to Failed with an error message.
    pub async fn update_task_failed(&self, id: &TaskId, error: &str) -> Result<(), InfraError> {
        let id_str = id.as_str().to_string();
        let error = error.to_string();
        let now = Utc::now().to_rfc3339();
        self.conn
            .call(move |conn| {
                conn.execute(
                    "UPDATE sync_tasks SET status = 'failed', error = ?1, updated_at = ?2 \
                     WHERE task_id = ?3",
                    params![error, now, id_str],
                )
                .map(|_| ())
                .map_err(|e| InfraError::Store {
                    op: "update_task_failed",
                    reason: format!("{e}"),
                })
            })
            .await
            .map_err(map_call_err)
    }

    /// Load task status from DB. Returns None if task_id is unknown.
    pub async fn load_task(
        &self,
        id: &TaskId,
    ) -> Result<Option<TaskStatus<SyncReport>>, InfraError> {
        let id_str = id.as_str().to_string();
        self.conn
            .call(move |conn| {
                let mut stmt = conn
                    .prepare(
                        "SELECT status, phase, result_json, error \
                         FROM sync_tasks WHERE task_id = ?1",
                    )
                    .map_err(|e| InfraError::Store {
                        op: "load_task",
                        reason: format!("{e}"),
                    })?;

                let result = stmt
                    .query_row(params![id_str], |row| {
                        let status: String = row.get(0)?;
                        let phase: String = row.get(1)?;
                        let result_json: Option<String> = row.get(2)?;
                        let error: Option<String> = row.get(3)?;
                        Ok((status, phase, result_json, error))
                    })
                    .optional()
                    .map_err(|e| InfraError::Store {
                        op: "load_task",
                        reason: format!("{e}"),
                    })?;

                match result {
                    None => Ok(None),
                    Some((status, phase, result_json, error)) => {
                        let task_status = match status.as_str() {
                            "pending" => TaskStatus::Pending,
                            "running" => TaskStatus::Running(phase),
                            "completed" => {
                                let report: SyncReport = result_json
                                    .as_deref()
                                    .map(serde_json::from_str)
                                    .transpose()
                                    .map_err(|e| InfraError::Store {
                                        op: "load_task",
                                        reason: format!("deserialize SyncReport: {e}"),
                                    })?
                                    .unwrap_or_default();
                                TaskStatus::Completed(report)
                            }
                            "failed" => TaskStatus::Failed(error.unwrap_or_default()),
                            other => {
                                return Err(InfraError::Store {
                                    op: "load_task",
                                    reason: format!("unknown status: {other}"),
                                });
                            }
                        };
                        Ok(Some(task_status))
                    }
                }
            })
            .await
            .map_err(map_call_err)
    }

    /// On startup, mark all `running` tasks as `failed`.
    ///
    /// If a task was `running` when the process terminated, it will never
    /// reach Completed/Failed. This recovers those zombie tasks.
    pub async fn recover_stale_running(&self) -> Result<usize, InfraError> {
        let now = Utc::now().to_rfc3339();
        self.conn
            .call(move |conn| {
                let count = conn
                    .execute(
                        "UPDATE sync_tasks SET status = 'failed', \
                         error = 'session terminated while task was running', \
                         updated_at = ?1 \
                         WHERE status = 'running'",
                        params![now],
                    )
                    .map_err(|e| InfraError::Store {
                        op: "recover_stale_running",
                        reason: format!("{e}"),
                    })?;
                Ok(count)
            })
            .await
            .map_err(map_call_err)
    }
}
