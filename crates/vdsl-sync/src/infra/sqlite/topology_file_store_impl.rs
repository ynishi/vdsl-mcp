//! TopologyFileStore trait implementation for SqliteSyncStore.

use async_trait::async_trait;
use rusqlite::params;

use crate::domain::file_type::FileType;
use crate::domain::topology_file::TopologyFile;
use crate::infra::error::InfraError;
use crate::infra::topology_file_store::TopologyFileStore;

use super::mapping::{query_topology_files, ts_to_string};
use super::{map_call_err, SqliteSyncStore};

#[async_trait]
impl TopologyFileStore for SqliteSyncStore {
    async fn upsert(&self, file: &TopologyFile) -> Result<(), InfraError> {
        let file = file.clone();
        self.conn
            .call(move |conn| {
                let registered_at_str = ts_to_string(file.registered_at());
                let deleted_at_str = file.deleted_at().map(ts_to_string);
                let params = params![
                    file.id(),
                    file.relative_path(),
                    file.canonical_hash(),
                    file.file_type().as_str(),
                    registered_at_str,
                    deleted_at_str,
                ];

                let result = conn.execute(
                    "INSERT INTO topology_files (id, relative_path, canonical_hash, file_type, registered_at, deleted_at)
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6)
                     ON CONFLICT (id) DO UPDATE SET
                         relative_path = excluded.relative_path,
                         canonical_hash = excluded.canonical_hash,
                         file_type = excluded.file_type,
                         registered_at = excluded.registered_at,
                         deleted_at = excluded.deleted_at",
                    params,
                );

                match result {
                    Ok(_) => Ok(()),
                    Err(rusqlite::Error::SqliteFailure(err, _))
                        if err.extended_code == rusqlite::ffi::SQLITE_CONSTRAINT_UNIQUE =>
                    {
                        // path衝突: 同一relative_pathの別IDが存在。
                        // 既存レコードをsoft delete（Rename後の旧pathに新ファイルが来た等）。
                        tracing::warn!(
                            id = file.id(),
                            path = file.relative_path(),
                            "topology_file upsert: path conflict — retiring existing record"
                        );
                        let now_rfc3339 = ts_to_string(chrono::Utc::now());
                        conn.execute(
                            "UPDATE topology_files SET deleted_at = ?
                             WHERE relative_path = ?2 AND id != ?3 AND deleted_at IS NULL",
                            params![now_rfc3339, file.relative_path(), file.id()],
                        )
                        .map_err(|e| InfraError::Store {
                            op: "sqlite",
                            reason: format!("retire conflicting topology_file failed: {e}"),
                        })?;

                        // リトライ
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
                            reason: format!("upsert topology_file retry failed: {e}"),
                        })?;
                        Ok(())
                    }
                    Err(e) => Err(InfraError::Store {
                        op: "sqlite",
                        reason: format!("upsert topology_file failed: {e}"),
                    }),
                }
            })
            .await
            .map_err(map_call_err)
    }

    async fn get_by_id(&self, id: &str) -> Result<Option<TopologyFile>, InfraError> {
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

    async fn get_by_path(&self, relative_path: &str) -> Result<Option<TopologyFile>, InfraError> {
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

    async fn find_by_canonical_hash(&self, hash: &str) -> Result<Option<TopologyFile>, InfraError> {
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
    ) -> Result<Vec<TopologyFile>, InfraError> {
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

    async fn list_deleted(&self) -> Result<Vec<TopologyFile>, InfraError> {
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

    async fn hard_delete(&self, id: &str) -> Result<bool, InfraError> {
        let id = id.to_string();
        self.conn
            .call(move |conn| {
                let deleted = conn
                    .execute(
                        "DELETE FROM topology_files WHERE id = ? AND deleted_at IS NOT NULL",
                        params![id],
                    )
                    .map_err(|e| InfraError::Store {
                        op: "sqlite",
                        reason: format!("hard_delete topology_file failed: {e}"),
                    })?;
                Ok(deleted > 0)
            })
            .await
            .map_err(map_call_err)
    }

    async fn count_active(&self) -> Result<usize, InfraError> {
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

    async fn list_active_paths(&self) -> Result<Vec<String>, InfraError> {
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
