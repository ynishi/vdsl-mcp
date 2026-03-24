//! LocationFileStore trait implementation for SqliteSyncStore.

use async_trait::async_trait;
use rusqlite::params;

use crate::domain::location::LocationId;
use crate::domain::location_file::LocationFile;
use crate::infra::error::InfraError;
use crate::infra::location_file_store::LocationFileStore;

use super::mapping::{query_location_files, ts_to_string};
use super::{map_call_err, SqliteSyncStore};

#[async_trait]
impl LocationFileStore for SqliteSyncStore {
    async fn upsert(&self, file: &LocationFile) -> Result<(), InfraError> {
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
                        file.fingerprint().byte_digest.as_ref().map(|d| d.to_prefixed_string()),
                        file.fingerprint().content_digest.as_ref().map(|d| d.0.clone()),
                        file.fingerprint().meta_digest.as_ref().map(|d| d.0.clone()),
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
    ) -> Result<Option<LocationFile>, InfraError> {
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

    async fn list_by_file(&self, file_id: &str) -> Result<Vec<LocationFile>, InfraError> {
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
    ) -> Result<Vec<LocationFile>, InfraError> {
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
    ) -> Result<std::collections::HashMap<String, Vec<LocationFile>>, InfraError> {
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

    async fn delete(&self, file_id: &str, location_id: &LocationId) -> Result<bool, InfraError> {
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

    async fn count_by_location(&self, location_id: &LocationId) -> Result<usize, InfraError> {
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
