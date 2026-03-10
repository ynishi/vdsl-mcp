//! SQLite implementation of [`SyncStore`].
//!
//! Uses normalized schema: `sync_entries` + `sync_locations` + `sync_remotes`.
//! Designed for single-writer (sync engine), concurrent readers OK.
//!
//! Uses `tokio-rusqlite` for non-blocking async access — each connection
//! runs on a dedicated background thread with mpsc channel dispatch.

mod mapping;
mod schema;

use std::collections::HashMap;
use std::path::Path;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use rusqlite::params;

use crate::domain::entry::SyncEntry;
use crate::domain::error::SyncError;
use crate::domain::file_type::FileType;
use crate::domain::location::{LocationId, LocationState, LocationSummary, SyncSummary};
use crate::infra::store::{RemoteConfig, SyncStore};

use mapping::{
    parse_loc_state, query_entries, row_to_remote_tuple, save_locations, ts_to_string,
    tuple_to_remote_config,
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
// SyncStore trait implementation
// =============================================================================

#[async_trait]
impl SyncStore for SqliteSyncStore {
    async fn insert_entry(&self, entry: &SyncEntry) -> Result<(), SyncError> {
        let entry = entry.clone();
        self.conn
            .call(move |conn| {
                let file_size_i64: Option<i64> = entry
                    .file_size
                    .map(|v| {
                        i64::try_from(v).map_err(|_| {
                            SyncError::Store(format!(
                                "file_size exceeds i64::MAX: {v} (entry {})",
                                entry.id
                            ))
                        })
                    })
                    .transpose()?;
                let synced_at_str = entry.synced_at.map(ts_to_string);
                let updated_at_str = ts_to_string(entry.updated_at);
                conn.execute(
                    "INSERT INTO sync_entries (id, relative_path, file_type, file_hash, content_hash, file_size, gen_id, error, synced_at, updated_at)
                     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    params![
                        entry.id,
                        entry.relative_path,
                        entry.file_type.as_str(),
                        entry.file_hash,
                        entry.content_hash,
                        file_size_i64,
                        entry.gen_id,
                        entry.error,
                        synced_at_str,
                        updated_at_str,
                    ],
                )
                .map_err(|e| SyncError::Store(format!("insert failed: {e}")))?;

                save_locations(conn, &entry.id, &entry.locations, &updated_at_str)?;
                Ok(())
            })
            .await
            .map_err(map_call_err)
    }

    async fn update_entry(&self, entry: &SyncEntry) -> Result<(), SyncError> {
        let entry = entry.clone();
        self.conn
            .call(move |conn| {
                let file_size_i64: Option<i64> = entry
                    .file_size
                    .map(|v| {
                        i64::try_from(v).map_err(|_| {
                            SyncError::Store(format!(
                                "file_size exceeds i64::MAX: {v} (entry {})",
                                entry.id
                            ))
                        })
                    })
                    .transpose()?;
                let synced_at_str = entry.synced_at.map(ts_to_string);
                let updated_at_str = ts_to_string(entry.updated_at);
                conn.execute(
                    "UPDATE sync_entries SET file_type = ?, file_hash = ?, content_hash = ?, file_size = ?, gen_id = ?, error = ?, synced_at = ?, updated_at = ?
                     WHERE id = ?",
                    params![
                        entry.file_type.as_str(),
                        entry.file_hash,
                        entry.content_hash,
                        file_size_i64,
                        entry.gen_id,
                        entry.error,
                        synced_at_str,
                        updated_at_str,
                        entry.id,
                    ],
                )
                .map_err(|e| SyncError::Store(format!("update failed: {e}")))?;

                save_locations(conn, &entry.id, &entry.locations, &updated_at_str)?;
                Ok(())
            })
            .await
            .map_err(map_call_err)
    }

    async fn get_by_path(&self, path: &str) -> Result<Option<SyncEntry>, SyncError> {
        let path = path.to_string();
        self.conn
            .call(move |conn| {
                let entries = query_entries(
                    conn,
                    "SELECT * FROM sync_entries WHERE relative_path = ?",
                    &[&path as &dyn rusqlite::types::ToSql],
                )?;
                Ok(entries.into_iter().next())
            })
            .await
            .map_err(map_call_err)
    }

    async fn find_duplicate(
        &self,
        file_hash: &str,
        content_hash: Option<&str>,
        exclude_path: &str,
    ) -> Result<Option<SyncEntry>, SyncError> {
        let file_hash = file_hash.to_string();
        let content_hash = content_hash.map(|s| s.to_string());
        let exclude_path = exclude_path.to_string();
        self.conn
            .call(move |conn| {
                // Priority 1: content_hash match (semantic duplicate)
                if let Some(ref ch) = content_hash {
                    let entries = query_entries(
                        conn,
                        "SELECT * FROM sync_entries WHERE content_hash = ? AND relative_path != ?",
                        &[
                            ch as &dyn rusqlite::types::ToSql,
                            &exclude_path as &dyn rusqlite::types::ToSql,
                        ],
                    )?;
                    if let Some(entry) = entries.into_iter().next() {
                        return Ok(Some(entry));
                    }
                }
                // Priority 2: file_hash match (byte-exact duplicate)
                let entries = query_entries(
                    conn,
                    "SELECT * FROM sync_entries WHERE file_hash = ? AND relative_path != ?",
                    &[
                        &file_hash as &dyn rusqlite::types::ToSql,
                        &exclude_path as &dyn rusqlite::types::ToSql,
                    ],
                )?;
                Ok(entries.into_iter().next())
            })
            .await
            .map_err(map_call_err)
    }

    async fn delete_entry(&self, path: &str) -> Result<bool, SyncError> {
        let path = path.to_string();
        self.conn
            .call(move |conn| {
                let changes = conn
                    .execute(
                        "DELETE FROM sync_entries WHERE relative_path = ?",
                        params![path],
                    )
                    .map_err(|e| SyncError::Store(format!("{e}")))?;
                Ok(changes > 0)
            })
            .await
            .map_err(map_call_err)
    }

    async fn set_location_state(
        &self,
        entry_id: &str,
        location: &LocationId,
        state: LocationState,
    ) -> Result<(), SyncError> {
        let entry_id = entry_id.to_string();
        let loc_str = location.as_str().to_string();
        let state_str = state.as_str().to_string();
        self.conn
            .call(move |conn| {
                let ts = ts_to_string(chrono::Utc::now());
                conn.execute(
                    "INSERT INTO sync_locations (entry_id, location_id, state, updated_at)
                     VALUES (?, ?, ?, ?)
                     ON CONFLICT (entry_id, location_id) DO UPDATE SET state = excluded.state, updated_at = excluded.updated_at",
                    params![entry_id, loc_str, state_str, ts],
                )
                .map_err(|e| SyncError::Store(format!("{e}")))?;
                conn.execute(
                    "UPDATE sync_entries SET updated_at = ? WHERE id = ?",
                    params![ts, entry_id],
                )
                .map_err(|e| SyncError::Store(format!("{e}")))?;
                Ok(())
            })
            .await
            .map_err(map_call_err)
    }

    async fn set_error(&self, path: &str, err: Option<&str>) -> Result<(), SyncError> {
        let path = path.to_string();
        let err = err.map(|s| s.to_string());
        self.conn
            .call(move |conn| {
                let ts = ts_to_string(chrono::Utc::now());
                conn.execute(
                    "UPDATE sync_entries SET error = ?, updated_at = ? WHERE relative_path = ?",
                    params![err, ts, path],
                )
                .map_err(|e| SyncError::Store(format!("{e}")))?;
                Ok(())
            })
            .await
            .map_err(map_call_err)
    }

    async fn set_synced_at(&self, path: &str, ts: DateTime<Utc>) -> Result<(), SyncError> {
        let path = path.to_string();
        let ts = ts_to_string(ts);
        self.conn
            .call(move |conn| {
                conn.execute(
                    "UPDATE sync_entries SET synced_at = ?, updated_at = ? WHERE relative_path = ?",
                    params![ts, ts, path],
                )
                .map_err(|e| SyncError::Store(format!("{e}")))?;
                Ok(())
            })
            .await
            .map_err(map_call_err)
    }

    async fn pending(&self, dest: &LocationId) -> Result<Vec<SyncEntry>, SyncError> {
        let dest_str = dest.as_str().to_string();
        self.conn
            .call(move |conn| {
                query_entries(
                    conn,
                    "SELECT e.* FROM sync_entries e
                     INNER JOIN sync_locations l ON e.id = l.entry_id
                     WHERE l.location_id = ? AND l.state IN ('pending', 'unknown')
                     ORDER BY e.updated_at",
                    &[&dest_str as &dyn rusqlite::types::ToSql],
                )
            })
            .await
            .map_err(map_call_err)
    }

    async fn list(
        &self,
        file_type: Option<FileType>,
        limit: Option<usize>,
    ) -> Result<Vec<SyncEntry>, SyncError> {
        self.conn
            .call(move |conn| {
                let mut sql = String::from("SELECT * FROM sync_entries");
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
                query_entries(conn, &sql, &refs)
            })
            .await
            .map_err(map_call_err)
    }

    /// # Read consistency
    ///
    /// The three queries (COUNT entries, COUNT errors, GROUP BY locations) run
    /// inside a single `conn.call` closure, which executes on the dedicated
    /// background thread. Under WAL mode with a single-writer design, no
    /// concurrent writes occur during the closure, so the results are consistent.
    async fn summary(&self) -> Result<SyncSummary, SyncError> {
        self.conn
            .call(|conn| {
                let total_entries: usize = conn
                    .query_row("SELECT COUNT(*) FROM sync_entries", [], |row| row.get(0))
                    .map_err(|e| SyncError::Store(format!("{e}")))?;

                let total_errors: usize = conn
                    .query_row(
                        "SELECT COUNT(*) FROM sync_entries WHERE error IS NOT NULL",
                        [],
                        |row| row.get(0),
                    )
                    .map_err(|e| SyncError::Store(format!("{e}")))?;

                let mut stmt = conn
                    .prepare("SELECT location_id, state, COUNT(*) FROM sync_locations GROUP BY location_id, state")
                    .map_err(|e| SyncError::Store(format!("{e}")))?;

                let rows = stmt
                    .query_map([], |row| {
                        let loc: String = row.get(0)?;
                        let state: String = row.get(1)?;
                        let count: usize = row.get(2)?;
                        Ok((loc, state, count))
                    })
                    .map_err(|e| SyncError::Store(format!("{e}")))?;

                let mut locations: HashMap<LocationId, LocationSummary> = HashMap::new();
                for row in rows {
                    let (loc_str, state_str, count) =
                        row.map_err(|e| SyncError::Store(format!("{e}")))?;
                    let loc_id = LocationId::new(&loc_str).map_err(|_| {
                        SyncError::Store(format!("corrupt location_id in DB: {loc_str:?}"))
                    })?;
                    let summary = locations.entry(loc_id).or_default();
                    let state = parse_loc_state(&state_str)?;
                    match state {
                        LocationState::Present => summary.present = count,
                        LocationState::Pending => summary.pending = count,
                        LocationState::Syncing => summary.syncing = count,
                        LocationState::Unknown => summary.unknown = count,
                        LocationState::Absent => summary.absent = count,
                    }
                }

                Ok(SyncSummary {
                    locations,
                    total_entries,
                    total_errors,
                })
            })
            .await
            .map_err(map_call_err)
    }

    async fn errors(&self) -> Result<Vec<SyncEntry>, SyncError> {
        self.conn
            .call(|conn| {
                query_entries(
                    conn,
                    "SELECT * FROM sync_entries WHERE error IS NOT NULL ORDER BY updated_at DESC",
                    &[],
                )
            })
            .await
            .map_err(map_call_err)
    }

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

    fn make_entry(path: &str, ft: FileType, locs: Vec<(&str, LocationState)>) -> SyncEntry {
        let locations: HashMap<LocationId, LocationState> = locs
            .into_iter()
            .map(|(id, s)| (LocationId::new(id).expect("valid test location"), s))
            .collect();
        SyncEntry {
            id: uuid::Uuid::new_v4().to_string(),
            relative_path: path.into(),
            file_type: ft,
            file_hash: format!("fh_{}", path.replace('/', "_")),
            content_hash: None,
            file_size: None,
            gen_id: None,
            locations,
            error: None,
            synced_at: None,
            updated_at: chrono::Utc::now(),
        }
    }

    #[tokio::test]
    async fn insert_and_get() {
        let store = SqliteSyncStore::open_in_memory()
            .await
            .expect("open in-memory");
        let entry = make_entry(
            "/output/test.png",
            FileType::Image,
            vec![
                ("local", LocationState::Present),
                ("cloud", LocationState::Pending),
            ],
        );

        store.insert_entry(&entry).await.expect("insert");
        let got = store
            .get_by_path("/output/test.png")
            .await
            .expect("get")
            .expect("found");

        assert_eq!(got.relative_path, "/output/test.png");
        assert_eq!(got.file_type, FileType::Image);
        assert_eq!(
            got.location_state(&LocationId::local()),
            LocationState::Present
        );
        assert_eq!(
            got.location_state(&LocationId::new("cloud").expect("valid")),
            LocationState::Pending
        );
    }

    #[tokio::test]
    async fn get_nonexistent() {
        let store = SqliteSyncStore::open_in_memory().await.expect("open");
        let got = store.get_by_path("/no/such/file").await.expect("get");
        assert!(got.is_none());
    }

    #[tokio::test]
    async fn update_entry() {
        let store = SqliteSyncStore::open_in_memory().await.expect("open");
        let mut entry = make_entry(
            "/output/up.png",
            FileType::Image,
            vec![("local", LocationState::Present)],
        );
        store.insert_entry(&entry).await.expect("insert");

        entry.file_hash = "abc123".into();
        entry.content_hash = Some("content_abc".into());
        entry.locations.insert(
            LocationId::new("cloud").expect("valid"),
            LocationState::Pending,
        );
        store.update_entry(&entry).await.expect("update");

        let got = store
            .get_by_path("/output/up.png")
            .await
            .expect("get")
            .expect("found");
        assert_eq!(got.file_hash, "abc123");
        assert_eq!(got.content_hash.as_deref(), Some("content_abc"));
        assert_eq!(
            got.location_state(&LocationId::new("cloud").expect("valid")),
            LocationState::Pending
        );
    }

    #[tokio::test]
    async fn delete_entry() {
        let store = SqliteSyncStore::open_in_memory().await.expect("open");
        let entry = make_entry("/output/del.png", FileType::Image, vec![]);
        store.insert_entry(&entry).await.expect("insert");

        let deleted = store.delete_entry("/output/del.png").await.expect("delete");
        assert!(deleted);

        let got = store.get_by_path("/output/del.png").await.expect("get");
        assert!(got.is_none());

        let deleted2 = store
            .delete_entry("/output/del.png")
            .await
            .expect("delete2");
        assert!(!deleted2);
    }

    #[tokio::test]
    async fn duplicate_detection_by_file_hash() {
        let store = SqliteSyncStore::open_in_memory().await.expect("open");
        let mut e1 = make_entry("/output/a.png", FileType::Image, vec![]);
        e1.file_hash = "deadbeef".into();
        store.insert_entry(&e1).await.expect("insert");

        let dup = store
            .find_duplicate("deadbeef", None, "/output/b.png")
            .await
            .expect("find_duplicate");
        assert!(dup.is_some());
        assert_eq!(dup.expect("dup").relative_path, "/output/a.png");

        let no_dup = store
            .find_duplicate("deadbeef", None, "/output/a.png")
            .await
            .expect("find_duplicate");
        assert!(no_dup.is_none());
    }

    #[tokio::test]
    async fn duplicate_detection_content_hash_priority() {
        let store = SqliteSyncStore::open_in_memory().await.expect("open");
        let mut e1 = make_entry("/output/a.png", FileType::Image, vec![]);
        e1.file_hash = "file_aaa".into();
        e1.content_hash = Some("content_xxx".into());
        store.insert_entry(&e1).await.expect("insert");

        // content_hash match found even though file_hash differs
        let dup = store
            .find_duplicate("file_bbb", Some("content_xxx"), "/output/b.png")
            .await
            .expect("find_duplicate");
        assert!(dup.is_some());
        assert_eq!(dup.expect("dup").relative_path, "/output/a.png");
    }

    #[tokio::test]
    async fn set_location_state() {
        let store = SqliteSyncStore::open_in_memory().await.expect("open");
        let entry = make_entry(
            "/output/loc.png",
            FileType::Image,
            vec![("local", LocationState::Present)],
        );
        store.insert_entry(&entry).await.expect("insert");

        store
            .set_location_state(
                &entry.id,
                &LocationId::new("cloud").expect("valid"),
                LocationState::Syncing,
            )
            .await
            .expect("set_location_state");

        let got = store
            .get_by_path("/output/loc.png")
            .await
            .expect("get")
            .expect("found");
        assert_eq!(
            got.location_state(&LocationId::new("cloud").expect("valid")),
            LocationState::Syncing
        );
    }

    #[tokio::test]
    async fn pending_query() {
        let store = SqliteSyncStore::open_in_memory().await.expect("open");
        store
            .insert_entry(&make_entry(
                "/a.png",
                FileType::Image,
                vec![
                    ("local", LocationState::Present),
                    ("cloud", LocationState::Pending),
                ],
            ))
            .await
            .expect("insert a");
        store
            .insert_entry(&make_entry(
                "/b.png",
                FileType::Image,
                vec![
                    ("local", LocationState::Present),
                    ("cloud", LocationState::Present),
                ],
            ))
            .await
            .expect("insert b");
        store
            .insert_entry(&make_entry(
                "/c.png",
                FileType::Image,
                vec![
                    ("local", LocationState::Present),
                    ("cloud", LocationState::Unknown),
                ],
            ))
            .await
            .expect("insert c");

        let cloud = LocationId::new("cloud").expect("valid");
        let pending = store.pending(&cloud).await.expect("pending");
        assert_eq!(pending.len(), 2); // a (pending) + c (unknown)
    }

    #[tokio::test]
    async fn list_with_filter() {
        let store = SqliteSyncStore::open_in_memory().await.expect("open");
        store
            .insert_entry(&make_entry("/a.png", FileType::Image, vec![]))
            .await
            .expect("insert");
        store
            .insert_entry(&make_entry("/b.json", FileType::Recipe, vec![]))
            .await
            .expect("insert");
        store
            .insert_entry(&make_entry("/c.png", FileType::Image, vec![]))
            .await
            .expect("insert");

        let all = store.list(None, None).await.expect("list all");
        assert_eq!(all.len(), 3);

        let images = store
            .list(Some(FileType::Image), None)
            .await
            .expect("list images");
        assert_eq!(images.len(), 2);

        let limited = store.list(None, Some(1)).await.expect("list limited");
        assert_eq!(limited.len(), 1);
    }

    #[tokio::test]
    async fn summary() {
        let store = SqliteSyncStore::open_in_memory().await.expect("open");
        store
            .insert_entry(&make_entry(
                "/a.png",
                FileType::Image,
                vec![
                    ("local", LocationState::Present),
                    ("cloud", LocationState::Pending),
                    ("pod", LocationState::Unknown),
                ],
            ))
            .await
            .expect("insert");
        store
            .insert_entry(&make_entry(
                "/b.png",
                FileType::Image,
                vec![
                    ("local", LocationState::Present),
                    ("cloud", LocationState::Present),
                    ("pod", LocationState::Present),
                ],
            ))
            .await
            .expect("insert");

        let s = store.summary().await.expect("summary");
        assert_eq!(s.total_entries, 2);
        assert_eq!(s.total_errors, 0);

        let local_summary = s
            .locations
            .get(&LocationId::local())
            .expect("local summary");
        assert_eq!(local_summary.present, 2);

        let cloud_summary = s
            .locations
            .get(&LocationId::new("cloud").expect("valid"))
            .expect("cloud summary");
        assert_eq!(cloud_summary.present, 1);
        assert_eq!(cloud_summary.pending, 1);
    }

    #[tokio::test]
    async fn errors_query() {
        let store = SqliteSyncStore::open_in_memory().await.expect("open");
        let mut entry = make_entry("/err.png", FileType::Image, vec![]);
        entry.error = Some("connection refused".into());
        store.insert_entry(&entry).await.expect("insert");

        store
            .insert_entry(&make_entry("/ok.png", FileType::Image, vec![]))
            .await
            .expect("insert");

        let errs = store.errors().await.expect("errors");
        assert_eq!(errs.len(), 1);
        assert_eq!(errs[0].error.as_deref(), Some("connection refused"));
    }

    #[tokio::test]
    async fn set_error_and_clear() {
        let store = SqliteSyncStore::open_in_memory().await.expect("open");
        store
            .insert_entry(&make_entry("/e.png", FileType::Image, vec![]))
            .await
            .expect("insert");

        store
            .set_error("/e.png", Some("timeout"))
            .await
            .expect("set_error");
        let got = store
            .get_by_path("/e.png")
            .await
            .expect("get")
            .expect("found");
        assert_eq!(got.error.as_deref(), Some("timeout"));

        store.set_error("/e.png", None).await.expect("clear_error");
        let got2 = store
            .get_by_path("/e.png")
            .await
            .expect("get")
            .expect("found");
        assert!(got2.error.is_none());
    }

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

    #[tokio::test]
    async fn cascade_delete_locations() {
        let store = SqliteSyncStore::open_in_memory().await.expect("open");
        let entry = make_entry(
            "/cascade.png",
            FileType::Image,
            vec![
                ("local", LocationState::Present),
                ("cloud", LocationState::Pending),
            ],
        );
        let entry_id = entry.id.clone();
        store.insert_entry(&entry).await.expect("insert");

        store.delete_entry("/cascade.png").await.expect("delete");

        // Verify no orphan location rows
        let count: usize = store
            .conn
            .call(move |conn| {
                conn.query_row(
                    "SELECT COUNT(*) FROM sync_locations WHERE entry_id = ?",
                    params![entry_id],
                    |row| row.get(0),
                )
                .map_err(|e| SyncError::Store(format!("{e}")))
            })
            .await
            .expect("count");
        assert_eq!(count, 0);
    }
}
