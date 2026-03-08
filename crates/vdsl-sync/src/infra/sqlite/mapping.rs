//! SQLite ↔ domain mapping helpers.
//!
//! Converts between SQLite rows and domain types (SyncEntry, RemoteConfig, etc.).

use std::collections::HashMap;

use chrono::{DateTime, Utc};
use rusqlite::{params, Connection};

use crate::domain::entry::SyncEntry;
use crate::domain::error::SyncError;
use crate::domain::file_type::FileType;
use crate::domain::location::{LocationId, LocationState};
use crate::infra::store::RemoteConfig;

/// Format a DateTime<Utc> as RFC 3339 string for SQLite storage.
///
/// Produces `YYYY-MM-DDTHH:MM:SSZ` (no sub-seconds, always 'Z' suffix).
pub(crate) fn ts_to_string(dt: DateTime<Utc>) -> String {
    dt.to_rfc3339_opts(chrono::SecondsFormat::Secs, true)
}

/// Parse an RFC 3339 string from SQLite into DateTime<Utc>.
pub(crate) fn parse_ts(s: &str) -> Result<DateTime<Utc>, SyncError> {
    DateTime::parse_from_rfc3339(s)
        .map(|dt| dt.with_timezone(&Utc))
        .map_err(|_| SyncError::Store(format!("corrupt timestamp in DB: {s:?}")))
}

/// Intermediate row struct (before location hydration).
pub(crate) struct EntryRow {
    pub id: String,
    pub relative_path: String,
    pub file_type_str: String,
    pub file_hash: String,
    pub content_hash: Option<String>,
    /// Stored as INTEGER (i64) in SQLite, converted to u64 in domain layer.
    pub file_size_raw: Option<i64>,
    pub gen_id: Option<String>,
    pub error: Option<String>,
    pub synced_at: Option<String>,
    pub updated_at: String,
}

pub(crate) fn parse_loc_state(val: &str) -> Result<LocationState, SyncError> {
    val.parse()
        .map_err(|_| SyncError::Store(format!("corrupt location state in DB: {val:?}")))
}

pub(crate) fn row_to_entry(row: &rusqlite::Row<'_>) -> rusqlite::Result<EntryRow> {
    Ok(EntryRow {
        id: row.get("id")?,
        relative_path: row.get("relative_path")?,
        file_type_str: row.get("file_type")?,
        file_hash: row.get("file_hash")?,
        content_hash: row.get("content_hash")?,
        file_size_raw: row.get("file_size")?,
        gen_id: row.get("gen_id")?,
        error: row.get("error")?,
        synced_at: row.get("synced_at")?,
        updated_at: row.get("updated_at")?,
    })
}

pub(crate) fn build_entry(
    r: EntryRow,
    locations: HashMap<LocationId, LocationState>,
) -> Result<SyncEntry, SyncError> {
    let file_type: FileType = r.file_type_str.parse().map_err(|_| {
        SyncError::Store(format!(
            "corrupt file_type in DB: {:?} (entry {})",
            r.file_type_str, r.id
        ))
    })?;
    let synced_at = r.synced_at.as_deref().map(parse_ts).transpose()?;
    let updated_at = parse_ts(&r.updated_at)?;
    let file_size = r
        .file_size_raw
        .map(|v| {
            u64::try_from(v).map_err(|_| {
                SyncError::Store(format!("corrupt file_size in DB: {v} (entry {})", r.id))
            })
        })
        .transpose()?;
    Ok(SyncEntry {
        id: r.id,
        relative_path: r.relative_path,
        file_type,
        file_hash: r.file_hash,
        content_hash: r.content_hash,
        file_size,
        gen_id: r.gen_id,
        locations,
        error: r.error,
        synced_at,
        updated_at,
    })
}

/// Maximum number of SQL parameters per batch query.
///
/// SQLite default `SQLITE_MAX_VARIABLE_NUMBER` is 999.
/// Use a conservative chunk size to stay well within limits.
const BATCH_CHUNK_SIZE: usize = 500;

/// Batch-load locations for multiple entry IDs, chunked to respect
/// SQLite's `SQLITE_MAX_VARIABLE_NUMBER` limit.
///
/// Returns a map of entry_id -> (LocationId -> LocationState).
pub(crate) fn load_locations_batch(
    conn: &Connection,
    entry_ids: &[&str],
) -> Result<HashMap<String, HashMap<LocationId, LocationState>>, SyncError> {
    if entry_ids.is_empty() {
        return Ok(HashMap::new());
    }

    let mut result: HashMap<String, HashMap<LocationId, LocationState>> = HashMap::new();

    for chunk in entry_ids.chunks(BATCH_CHUNK_SIZE) {
        let placeholders: Vec<&str> = chunk.iter().map(|_| "?").collect();
        let sql = format!(
            "SELECT entry_id, location_id, state FROM sync_locations WHERE entry_id IN ({})",
            placeholders.join(",")
        );

        let mut stmt = conn
            .prepare(&sql)
            .map_err(|e| SyncError::Store(format!("{e}")))?;

        let params: Vec<&dyn rusqlite::types::ToSql> = chunk
            .iter()
            .map(|id| id as &dyn rusqlite::types::ToSql)
            .collect();

        let rows = stmt
            .query_map(params.as_slice(), |row| {
                let eid: String = row.get(0)?;
                let loc_str: String = row.get(1)?;
                let state_str: String = row.get(2)?;
                Ok((eid, loc_str, state_str))
            })
            .map_err(|e| SyncError::Store(format!("{e}")))?;

        for row in rows {
            let (eid, loc_str, state_str) = row.map_err(|e| SyncError::Store(format!("{e}")))?;
            let loc_id = LocationId::new(&loc_str)
                .map_err(|_| SyncError::Store(format!("corrupt location_id in DB: {loc_str:?}")))?;
            let state = parse_loc_state(&state_str)?;
            result.entry(eid).or_default().insert(loc_id, state);
        }
    }

    Ok(result)
}

pub(crate) fn query_entries(
    conn: &Connection,
    sql: &str,
    params: &[&dyn rusqlite::types::ToSql],
) -> Result<Vec<SyncEntry>, SyncError> {
    let mut stmt = conn
        .prepare(sql)
        .map_err(|e| SyncError::Store(format!("{e}")))?;
    let rows = stmt
        .query_map(params, row_to_entry)
        .map_err(|e| SyncError::Store(format!("{e}")))?;

    let mut entry_rows = Vec::new();
    for row in rows {
        let r = row.map_err(|e| SyncError::Store(format!("{e}")))?;
        entry_rows.push(r);
    }

    let ids: Vec<&str> = entry_rows.iter().map(|r| r.id.as_str()).collect();
    let mut locations_map = load_locations_batch(conn, &ids)?;

    let mut entries = Vec::with_capacity(entry_rows.len());
    for r in entry_rows {
        let locations = match locations_map.remove(&r.id) {
            Some(locs) => locs,
            None => {
                tracing::warn!(
                    entry_id = %r.id,
                    relative_path = %r.relative_path,
                    "no location data found in sync_locations table — possible DB integrity issue"
                );
                HashMap::new()
            }
        };
        entries.push(build_entry(r, locations)?);
    }

    Ok(entries)
}

/// Extract raw remote row fields from a rusqlite row.
pub(crate) fn row_to_remote_tuple(
    row: &rusqlite::Row<'_>,
) -> rusqlite::Result<(String, String, String, String, String)> {
    Ok((
        row.get(0)?,
        row.get(1)?,
        row.get(2)?,
        row.get(3)?,
        row.get(4)?,
    ))
}

/// Build a `RemoteConfig` from raw SQLite row tuple.
pub(crate) fn tuple_to_remote_config(
    loc_str: String,
    backend: String,
    remote_root: String,
    config_str: String,
    created_at_str: String,
) -> Result<RemoteConfig, SyncError> {
    let loc_id = LocationId::new(&loc_str).map_err(|_| {
        SyncError::Store(format!("corrupt location_id in sync_remotes: {loc_str:?}"))
    })?;
    let config: serde_json::Value = serde_json::from_str(&config_str).map_err(|e| {
        SyncError::Store(format!(
            "corrupt config JSON in sync_remotes for {loc_str:?}: {e}"
        ))
    })?;
    let created_at = parse_ts(&created_at_str)?;
    Ok(RemoteConfig {
        location_id: loc_id,
        backend,
        remote_root,
        config,
        created_at,
    })
}

/// Persist location states for an entry.
///
/// Uses DELETE + INSERT (not UPSERT) to guarantee that locations removed
/// from `entry.locations` are also removed from the DB, preventing orphaned
/// rows. An UPSERT approach would require a separate DELETE for stale rows,
/// adding complexity without meaningful performance gain at the expected
/// scale (typically < 10 locations per entry).
pub(crate) fn save_locations(
    conn: &Connection,
    entry_id: &str,
    locations: &HashMap<LocationId, LocationState>,
    ts: &str,
) -> Result<(), SyncError> {
    conn.execute(
        "DELETE FROM sync_locations WHERE entry_id = ?",
        params![entry_id],
    )
    .map_err(|e| SyncError::Store(format!("delete locations failed: {e}")))?;

    let mut stmt = conn
        .prepare(
            "INSERT INTO sync_locations (entry_id, location_id, state, updated_at)
             VALUES (?, ?, ?, ?)",
        )
        .map_err(|e| SyncError::Store(format!("{e}")))?;

    for (loc, state) in locations {
        stmt.execute(params![entry_id, loc.as_str(), state.as_str(), ts])
            .map_err(|e| SyncError::Store(format!("{e}")))?;
    }
    Ok(())
}
