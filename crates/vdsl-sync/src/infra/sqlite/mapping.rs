//! SQLite ↔ domain mapping helpers.
//!
//! Converts between SQLite rows and domain types (TrackedFile, Transfer, RemoteConfig).

use chrono::{DateTime, Utc};
use rusqlite::Connection;

use crate::domain::error::SyncError;
use crate::domain::file_type::FileType;
use crate::domain::location::LocationId;
use crate::domain::retry::TransferErrorKind;
use crate::domain::tracked_file::TrackedFile;
use crate::domain::transfer::{Transfer, TransferState};
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

// =============================================================================
// RemoteConfig mapping
// =============================================================================

/// Extract raw remote row fields from a rusqlite row.
///
/// SELECT order: location_id, backend, config, created_at
pub(crate) fn row_to_remote_tuple(
    row: &rusqlite::Row<'_>,
) -> rusqlite::Result<(String, String, String, String)> {
    Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?))
}

/// Build a `RemoteConfig` from raw SQLite row tuple.
pub(crate) fn tuple_to_remote_config(
    loc_str: String,
    backend: String,
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
        config,
        created_at,
    })
}

// =============================================================================
// TrackedFile mapping
// =============================================================================

/// TrackedFile row intermediate struct.
pub(crate) struct TrackedFileRow {
    pub id: String,
    pub relative_path: String,
    pub file_type_str: String,
    pub file_hash: String,
    pub content_hash: Option<String>,
    pub file_size_raw: i64,
    pub embedded_id: Option<String>,
    pub registered_at: String,
    pub updated_at: String,
}

pub(crate) fn row_to_tracked_file_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<TrackedFileRow> {
    Ok(TrackedFileRow {
        id: row.get("id")?,
        relative_path: row.get("relative_path")?,
        file_type_str: row.get("file_type")?,
        file_hash: row.get("file_hash")?,
        content_hash: row.get("content_hash")?,
        file_size_raw: row.get("file_size")?,
        embedded_id: row.get("embedded_id")?,
        registered_at: row.get("registered_at")?,
        updated_at: row.get("updated_at")?,
    })
}

pub(crate) fn build_tracked_file(r: TrackedFileRow) -> Result<TrackedFile, SyncError> {
    let file_type: FileType = r.file_type_str.parse().map_err(|_| {
        SyncError::Store(format!(
            "corrupt file_type in tracked_files: {:?} (id {})",
            r.file_type_str, r.id
        ))
    })?;
    let file_size = u64::try_from(r.file_size_raw).map_err(|_| {
        SyncError::Store(format!(
            "corrupt file_size in tracked_files: {} (id {})",
            r.file_size_raw, r.id
        ))
    })?;
    let registered_at = parse_ts(&r.registered_at)?;
    let updated_at = parse_ts(&r.updated_at)?;

    Ok(TrackedFile::reconstitute(
        r.id,
        r.relative_path,
        file_type,
        r.file_hash,
        r.content_hash,
        file_size,
        r.embedded_id,
        registered_at,
        updated_at,
    ))
}

pub(crate) fn query_tracked_files(
    conn: &Connection,
    sql: &str,
    params: &[&dyn rusqlite::types::ToSql],
) -> Result<Vec<TrackedFile>, SyncError> {
    let mut stmt = conn
        .prepare(sql)
        .map_err(|e| SyncError::Store(format!("{e}")))?;
    let rows = stmt
        .query_map(params, row_to_tracked_file_row)
        .map_err(|e| SyncError::Store(format!("{e}")))?;

    let mut files = Vec::new();
    for row in rows {
        let r = row.map_err(|e| SyncError::Store(format!("{e}")))?;
        files.push(build_tracked_file(r)?);
    }
    Ok(files)
}

// =============================================================================
// Transfer mapping
// =============================================================================

/// Transfer row intermediate struct.
pub(crate) struct TransferRow {
    pub id: String,
    pub file_id: String,
    pub src: String,
    pub dest: String,
    pub state: String,
    pub error: Option<String>,
    pub error_kind: Option<String>,
    pub attempt: i64,
    pub created_at: String,
    pub started_at: Option<String>,
    pub finished_at: Option<String>,
}

pub(crate) fn row_to_transfer_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<TransferRow> {
    Ok(TransferRow {
        id: row.get("id")?,
        file_id: row.get("file_id")?,
        src: row.get("src")?,
        dest: row.get("dest")?,
        state: row.get("state")?,
        error: row.get("error")?,
        error_kind: row.get("error_kind")?,
        attempt: row.get("attempt")?,
        created_at: row.get("created_at")?,
        started_at: row.get("started_at")?,
        finished_at: row.get("finished_at")?,
    })
}

pub(crate) fn build_transfer(r: TransferRow) -> Result<Transfer, SyncError> {
    let src = LocationId::new(&r.src).map_err(|_| {
        SyncError::Store(format!(
            "corrupt src in transfers: {:?} (id {})",
            r.src, r.id
        ))
    })?;
    let dest = LocationId::new(&r.dest).map_err(|_| {
        SyncError::Store(format!(
            "corrupt dest in transfers: {:?} (id {})",
            r.dest, r.id
        ))
    })?;
    let state: TransferState = r.state.parse().map_err(|_| {
        SyncError::Store(format!(
            "corrupt state in transfers: {:?} (id {})",
            r.state, r.id
        ))
    })?;
    let error_kind: Option<TransferErrorKind> = r
        .error_kind
        .as_deref()
        .map(|s| {
            s.parse::<TransferErrorKind>().map_err(|_| {
                SyncError::Store(format!(
                    "corrupt error_kind in transfers: {:?} (id {})",
                    s, r.id
                ))
            })
        })
        .transpose()?;
    let attempt = u32::try_from(r.attempt).map_err(|_| {
        SyncError::Store(format!(
            "corrupt attempt in transfers: {} (id {})",
            r.attempt, r.id
        ))
    })?;
    let created_at = parse_ts(&r.created_at)?;
    let started_at = r.started_at.as_deref().map(parse_ts).transpose()?;
    let finished_at = r.finished_at.as_deref().map(parse_ts).transpose()?;

    Ok(Transfer::reconstitute(
        r.id,
        r.file_id,
        src,
        dest,
        state,
        r.error,
        error_kind,
        attempt,
        created_at,
        started_at,
        finished_at,
    ))
}

pub(crate) fn query_transfers(
    conn: &Connection,
    sql: &str,
    params: &[&dyn rusqlite::types::ToSql],
) -> Result<Vec<Transfer>, SyncError> {
    let mut stmt = conn
        .prepare(sql)
        .map_err(|e| SyncError::Store(format!("{e}")))?;
    let rows = stmt
        .query_map(params, row_to_transfer_row)
        .map_err(|e| SyncError::Store(format!("{e}")))?;

    let mut transfers = Vec::new();
    for row in rows {
        let r = row.map_err(|e| SyncError::Store(format!("{e}")))?;
        transfers.push(build_transfer(r)?);
    }
    Ok(transfers)
}
