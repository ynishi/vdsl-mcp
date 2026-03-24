//! SQLite ↔ domain mapping helpers.
//!
//! Converts between SQLite rows and domain types (Transfer, TopologyFile, LocationFile).

use chrono::{DateTime, Utc};
use rusqlite::Connection;

use crate::application::error::SyncError;
use crate::domain::file_type::FileType;
use crate::domain::fingerprint::FileFingerprint;
use crate::domain::location::LocationId;
use crate::domain::location_file::{LocationFile, LocationFileState};
use crate::domain::retry::TransferErrorKind;
use crate::domain::topology_file::TopologyFile;
use crate::domain::transfer::{Transfer, TransferKind, TransferState};
use crate::infra::error::InfraError;

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
        .map_err(|_| {
            InfraError::Store {
                op: "sqlite",
                reason: format!("corrupt timestamp in DB: {s:?}"),
            }
            .into()
        })
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
    pub kind: Option<String>,
    pub state: String,
    pub error: Option<String>,
    pub error_kind: Option<String>,
    pub attempt: i64,
    pub created_at: String,
    pub started_at: Option<String>,
    pub finished_at: Option<String>,
    pub depends_on: Option<String>,
}

pub(crate) fn row_to_transfer_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<TransferRow> {
    Ok(TransferRow {
        id: row.get("id")?,
        file_id: row.get("file_id")?,
        src: row.get("src")?,
        dest: row.get("dest")?,
        kind: row.get("kind")?,
        state: row.get("state")?,
        error: row.get("error")?,
        error_kind: row.get("error_kind")?,
        attempt: row.get("attempt")?,
        created_at: row.get("created_at")?,
        started_at: row.get("started_at")?,
        finished_at: row.get("finished_at")?,
        depends_on: row.get("depends_on")?,
    })
}

pub(crate) fn build_transfer(r: TransferRow) -> Result<Transfer, SyncError> {
    let src = LocationId::new(&r.src).map_err(|_| InfraError::Store {
        op: "sqlite",
        reason: format!("corrupt src in transfers: {:?} (id {})", r.src, r.id),
    })?;
    let dest = LocationId::new(&r.dest).map_err(|_| InfraError::Store {
        op: "sqlite",
        reason: format!("corrupt dest in transfers: {:?} (id {})", r.dest, r.id),
    })?;
    let kind: TransferKind = r
        .kind
        .as_deref()
        .unwrap_or("sync")
        .parse()
        .unwrap_or(TransferKind::Sync);
    let state: TransferState = r.state.parse().map_err(|_| InfraError::Store {
        op: "sqlite",
        reason: format!("corrupt state in transfers: {:?} (id {})", r.state, r.id),
    })?;
    let error_kind: Option<TransferErrorKind> = r
        .error_kind
        .as_deref()
        .map(|s| {
            s.parse::<TransferErrorKind>()
                .map_err(|_| InfraError::Store {
                    op: "sqlite",
                    reason: format!("corrupt error_kind in transfers: {:?} (id {})", s, r.id),
                })
        })
        .transpose()?;
    let attempt = u32::try_from(r.attempt).map_err(|_| InfraError::Store {
        op: "sqlite",
        reason: format!("corrupt attempt in transfers: {} (id {})", r.attempt, r.id),
    })?;
    let created_at = parse_ts(&r.created_at)?;
    let started_at = r.started_at.as_deref().map(parse_ts).transpose()?;
    let finished_at = r.finished_at.as_deref().map(parse_ts).transpose()?;

    Ok(Transfer::reconstitute_with_dependency(
        r.id,
        r.file_id,
        src,
        dest,
        kind,
        state,
        r.error,
        error_kind,
        attempt,
        created_at,
        started_at,
        finished_at,
        r.depends_on,
    ))
}

pub(crate) fn query_transfers(
    conn: &Connection,
    sql: &str,
    params: &[&dyn rusqlite::types::ToSql],
) -> Result<Vec<Transfer>, SyncError> {
    let mut stmt = conn.prepare(sql).map_err(|e| InfraError::Store {
        op: "sqlite",
        reason: format!("{e}"),
    })?;
    let rows = stmt
        .query_map(params, row_to_transfer_row)
        .map_err(|e| InfraError::Store {
            op: "sqlite",
            reason: format!("{e}"),
        })?;

    let mut transfers = Vec::new();
    for row in rows {
        let r = row.map_err(|e| InfraError::Store {
            op: "sqlite",
            reason: format!("{e}"),
        })?;
        transfers.push(build_transfer(r)?);
    }
    Ok(transfers)
}

// =============================================================================
// TopologyFile mapping
// =============================================================================

pub(crate) struct TopologyFileRow {
    pub id: String,
    pub relative_path: String,
    pub canonical_hash: Option<String>,
    pub file_type_str: String,
    pub registered_at: String,
    pub deleted_at: Option<String>,
}

pub(crate) fn row_to_topology_file_row(
    row: &rusqlite::Row<'_>,
) -> rusqlite::Result<TopologyFileRow> {
    Ok(TopologyFileRow {
        id: row.get("id")?,
        relative_path: row.get("relative_path")?,
        canonical_hash: row.get("canonical_hash")?,
        file_type_str: row.get("file_type")?,
        registered_at: row.get("registered_at")?,
        deleted_at: row.get("deleted_at")?,
    })
}

pub(crate) fn build_topology_file(r: TopologyFileRow) -> Result<TopologyFile, SyncError> {
    let file_type: FileType = r.file_type_str.parse().map_err(|_| InfraError::Store {
        op: "sqlite",
        reason: format!(
            "corrupt file_type in topology_files: {:?} (id {})",
            r.file_type_str, r.id
        ),
    })?;
    let registered_at = parse_ts(&r.registered_at)?;
    let deleted_at = r.deleted_at.as_deref().map(parse_ts).transpose()?;

    use crate::domain::digest::ContentDigest;
    Ok(TopologyFile::reconstitute(
        r.id,
        r.relative_path,
        r.canonical_hash.map(ContentDigest),
        file_type,
        registered_at,
        deleted_at,
    ))
}

pub(crate) fn query_topology_files(
    conn: &Connection,
    sql: &str,
    params: &[&dyn rusqlite::types::ToSql],
) -> Result<Vec<TopologyFile>, SyncError> {
    let mut stmt = conn.prepare(sql).map_err(|e| InfraError::Store {
        op: "sqlite",
        reason: format!("{e}"),
    })?;
    let rows = stmt
        .query_map(params, row_to_topology_file_row)
        .map_err(|e| InfraError::Store {
            op: "sqlite",
            reason: format!("{e}"),
        })?;

    let mut files = Vec::new();
    for row in rows {
        let r = row.map_err(|e| InfraError::Store {
            op: "sqlite",
            reason: format!("{e}"),
        })?;
        files.push(build_topology_file(r)?);
    }
    Ok(files)
}

// =============================================================================
// LocationFile mapping
// =============================================================================

pub(crate) struct LocationFileRow {
    pub file_id: String,
    pub location_id: String,
    pub relative_path: String,
    pub file_hash: Option<String>,
    pub content_hash: Option<String>,
    pub meta_hash: Option<String>,
    pub size: i64,
    pub modified_at: Option<String>,
    pub state: String,
    pub embedded_id: Option<String>,
    pub updated_at: String,
}

pub(crate) fn row_to_location_file_row(
    row: &rusqlite::Row<'_>,
) -> rusqlite::Result<LocationFileRow> {
    Ok(LocationFileRow {
        file_id: row.get("file_id")?,
        location_id: row.get("location_id")?,
        relative_path: row.get("relative_path")?,
        file_hash: row.get("file_hash")?,
        content_hash: row.get("content_hash")?,
        meta_hash: row.get("meta_hash")?,
        size: row.get("size")?,
        modified_at: row.get("modified_at")?,
        state: row.get("state")?,
        embedded_id: row.get("embedded_id")?,
        updated_at: row.get("updated_at")?,
    })
}

pub(crate) fn build_location_file(r: LocationFileRow) -> Result<LocationFile, SyncError> {
    let location_id = LocationId::new(&r.location_id).map_err(|_| InfraError::Store {
        op: "sqlite",
        reason: format!(
            "corrupt location_id in location_files: {:?} (file_id {})",
            r.location_id, r.file_id
        ),
    })?;
    let state: LocationFileState = r.state.parse().map_err(|_| InfraError::Store {
        op: "sqlite",
        reason: format!(
            "corrupt state in location_files: {:?} (file_id {})",
            r.state, r.file_id
        ),
    })?;
    let size = u64::try_from(r.size).map_err(|_| InfraError::Store {
        op: "sqlite",
        reason: format!(
            "corrupt size in location_files: {} (file_id {})",
            r.size, r.file_id
        ),
    })?;
    let modified_at = r.modified_at.as_deref().map(parse_ts).transpose()?;
    let updated_at = parse_ts(&r.updated_at)?;

    use crate::domain::digest::{ByteDigest, ContentDigest, MetaDigest};
    let fingerprint = FileFingerprint {
        byte_digest: r.file_hash.as_deref().map(ByteDigest::parse),
        content_digest: r.content_hash.map(ContentDigest),
        meta_digest: r.meta_hash.map(MetaDigest),
        size,
        modified_at,
    };

    Ok(LocationFile::reconstitute(
        r.file_id,
        location_id,
        r.relative_path,
        fingerprint,
        state,
        r.embedded_id,
        updated_at,
    ))
}

pub(crate) fn query_location_files(
    conn: &Connection,
    sql: &str,
    params: &[&dyn rusqlite::types::ToSql],
) -> Result<Vec<LocationFile>, SyncError> {
    let mut stmt = conn.prepare(sql).map_err(|e| InfraError::Store {
        op: "sqlite",
        reason: format!("{e}"),
    })?;
    let rows = stmt
        .query_map(params, row_to_location_file_row)
        .map_err(|e| InfraError::Store {
            op: "sqlite",
            reason: format!("{e}"),
        })?;

    let mut files = Vec::new();
    for row in rows {
        let r = row.map_err(|e| InfraError::Store {
            op: "sqlite",
            reason: format!("{e}"),
        })?;
        files.push(build_location_file(r)?);
    }
    Ok(files)
}
