//! SQLite schema — PRAGMA initialization and table definitions.

use rusqlite::Connection;

use crate::application::error::SyncError;
use crate::infra::error::InfraError;

/// Per-connection initialization: PRAGMAs that must be set on every connection.
pub(crate) fn init_connection(conn: &mut Connection) -> Result<(), SyncError> {
    conn.execute_batch(
        "PRAGMA journal_mode = WAL;
         PRAGMA foreign_keys = ON;
         PRAGMA busy_timeout = 5000;",
    )
    .map_err(|e| InfraError::Store {
        op: "sqlite",
        reason: format!("pragma init failed: {e}"),
    })?;
    create_tables(conn)?;
    Ok(())
}

fn create_tables(conn: &mut Connection) -> Result<(), SyncError> {
    conn.execute_batch(
        "
        CREATE TABLE IF NOT EXISTS sync_remotes (
            location_id TEXT PRIMARY KEY,
            backend     TEXT NOT NULL,
            remote_root TEXT NOT NULL DEFAULT '',
            config      TEXT NOT NULL DEFAULT '{}',
            created_at  TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS tracked_files (
            id            TEXT PRIMARY KEY,
            relative_path TEXT UNIQUE NOT NULL,
            file_type     TEXT NOT NULL,
            file_hash     TEXT NOT NULL,
            content_hash  TEXT,
            file_size     INTEGER NOT NULL,
            embedded_id   TEXT,
            deleted_at    TEXT,
            modified_at   TEXT,
            registered_at TEXT NOT NULL,
            updated_at    TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_files_hash ON tracked_files(file_hash);
        CREATE INDEX IF NOT EXISTS idx_files_content_hash ON tracked_files(content_hash);
        CREATE INDEX IF NOT EXISTS idx_files_embedded_id ON tracked_files(embedded_id);

        CREATE TABLE IF NOT EXISTS topology_files (
            id              TEXT PRIMARY KEY,
            relative_path   TEXT NOT NULL,
            canonical_hash  TEXT,
            file_type       TEXT NOT NULL,
            registered_at   TEXT NOT NULL,
            deleted_at      TEXT
        );
        CREATE UNIQUE INDEX IF NOT EXISTS idx_topology_files_path
            ON topology_files(relative_path) WHERE deleted_at IS NULL;
        CREATE INDEX IF NOT EXISTS idx_topology_files_canonical_hash
            ON topology_files(canonical_hash) WHERE deleted_at IS NULL;

        CREATE TABLE IF NOT EXISTS location_files (
            file_id         TEXT NOT NULL,
            location_id     TEXT NOT NULL,
            relative_path   TEXT NOT NULL,
            file_hash       TEXT,
            content_hash    TEXT,
            meta_hash       TEXT,
            size            INTEGER NOT NULL,
            modified_at     TEXT,
            state           TEXT NOT NULL DEFAULT 'active',
            embedded_id     TEXT,
            updated_at      TEXT NOT NULL,
            PRIMARY KEY (file_id, location_id)
        );
        CREATE INDEX IF NOT EXISTS idx_location_files_location
            ON location_files(location_id);
        CREATE INDEX IF NOT EXISTS idx_location_files_state
            ON location_files(file_id, state);

        CREATE TABLE IF NOT EXISTS transfers (
            id          TEXT PRIMARY KEY,
            file_id     TEXT NOT NULL REFERENCES topology_files(id) ON DELETE CASCADE,
            src         TEXT NOT NULL,
            dest        TEXT NOT NULL,
            kind        TEXT NOT NULL DEFAULT 'sync',
            state       TEXT NOT NULL DEFAULT 'queued',
            error       TEXT,
            error_kind  TEXT,
            attempt     INTEGER NOT NULL DEFAULT 1,
            depends_on  TEXT,
            created_at  TEXT NOT NULL,
            started_at  TEXT,
            finished_at TEXT
        );
        CREATE INDEX IF NOT EXISTS idx_transfers_dest_state ON transfers(dest, state);
        CREATE INDEX IF NOT EXISTS idx_transfers_file_dest ON transfers(file_id, dest, created_at DESC);
        CREATE INDEX IF NOT EXISTS idx_transfers_state ON transfers(state);
        CREATE INDEX IF NOT EXISTS idx_transfers_depends_on ON transfers(depends_on);
        ",
    )
    .map_err(|e| InfraError::Store {
        op: "sqlite",
        reason: format!("table creation failed: {e}"),
    })?;

    Ok(())
}
