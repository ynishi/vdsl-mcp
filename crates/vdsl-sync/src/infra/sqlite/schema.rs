//! SQLite schema — PRAGMA initialization and table migrations.

use rusqlite::Connection;

use crate::domain::error::SyncError;

/// Per-connection initialization: PRAGMAs that must be set on every connection.
pub(crate) fn init_connection(conn: &mut Connection) -> Result<(), SyncError> {
    conn.execute_batch(
        "PRAGMA journal_mode = WAL;
         PRAGMA foreign_keys = ON;
         PRAGMA busy_timeout = 5000;",
    )
    .map_err(|e| SyncError::Store(format!("pragma init failed: {e}")))?;
    migrate(conn)?;
    Ok(())
}

fn migrate(conn: &mut Connection) -> Result<(), SyncError> {
    conn.execute_batch(
        "
        CREATE TABLE IF NOT EXISTS sync_remotes (
            location_id TEXT PRIMARY KEY,
            backend     TEXT NOT NULL,
            remote_root TEXT NOT NULL DEFAULT '',
            config      TEXT NOT NULL DEFAULT '{}',
            created_at  TEXT NOT NULL
        );
        ",
    )
    .map_err(|e| SyncError::Store(format!("remotes migration failed: {e}")))?;

    conn.execute_batch(
        "
        CREATE TABLE IF NOT EXISTS tracked_files (
            id            TEXT PRIMARY KEY,
            relative_path TEXT UNIQUE NOT NULL,
            file_type     TEXT NOT NULL,
            file_hash     TEXT NOT NULL,
            content_hash  TEXT,
            file_size     INTEGER NOT NULL,
            embedded_id   TEXT,
            registered_at TEXT NOT NULL,
            updated_at    TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_files_hash ON tracked_files(file_hash);
        CREATE INDEX IF NOT EXISTS idx_files_content_hash ON tracked_files(content_hash);
        CREATE INDEX IF NOT EXISTS idx_files_embedded_id ON tracked_files(embedded_id);

        CREATE TABLE IF NOT EXISTS transfers (
            id          TEXT PRIMARY KEY,
            file_id     TEXT NOT NULL REFERENCES tracked_files(id) ON DELETE CASCADE,
            src         TEXT NOT NULL,
            dest        TEXT NOT NULL,
            state       TEXT NOT NULL DEFAULT 'queued',
            error       TEXT,
            error_kind  TEXT,
            attempt     INTEGER NOT NULL DEFAULT 1,
            created_at  TEXT NOT NULL,
            started_at  TEXT,
            finished_at TEXT
        );
        CREATE INDEX IF NOT EXISTS idx_transfers_dest_state ON transfers(dest, state);
        CREATE INDEX IF NOT EXISTS idx_transfers_file_dest ON transfers(file_id, dest, created_at DESC);
        CREATE INDEX IF NOT EXISTS idx_transfers_state ON transfers(state);
        ",
    )
    .map_err(|e| SyncError::Store(format!("migration failed: {e}")))?;

    Ok(())
}
