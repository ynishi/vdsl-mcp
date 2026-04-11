//! SQLite schema — PRAGMA initialization, versioning, and table definitions.
//!
//! Schema versioning uses `PRAGMA user_version`. The current schema is
//! `SCHEMA_VERSION`. On open, `init_connection` runs all PRAGMAs, then
//! `migrate` brings the database from its stored version up to the current.
//!
//! Adding a new schema version:
//! 1. Bump `SCHEMA_VERSION`.
//! 2. Add a new arm in `migrate` that handles the previous version.
//! 3. Always use `ALTER TABLE` / additive changes when possible.

use rusqlite::Connection;

use crate::infra::error::InfraError;

/// Current schema version. Bump on every schema change.
pub(crate) const SCHEMA_VERSION: i32 = 1;

/// Per-connection initialization: PRAGMAs that must be set on every connection.
///
/// Also runs migrations once per database (idempotent — `user_version` gates them).
pub(crate) fn init_connection(conn: &mut Connection) -> Result<(), InfraError> {
    conn.execute_batch(
        "PRAGMA journal_mode = WAL;
         PRAGMA foreign_keys = ON;
         PRAGMA busy_timeout = 5000;",
    )
    .map_err(|e| InfraError::Store {
        op: "sqlite",
        reason: format!("pragma init failed: {e}"),
    })?;
    migrate(conn)?;
    Ok(())
}

/// Read `PRAGMA user_version`.
fn read_version(conn: &Connection) -> Result<i32, InfraError> {
    conn.query_row("PRAGMA user_version", [], |row| row.get::<_, i32>(0))
        .map_err(|e| InfraError::Store {
            op: "sqlite",
            reason: format!("read user_version failed: {e}"),
        })
}

/// Set `PRAGMA user_version`. `user_version` does not accept parameter binding,
/// so we format the integer literal directly (safe — caller passes i32 only).
fn set_version(conn: &Connection, version: i32) -> Result<(), InfraError> {
    conn.execute_batch(&format!("PRAGMA user_version = {version};"))
        .map_err(|e| InfraError::Store {
            op: "sqlite",
            reason: format!("set user_version failed: {e}"),
        })
}

/// Run migrations from the stored `user_version` up to `SCHEMA_VERSION`.
fn migrate(conn: &mut Connection) -> Result<(), InfraError> {
    let current = read_version(conn)?;
    if current > SCHEMA_VERSION {
        return Err(InfraError::Store {
            op: "sqlite",
            reason: format!(
                "database schema version {current} is newer than supported {SCHEMA_VERSION} \
                 — downgrade not supported"
            ),
        });
    }
    if current < 1 {
        migrate_v0_to_v1(conn)?;
        set_version(conn, 1)?;
    }
    // Future migrations:
    // if current < 2 { migrate_v1_to_v2(conn)?; set_version(conn, 2)?; }
    Ok(())
}

/// v0 → v1: initial schema. Creates all baseline tables.
fn migrate_v0_to_v1(conn: &mut Connection) -> Result<(), InfraError> {
    conn.execute_batch(
        "
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
            file_id         TEXT NOT NULL REFERENCES topology_files(id) ON DELETE CASCADE,
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

        CREATE TABLE IF NOT EXISTS sync_tasks (
            task_id     TEXT PRIMARY KEY,
            status      TEXT NOT NULL DEFAULT 'pending',
            phase       TEXT NOT NULL DEFAULT '',
            result_json TEXT,
            error       TEXT,
            created_at  TEXT NOT NULL,
            updated_at  TEXT NOT NULL
        );
        ",
    )
    .map_err(|e| InfraError::Store {
        op: "sqlite",
        reason: format!("v0→v1 migration failed: {e}"),
    })?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fresh() -> Connection {
        let mut conn = Connection::open_in_memory().expect("open in-memory");
        init_connection(&mut conn).expect("init");
        conn
    }

    #[test]
    fn init_sets_schema_version_to_current() {
        let conn = fresh();
        let v = read_version(&conn).expect("read version");
        assert_eq!(v, SCHEMA_VERSION);
    }

    #[test]
    fn init_is_idempotent() {
        let mut conn = Connection::open_in_memory().expect("open in-memory");
        init_connection(&mut conn).expect("init1");
        init_connection(&mut conn).expect("init2");
        let v = read_version(&conn).expect("read version");
        assert_eq!(v, SCHEMA_VERSION);
    }

    #[test]
    fn newer_db_version_is_rejected() {
        let mut conn = Connection::open_in_memory().expect("open in-memory");
        set_version(&conn, SCHEMA_VERSION + 1).expect("set future version");
        let err = init_connection(&mut conn).expect_err("must reject newer schema");
        let msg = err.to_string();
        assert!(msg.contains("newer than supported"), "got: {msg}");
    }

    #[test]
    fn location_files_fk_cascades_on_topology_delete() {
        let conn = fresh();
        conn.execute(
            "INSERT INTO topology_files (id, relative_path, file_type, registered_at) \
             VALUES ('tf1', 'a.png', 'image', '2025-01-01T00:00:00Z')",
            [],
        )
        .expect("insert tf");
        conn.execute(
            "INSERT INTO location_files (file_id, location_id, relative_path, size, updated_at) \
             VALUES ('tf1', 'local', 'a.png', 0, '2025-01-01T00:00:00Z')",
            [],
        )
        .expect("insert lf");
        conn.execute("DELETE FROM topology_files WHERE id = 'tf1'", [])
            .expect("delete tf");
        let count: i64 = conn
            .query_row("SELECT COUNT(*) FROM location_files", [], |r| r.get(0))
            .expect("count");
        assert_eq!(count, 0, "FK CASCADE should remove orphan location_files");
    }
}
