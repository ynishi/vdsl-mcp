//! SQLite backend for VDSL Repository.
//!
//! Schema-compatible with the Lua-side `runtime/db.lua`.
//! Database path: `{working_dir}/.vdsl/generations.db`

use rusqlite::{params, Connection, OptionalExtension};
use std::path::{Path, PathBuf};
use std::sync::Mutex;

use crate::domain::repository::*;

const SCHEMA: &str = r#"
CREATE TABLE IF NOT EXISTS workspaces (
    id         TEXT PRIMARY KEY,
    name       TEXT NOT NULL,
    created_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_workspaces_name ON workspaces(name);

CREATE TABLE IF NOT EXISTS runs (
    id           TEXT PRIMARY KEY,
    workspace_id TEXT NOT NULL REFERENCES workspaces(id),
    script       TEXT,
    created_at   TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_runs_workspace ON runs(workspace_id);
CREATE INDEX IF NOT EXISTS idx_runs_created   ON runs(created_at);

CREATE TABLE IF NOT EXISTS generations (
    id         TEXT PRIMARY KEY,
    run_id     TEXT NOT NULL REFERENCES runs(id),
    seed       INTEGER,
    model      TEXT,
    output     TEXT,
    created_at TEXT NOT NULL,
    recipe     TEXT,
    meta       TEXT
);
CREATE INDEX IF NOT EXISTS idx_gens_run     ON generations(run_id);
CREATE INDEX IF NOT EXISTS idx_gens_created ON generations(created_at);
CREATE INDEX IF NOT EXISTS idx_gens_model   ON generations(model);
"#;

const MIGRATE_META: &str = "ALTER TABLE generations ADD COLUMN meta TEXT;";

/// SQLite-backed repository.
pub struct SqliteRepository {
    conn: Mutex<Connection>,
    #[allow(dead_code)]
    path: PathBuf,
}

impl SqliteRepository {
    /// Open or create the database at `{base_dir}/.vdsl/generations.db`.
    pub fn open(base_dir: &Path) -> Result<Self, RepositoryError> {
        let db_dir = base_dir.join(".vdsl");
        std::fs::create_dir_all(&db_dir).map_err(|e| {
            RepositoryError::Database(format!("failed to create {}: {e}", db_dir.display()))
        })?;

        let db_path = db_dir.join("generations.db");
        let conn = Connection::open(&db_path).map_err(|e| {
            RepositoryError::Database(format!("failed to open {}: {e}", db_path.display()))
        })?;

        conn.execute_batch("PRAGMA journal_mode=WAL; PRAGMA foreign_keys=ON;")
            .map_err(|e| RepositoryError::Database(format!("pragma failed: {e}")))?;

        conn.execute_batch(SCHEMA)
            .map_err(|e| RepositoryError::Database(format!("schema creation failed: {e}")))?;

        // Migration: add meta column if missing (idempotent)
        let _ = conn.execute_batch(MIGRATE_META);

        Ok(Self {
            conn: Mutex::new(conn),
            path: db_path,
        })
    }

    /// Open an in-memory database (for tests).
    #[cfg(test)]
    pub fn open_memory() -> Result<Self, RepositoryError> {
        let conn = Connection::open_in_memory()
            .map_err(|e| RepositoryError::Database(format!("in-memory open failed: {e}")))?;

        conn.execute_batch("PRAGMA foreign_keys=ON;")
            .map_err(|e| RepositoryError::Database(format!("pragma failed: {e}")))?;

        conn.execute_batch(SCHEMA)
            .map_err(|e| RepositoryError::Database(format!("schema creation failed: {e}")))?;

        Ok(Self {
            conn: Mutex::new(conn),
            path: PathBuf::from(":memory:"),
        })
    }
}

fn now_iso() -> String {
    chrono::Utc::now().format("%Y-%m-%dT%H:%M:%SZ").to_string()
}

fn new_uuid() -> String {
    uuid::Uuid::new_v4().to_string()
}

impl Repository for SqliteRepository {
    fn ensure_workspace(&self, name: &str) -> Result<Workspace, RepositoryError> {
        if name.is_empty() {
            return Err(RepositoryError::InvalidArgument(
                "workspace name must be non-empty".into(),
            ));
        }

        let conn = self
            .conn
            .lock()
            .map_err(|e| RepositoryError::Database(format!("lock failed: {e}")))?;

        // Try to find existing
        let existing: Option<Workspace> = conn
            .query_row(
                "SELECT id, name, created_at FROM workspaces WHERE name = ?1",
                params![name],
                |row| {
                    Ok(Workspace {
                        id: row.get(0)?,
                        name: row.get(1)?,
                        created_at: row.get(2)?,
                    })
                },
            )
            .optional()
            .map_err(|e| RepositoryError::Database(format!("query failed: {e}")))?;

        if let Some(ws) = existing {
            return Ok(ws);
        }

        // Create new
        let ws = Workspace {
            id: new_uuid(),
            name: name.to_string(),
            created_at: now_iso(),
        };

        conn.execute(
            "INSERT INTO workspaces (id, name, created_at) VALUES (?1, ?2, ?3)",
            params![ws.id, ws.name, ws.created_at],
        )
        .map_err(|e| RepositoryError::Database(format!("insert workspace failed: {e}")))?;

        Ok(ws)
    }

    fn list_workspaces(&self) -> Result<Vec<Workspace>, RepositoryError> {
        let conn = self
            .conn
            .lock()
            .map_err(|e| RepositoryError::Database(format!("lock failed: {e}")))?;

        let mut stmt = conn
            .prepare("SELECT id, name, created_at FROM workspaces ORDER BY created_at DESC")
            .map_err(|e| RepositoryError::Database(format!("prepare failed: {e}")))?;

        let rows = stmt
            .query_map([], |row| {
                Ok(Workspace {
                    id: row.get(0)?,
                    name: row.get(1)?,
                    created_at: row.get(2)?,
                })
            })
            .map_err(|e| RepositoryError::Database(format!("query failed: {e}")))?;

        let mut result = Vec::new();
        for row in rows {
            result
                .push(row.map_err(|e| RepositoryError::Database(format!("row read failed: {e}")))?);
        }
        Ok(result)
    }

    fn create_run(&self, workspace_id: &str, script: Option<&str>) -> Result<Run, RepositoryError> {
        let conn = self
            .conn
            .lock()
            .map_err(|e| RepositoryError::Database(format!("lock failed: {e}")))?;

        let run = Run {
            id: new_uuid(),
            workspace_id: workspace_id.to_string(),
            script: script.map(|s| s.to_string()),
            created_at: now_iso(),
        };

        conn.execute(
            "INSERT INTO runs (id, workspace_id, script, created_at) VALUES (?1, ?2, ?3, ?4)",
            params![run.id, run.workspace_id, run.script, run.created_at],
        )
        .map_err(|e| RepositoryError::Database(format!("insert run failed: {e}")))?;

        Ok(run)
    }

    fn find_runs_by_workspace(
        &self,
        workspace_id: &str,
        limit: u32,
    ) -> Result<Vec<Run>, RepositoryError> {
        let conn = self
            .conn
            .lock()
            .map_err(|e| RepositoryError::Database(format!("lock failed: {e}")))?;

        let mut stmt = conn
            .prepare(
                "SELECT id, workspace_id, script, created_at FROM runs \
                 WHERE workspace_id = ?1 ORDER BY created_at DESC LIMIT ?2",
            )
            .map_err(|e| RepositoryError::Database(format!("prepare failed: {e}")))?;

        let rows = stmt
            .query_map(params![workspace_id, limit], |row| {
                Ok(Run {
                    id: row.get(0)?,
                    workspace_id: row.get(1)?,
                    script: row.get(2)?,
                    created_at: row.get(3)?,
                })
            })
            .map_err(|e| RepositoryError::Database(format!("query failed: {e}")))?;

        let mut result = Vec::new();
        for row in rows {
            result
                .push(row.map_err(|e| RepositoryError::Database(format!("row read failed: {e}")))?);
        }
        Ok(result)
    }

    fn save_generation(&self, gen: &Generation) -> Result<(), RepositoryError> {
        if gen.run_id.is_empty() {
            return Err(RepositoryError::InvalidArgument(
                "run_id is required".into(),
            ));
        }

        let conn = self
            .conn
            .lock()
            .map_err(|e| RepositoryError::Database(format!("lock failed: {e}")))?;

        conn.execute(
            "INSERT INTO generations (id, run_id, seed, model, output, created_at, recipe, meta) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
            params![
                gen.id,
                gen.run_id,
                gen.seed,
                gen.model,
                gen.output,
                gen.created_at,
                gen.recipe,
                gen.meta,
            ],
        )
        .map_err(|e| RepositoryError::Database(format!("insert generation failed: {e}")))?;

        Ok(())
    }

    fn find_generation(&self, id: &str) -> Result<Option<Generation>, RepositoryError> {
        let conn = self
            .conn
            .lock()
            .map_err(|e| RepositoryError::Database(format!("lock failed: {e}")))?;

        conn.query_row(
            "SELECT id, run_id, seed, model, output, created_at, recipe, meta \
             FROM generations WHERE id = ?1",
            params![id],
            |row| {
                Ok(Generation {
                    id: row.get(0)?,
                    run_id: row.get(1)?,
                    seed: row.get(2)?,
                    model: row.get(3)?,
                    output: row.get(4)?,
                    created_at: row.get(5)?,
                    recipe: row.get(6)?,
                    meta: row.get(7)?,
                })
            },
        )
        .optional()
        .map_err(|e| RepositoryError::Database(format!("query failed: {e}")))
    }

    fn find_by_run(&self, run_id: &str) -> Result<Vec<Generation>, RepositoryError> {
        let conn = self
            .conn
            .lock()
            .map_err(|e| RepositoryError::Database(format!("lock failed: {e}")))?;

        let mut stmt = conn
            .prepare(
                "SELECT id, run_id, seed, model, output, created_at, recipe, meta \
                 FROM generations WHERE run_id = ?1 ORDER BY created_at",
            )
            .map_err(|e| RepositoryError::Database(format!("prepare failed: {e}")))?;

        let rows = stmt
            .query_map(params![run_id], |row| {
                Ok(Generation {
                    id: row.get(0)?,
                    run_id: row.get(1)?,
                    seed: row.get(2)?,
                    model: row.get(3)?,
                    output: row.get(4)?,
                    created_at: row.get(5)?,
                    recipe: row.get(6)?,
                    meta: row.get(7)?,
                })
            })
            .map_err(|e| RepositoryError::Database(format!("query failed: {e}")))?;

        let mut result = Vec::new();
        for row in rows {
            result
                .push(row.map_err(|e| RepositoryError::Database(format!("row read failed: {e}")))?);
        }
        Ok(result)
    }

    fn query_generations(
        &self,
        filter: &GenerationFilter,
        opts: &QueryOpts,
    ) -> Result<Vec<GenerationRow>, RepositoryError> {
        let conn = self
            .conn
            .lock()
            .map_err(|e| RepositoryError::Database(format!("lock failed: {e}")))?;

        let mut sql = String::from(
            "SELECT g.id, g.run_id, g.seed, g.model, g.output, g.created_at, g.recipe, g.meta, \
             r.script, r.workspace_id, w.name as workspace_name \
             FROM generations g \
             JOIN runs r ON g.run_id = r.id \
             JOIN workspaces w ON r.workspace_id = w.id",
        );

        let mut conditions = Vec::new();
        let mut bind_values: Vec<Box<dyn rusqlite::types::ToSql>> = Vec::new();

        if let Some(ref model) = filter.model {
            conditions.push(format!("g.model = ?{}", bind_values.len() + 1));
            bind_values.push(Box::new(model.clone()));
        }
        if let Some(ref script) = filter.script {
            conditions.push(format!("r.script = ?{}", bind_values.len() + 1));
            bind_values.push(Box::new(script.clone()));
        }
        if let Some(ref workspace) = filter.workspace {
            conditions.push(format!("w.name = ?{}", bind_values.len() + 1));
            bind_values.push(Box::new(workspace.clone()));
        }
        if let Some(ref date_from) = filter.date_from {
            conditions.push(format!("g.created_at >= ?{}", bind_values.len() + 1));
            bind_values.push(Box::new(date_from.clone()));
        }
        if let Some(ref date_to) = filter.date_to {
            conditions.push(format!("g.created_at <= ?{}", bind_values.len() + 1));
            bind_values.push(Box::new(date_to.clone()));
        }

        if !conditions.is_empty() {
            sql.push_str(" WHERE ");
            sql.push_str(&conditions.join(" AND "));
        }

        sql.push_str(" ORDER BY g.created_at DESC");

        let limit = opts.limit.unwrap_or(50);
        sql.push_str(&format!(" LIMIT {limit}"));

        if let Some(offset) = opts.offset {
            sql.push_str(&format!(" OFFSET {offset}"));
        }

        let params_refs: Vec<&dyn rusqlite::types::ToSql> =
            bind_values.iter().map(|b| b.as_ref()).collect();

        let mut stmt = conn
            .prepare(&sql)
            .map_err(|e| RepositoryError::Database(format!("prepare failed: {e}")))?;

        let rows = stmt
            .query_map(params_refs.as_slice(), |row| {
                Ok(GenerationRow {
                    gen: Generation {
                        id: row.get(0)?,
                        run_id: row.get(1)?,
                        seed: row.get(2)?,
                        model: row.get(3)?,
                        output: row.get(4)?,
                        created_at: row.get(5)?,
                        recipe: row.get(6)?,
                        meta: row.get(7)?,
                    },
                    script: row.get(8)?,
                    workspace_id: row.get(9)?,
                    workspace_name: row.get(10)?,
                })
            })
            .map_err(|e| RepositoryError::Database(format!("query failed: {e}")))?;

        let mut result = Vec::new();
        for row in rows {
            result
                .push(row.map_err(|e| RepositoryError::Database(format!("row read failed: {e}")))?);
        }
        Ok(result)
    }

    fn stats(&self, group_by: &str) -> Result<Vec<StatRow>, RepositoryError> {
        let conn = self
            .conn
            .lock()
            .map_err(|e| RepositoryError::Database(format!("lock failed: {e}")))?;

        let sql = match group_by {
            "model" => {
                "SELECT COALESCE(model, 'unknown') as grp, count(*) as cnt \
                 FROM generations GROUP BY model ORDER BY cnt DESC"
            }
            "script" => {
                "SELECT COALESCE(r.script, 'unknown') as grp, count(*) as cnt \
                 FROM generations g JOIN runs r ON g.run_id = r.id \
                 GROUP BY r.script ORDER BY cnt DESC"
            }
            "workspace" => {
                "SELECT w.name as grp, count(*) as cnt \
                 FROM generations g JOIN runs r ON g.run_id = r.id \
                 JOIN workspaces w ON r.workspace_id = w.id \
                 GROUP BY w.name ORDER BY cnt DESC"
            }
            "date" => {
                "SELECT substr(created_at,1,10) as grp, count(*) as cnt \
                 FROM generations GROUP BY substr(created_at,1,10) ORDER BY grp DESC"
            }
            _ => {
                return Err(RepositoryError::InvalidArgument(format!(
                    "unsupported group_by: {group_by}"
                )))
            }
        };

        let mut stmt = conn
            .prepare(sql)
            .map_err(|e| RepositoryError::Database(format!("prepare failed: {e}")))?;

        let rows = stmt
            .query_map([], |row| {
                Ok(StatRow {
                    group: row.get(0)?,
                    count: row.get(1)?,
                })
            })
            .map_err(|e| RepositoryError::Database(format!("query failed: {e}")))?;

        let mut result = Vec::new();
        for row in rows {
            result
                .push(row.map_err(|e| RepositoryError::Database(format!("row read failed: {e}")))?);
        }
        Ok(result)
    }

    fn get_meta(&self, gen_id: &str) -> Result<Option<String>, RepositoryError> {
        let conn = self
            .conn
            .lock()
            .map_err(|e| RepositoryError::Database(format!("lock failed: {e}")))?;

        let meta: Option<Option<String>> = conn
            .query_row(
                "SELECT meta FROM generations WHERE id = ?1",
                params![gen_id],
                |row| row.get(0),
            )
            .optional()
            .map_err(|e| RepositoryError::Database(format!("query failed: {e}")))?;

        Ok(meta.flatten())
    }

    fn set_meta(&self, gen_id: &str, meta_json: &str) -> Result<(), RepositoryError> {
        let conn = self
            .conn
            .lock()
            .map_err(|e| RepositoryError::Database(format!("lock failed: {e}")))?;

        let updated = conn
            .execute(
                "UPDATE generations SET meta = ?1 WHERE id = ?2",
                params![meta_json, gen_id],
            )
            .map_err(|e| RepositoryError::Database(format!("update failed: {e}")))?;

        if updated == 0 {
            return Err(RepositoryError::NotFound(format!(
                "generation {gen_id} not found"
            )));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_repo() -> SqliteRepository {
        SqliteRepository::open_memory().expect("open in-memory DB")
    }

    #[test]
    fn ensure_workspace_creates_and_reuses() {
        let repo = test_repo();
        let ws1 = repo.ensure_workspace("test_ws").unwrap();
        let ws2 = repo.ensure_workspace("test_ws").unwrap();
        assert_eq!(ws1.id, ws2.id);
        assert_eq!(ws1.name, "test_ws");
    }

    #[test]
    fn ensure_workspace_rejects_empty() {
        let repo = test_repo();
        assert!(repo.ensure_workspace("").is_err());
    }

    #[test]
    fn create_run_and_find() {
        let repo = test_repo();
        let ws = repo.ensure_workspace("ws").unwrap();
        let run = repo.create_run(&ws.id, Some("test.lua")).unwrap();
        let runs = repo.find_runs_by_workspace(&ws.id, 10).unwrap();
        assert_eq!(runs.len(), 1);
        assert_eq!(runs[0].id, run.id);
        assert_eq!(runs[0].script.as_deref(), Some("test.lua"));
    }

    #[test]
    fn save_and_find_generation() {
        let repo = test_repo();
        let ws = repo.ensure_workspace("ws").unwrap();
        let run = repo.create_run(&ws.id, None).unwrap();

        let gen = Generation {
            id: new_uuid(),
            run_id: run.id.clone(),
            seed: Some(42),
            model: Some("test_model.safetensors".into()),
            output: Some("output/test.png".into()),
            created_at: now_iso(),
            recipe: Some(r#"{"_v":2}"#.into()),
            meta: None,
        };

        repo.save_generation(&gen).unwrap();

        let found = repo.find_generation(&gen.id).unwrap().unwrap();
        assert_eq!(found.seed, Some(42));
        assert_eq!(found.model.as_deref(), Some("test_model.safetensors"));
    }

    #[test]
    fn find_by_run_returns_ordered() {
        let repo = test_repo();
        let ws = repo.ensure_workspace("ws").unwrap();
        let run = repo.create_run(&ws.id, None).unwrap();

        for i in 0..3 {
            let gen = Generation {
                id: new_uuid(),
                run_id: run.id.clone(),
                seed: Some(i),
                model: None,
                output: None,
                created_at: now_iso(),
                recipe: None,
                meta: None,
            };
            repo.save_generation(&gen).unwrap();
        }

        let gens = repo.find_by_run(&run.id).unwrap();
        assert_eq!(gens.len(), 3);
    }

    #[test]
    fn query_filter_by_model() {
        let repo = test_repo();
        let ws = repo.ensure_workspace("ws").unwrap();
        let run = repo.create_run(&ws.id, Some("script.lua")).unwrap();

        for model in &["a.safetensors", "b.safetensors", "a.safetensors"] {
            let gen = Generation {
                id: new_uuid(),
                run_id: run.id.clone(),
                seed: None,
                model: Some(model.to_string()),
                output: None,
                created_at: now_iso(),
                recipe: None,
                meta: None,
            };
            repo.save_generation(&gen).unwrap();
        }

        let filter = GenerationFilter {
            model: Some("a.safetensors".into()),
            ..Default::default()
        };
        let rows = repo
            .query_generations(&filter, &QueryOpts::default())
            .unwrap();
        assert_eq!(rows.len(), 2);
    }

    #[test]
    fn stats_by_model() {
        let repo = test_repo();
        let ws = repo.ensure_workspace("ws").unwrap();
        let run = repo.create_run(&ws.id, None).unwrap();

        for model in &["a", "a", "b"] {
            let gen = Generation {
                id: new_uuid(),
                run_id: run.id.clone(),
                seed: None,
                model: Some(model.to_string()),
                output: None,
                created_at: now_iso(),
                recipe: None,
                meta: None,
            };
            repo.save_generation(&gen).unwrap();
        }

        let stats = repo.stats("model").unwrap();
        assert_eq!(stats[0].group, "a");
        assert_eq!(stats[0].count, 2);
    }

    #[test]
    fn meta_set_and_get() {
        let repo = test_repo();
        let ws = repo.ensure_workspace("ws").unwrap();
        let run = repo.create_run(&ws.id, None).unwrap();
        let gen = Generation {
            id: new_uuid(),
            run_id: run.id,
            seed: None,
            model: None,
            output: None,
            created_at: now_iso(),
            recipe: None,
            meta: None,
        };
        repo.save_generation(&gen).unwrap();

        assert!(repo.get_meta(&gen.id).unwrap().is_none());

        repo.set_meta(&gen.id, r#"{"rating": 5}"#).unwrap();
        let meta = repo.get_meta(&gen.id).unwrap().unwrap();
        assert!(meta.contains("rating"));
    }
}
