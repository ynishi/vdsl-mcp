//! Background-task registry for `vdsl_run` polling.
//!
//! # Problem
//!
//! `vdsl_run` runs a Lua compile + ComfyUI batch generate + image
//! download pipeline that easily spans 20-40 min for a real sweep
//! (one MCP call previously timed out at 2400 s for a phase0 sketch
//! batch). A blocking single-call shape is `Pain`: the LLM client
//! holds an open MCP request the entire time, no progress signal,
//! and a client restart abandons the run.
//!
//! # Shape
//!
//! `vdsl_run` defaults to `background=true` (overridable per call):
//!
//! 1. The handler stages a [`RunRunState`] keyed by a freshly
//!    allocated `task_id` and `tokio::spawn`s the existing
//!    blocking pipeline as `run_blocking`.
//! 2. Returns `{ task_id, status: "running" }` immediately.
//! 3. Clients poll `vdsl_run_status(task_id)` for the current
//!    snapshot (running / ok / failed, full log on terminal).
//!
//! Polling is cheap — state lives in process memory.
//!
//! # Lifecycle
//!
//! - `running` → `ok | failed`
//! - Terminal entries are kept for the MCP-process lifetime so
//!   late pollers still get the full result.
//! - Mirrors [`crate::application::apply_registry`]; see there for
//!   broader rationale (2026-04-22 accident notes).

use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::{SystemTime, UNIX_EPOCH};

use serde::Serialize;
use tokio::sync::Mutex;

#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum RunStatus {
    Running,
    Ok,
    Failed,
}

#[derive(Debug, Clone, Serialize)]
pub struct RunRunState {
    pub task_id: String,
    pub script_label: String,
    pub status: RunStatus,
    /// Joined log text, populated on terminal state.
    pub log: String,
    pub started_at_ms: u64,
    pub finished_at_ms: Option<u64>,
    pub error: Option<String>,
}

impl RunRunState {
    pub fn new(task_id: String, script_label: String) -> Self {
        Self {
            task_id,
            script_label,
            status: RunStatus::Running,
            log: String::new(),
            started_at_ms: now_ms(),
            finished_at_ms: None,
            error: None,
        }
    }
}

#[derive(Clone, Default)]
pub struct RunRegistry {
    inner: Arc<RwLock<HashMap<String, Arc<Mutex<RunRunState>>>>>,
}

impl RunRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn insert(&self, state: RunRunState) -> Arc<Mutex<RunRunState>> {
        let handle = Arc::new(Mutex::new(state.clone()));
        if let Ok(mut map) = self.inner.write() {
            map.insert(state.task_id.clone(), handle.clone());
        }
        handle
    }

    pub async fn snapshot(&self, task_id: &str) -> Option<RunRunState> {
        let handle = {
            let map = self.inner.read().ok()?;
            map.get(task_id).cloned()?
        };
        let guard = handle.lock().await;
        Some(guard.clone())
    }

    #[cfg(test)]
    pub fn remove(&self, task_id: &str) {
        if let Ok(mut map) = self.inner.write() {
            map.remove(task_id);
        }
    }
}

pub async fn finalize_ok(handle: &Arc<Mutex<RunRunState>>, log: String) {
    let mut s = handle.lock().await;
    s.status = RunStatus::Ok;
    s.log = log;
    s.finished_at_ms = Some(now_ms());
}

pub async fn finalize_err(handle: &Arc<Mutex<RunRunState>>, error: String, partial_log: String) {
    let mut s = handle.lock().await;
    s.status = RunStatus::Failed;
    s.log = partial_log;
    s.error = Some(error);
    s.finished_at_ms = Some(now_ms());
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn insert_snapshot_round_trip() {
        let reg = RunRegistry::new();
        let state = RunRunState::new("run_1".into(), "phase0.lua".into());
        let _h = reg.insert(state);
        let snap = reg.snapshot("run_1").await.expect("present");
        assert_eq!(snap.status, RunStatus::Running);
        assert!(snap.finished_at_ms.is_none());
    }

    #[tokio::test]
    async fn finalize_ok_sets_terminal_state() {
        let reg = RunRegistry::new();
        let state = RunRunState::new("run_2".into(), "x.lua".into());
        let h = reg.insert(state);
        finalize_ok(&h, "all done".into()).await;
        let snap = reg.snapshot("run_2").await.unwrap();
        assert_eq!(snap.status, RunStatus::Ok);
        assert_eq!(snap.log, "all done");
        assert!(snap.finished_at_ms.is_some());
        assert!(snap.error.is_none());
    }

    #[tokio::test]
    async fn finalize_err_records_error_and_partial_log() {
        let reg = RunRegistry::new();
        let state = RunRunState::new("run_3".into(), "x.lua".into());
        let h = reg.insert(state);
        finalize_err(&h, "boom".into(), "log so far".into()).await;
        let snap = reg.snapshot("run_3").await.unwrap();
        assert_eq!(snap.status, RunStatus::Failed);
        assert_eq!(snap.log, "log so far");
        assert_eq!(snap.error.as_deref(), Some("boom"));
    }

    #[tokio::test]
    async fn snapshot_unknown_returns_none() {
        let reg = RunRegistry::new();
        assert!(reg.snapshot("nope").await.is_none());
    }
}
