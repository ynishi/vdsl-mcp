//! Background apply-task registry for `vdsl_profile_apply` polling.
//!
//! # Problem
//!
//! Early `vdsl_profile_apply` was a single synchronous MCP tool call that
//! blocked until every step finished. On a cold pod, install + model pull
//! takes 15–20 min; if the underlying SSH hung (runpod-cli `exec` without
//! keepalive), the MCP call could sit silent for 45+ min with no progress
//! signal and, on client restart, the whole apply started over from
//! Phase 1. See the project `CLAUDE.md` 2026-04-22 accident record.
//!
//! # Shape
//!
//! The caller hits [`vdsl_profile_apply`] with `dry_run=false`. Instead of
//! waiting for completion, the service now:
//!
//! 1. Builds the [`BatchPlan`] and stages an [`ApplyRunState`] in the
//!    registry, keyed by a freshly allocated `task_id`.
//! 2. `tokio::spawn`s the plan execution with a progress sink that
//!    mutates the state after each step result.
//! 3. Returns `{ plan_id, task_id }` immediately (< 1 s).
//!
//! Clients poll `vdsl_profile_apply_status(task_id)`, which snapshots
//! the current state (running / ok / failed, counts, partial results,
//! last-step tail). Polling is cheap — state lives in process memory,
//! no additional SSH traffic per poll.
//!
//! # Lifecycle
//!
//! - `pending` → `running` → `ok | failed`
//! - Terminal entries are kept in the registry so late pollers still get
//!   the full result. They are not garbage-collected automatically in v1;
//!   MCP-process lifetime bounds memory use.
//! - If the MCP process dies, in-memory state is lost. The pod-side
//!   detached tasks started via `runpod-cli task run` continue running
//!   — recovery by `runpod-cli task list` is possible but not wired
//!   yet (tracked separately).
//!
//! # Concurrency
//!
//! One [`Arc<Mutex<ApplyRunState>>`] per task so the spawned runner can
//! update progress without blocking pollers (they only clone the
//! snapshot). The outer registry uses a standard [`RwLock`] around a
//! [`HashMap`]; inserts and lookups are infrequent compared to per-step
//! progress writes, which target the inner mutex only.

use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::{SystemTime, UNIX_EPOCH};

use serde::Serialize;
use tokio::sync::Mutex;

use crate::application::profile_service::BatchStepResult;

/// Terminal / in-flight status of a background apply run.
#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum ApplyStatus {
    /// `tokio::spawn`'d and currently executing.
    Running,
    /// All steps finished with `StepStatus::Ok`.
    Ok,
    /// At least one step failed or the runner errored out.
    Failed,
}

/// Per-task mutable state. The spawned runner owns write access via
/// [`ApplyRegistry::mark_step_complete`] / [`ApplyRegistry::finalize`];
/// pollers clone this via [`ApplyRegistry::snapshot`].
#[derive(Debug, Clone, Serialize)]
pub struct ApplyRunState {
    pub task_id: String,
    pub plan_id: String,
    pub pod_id: String,
    pub status: ApplyStatus,

    /// Total leaf-step count known up-front from the plan. Groups
    /// contribute each of their children.
    pub total_steps: usize,
    /// Steps that have produced a [`BatchStepResult`] (ok / failed /
    /// skipped). Monotonic.
    pub completed_steps: usize,
    /// Step `id` currently executing, or the last one attempted when
    /// the run is terminal.
    pub current_step: Option<String>,

    /// Accumulated per-step results, in the order they completed.
    /// Groups flatten to their leaves.
    pub results: Vec<BatchStepResult>,

    /// Wall-clock timestamps (unix ms). `finished_at_ms` stays `None`
    /// while `status == Running`.
    pub started_at_ms: u64,
    pub finished_at_ms: Option<u64>,

    /// Populated when the runner itself panics or the plan expansion
    /// fails before any step runs. Per-step errors live in `results`.
    pub error: Option<String>,
}

impl ApplyRunState {
    pub fn new(task_id: String, pod_id: String, total_steps: usize) -> Self {
        Self {
            task_id,
            plan_id: String::new(),
            pod_id,
            status: ApplyStatus::Running,
            total_steps,
            completed_steps: 0,
            current_step: None,
            results: Vec::new(),
            started_at_ms: now_ms(),
            finished_at_ms: None,
            error: None,
        }
    }
}

/// Thread-safe map keyed by `task_id`. Cheap `Clone`: shares one `Arc`.
#[derive(Clone, Default)]
pub struct ApplyRegistry {
    inner: Arc<RwLock<HashMap<String, Arc<Mutex<ApplyRunState>>>>>,
}

impl ApplyRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    /// Insert a fresh state entry and hand back a handle the runner can
    /// write through. Overwrites any entry with the same `task_id`.
    pub fn insert(&self, state: ApplyRunState) -> Arc<Mutex<ApplyRunState>> {
        let handle = Arc::new(Mutex::new(state.clone()));
        if let Ok(mut map) = self.inner.write() {
            map.insert(state.task_id.clone(), handle.clone());
        }
        handle
    }

    /// Clone the current state as a JSON-serializable snapshot.
    /// Returns `None` for unknown `task_id`.
    pub async fn snapshot(&self, task_id: &str) -> Option<ApplyRunState> {
        let handle = {
            let map = self.inner.read().ok()?;
            map.get(task_id).cloned()?
        };
        let guard = handle.lock().await;
        Some(guard.clone())
    }

    /// Remove a terminal entry. Test utility; production leaves entries
    /// in place.
    #[cfg(test)]
    pub fn remove(&self, task_id: &str) {
        if let Ok(mut map) = self.inner.write() {
            map.remove(task_id);
        }
    }

    /// Count of registered tasks (test utility).
    #[cfg(test)]
    pub fn len(&self) -> usize {
        self.inner.read().map(|m| m.len()).unwrap_or(0)
    }

    #[cfg(test)]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// Runner-side helper: update progress after a step finishes.
/// Called by the `tokio::spawn`'d future inside `BatchService::run_background`.
pub async fn mark_step_complete(
    handle: &Arc<Mutex<ApplyRunState>>,
    result: BatchStepResult,
    current: Option<String>,
) {
    let mut s = handle.lock().await;
    s.completed_steps = s.completed_steps.saturating_add(1);
    s.current_step = current;
    s.results.push(result);
}

/// Runner-side helper: set the in-flight step before dispatch.
pub async fn mark_step_started(handle: &Arc<Mutex<ApplyRunState>>, step_id: String) {
    let mut s = handle.lock().await;
    s.current_step = Some(step_id);
}

/// Runner-side helper: finalize a run in terminal state.
pub async fn finalize(
    handle: &Arc<Mutex<ApplyRunState>>,
    status: ApplyStatus,
    plan_id: String,
    error: Option<String>,
) {
    let mut s = handle.lock().await;
    s.status = status;
    s.plan_id = plan_id;
    s.error = error;
    s.finished_at_ms = Some(now_ms());
    s.current_step = None;
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
    use crate::application::profile_service::StepStatus;

    fn sample_result(id: &str, status: StepStatus) -> BatchStepResult {
        BatchStepResult {
            id: id.to_string(),
            status,
            output: None,
            error: None,
        }
    }

    #[tokio::test]
    async fn insert_and_snapshot_round_trip() {
        let reg = ApplyRegistry::new();
        let state = ApplyRunState::new("apply_1".into(), "pod_a".into(), 3);
        let handle = reg.insert(state);

        // Write through the handle.
        mark_step_started(&handle, "1_apt".into()).await;
        mark_step_complete(
            &handle,
            sample_result("1_apt", StepStatus::Ok),
            Some("2_install".into()),
        )
        .await;

        let snap = reg.snapshot("apply_1").await.expect("present");
        assert_eq!(snap.completed_steps, 1);
        assert_eq!(snap.current_step.as_deref(), Some("2_install"));
        assert_eq!(snap.results.len(), 1);
        assert_eq!(snap.results[0].id, "1_apt");
        assert_eq!(snap.status, ApplyStatus::Running);
        assert!(snap.finished_at_ms.is_none());
    }

    #[tokio::test]
    async fn finalize_ok_clears_current_and_sets_timestamp() {
        let reg = ApplyRegistry::new();
        let state = ApplyRunState::new("apply_2".into(), "pod_a".into(), 1);
        let handle = reg.insert(state);

        mark_step_complete(&handle, sample_result("1_apt", StepStatus::Ok), None).await;
        finalize(&handle, ApplyStatus::Ok, "bt_abc".into(), None).await;

        let snap = reg.snapshot("apply_2").await.unwrap();
        assert_eq!(snap.status, ApplyStatus::Ok);
        assert_eq!(snap.plan_id, "bt_abc");
        assert!(snap.current_step.is_none());
        assert!(snap.finished_at_ms.is_some());
        assert!(snap.error.is_none());
    }

    #[tokio::test]
    async fn snapshot_unknown_task_returns_none() {
        let reg = ApplyRegistry::new();
        assert!(reg.snapshot("nope").await.is_none());
    }

    #[tokio::test]
    async fn finalize_failed_records_error() {
        let reg = ApplyRegistry::new();
        let state = ApplyRunState::new("apply_3".into(), "pod_a".into(), 2);
        let handle = reg.insert(state);

        mark_step_complete(&handle, sample_result("1_apt", StepStatus::Failed), None).await;
        finalize(
            &handle,
            ApplyStatus::Failed,
            "bt_zzz".into(),
            Some("boom".into()),
        )
        .await;

        let snap = reg.snapshot("apply_3").await.unwrap();
        assert_eq!(snap.status, ApplyStatus::Failed);
        assert_eq!(snap.error.as_deref(), Some("boom"));
    }
}
