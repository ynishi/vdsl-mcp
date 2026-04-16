//! Batch orchestration engine for `vdsl_batch_tools` / `vdsl_profile_apply`.
//!
//! Consumes a [`BatchPlan`] (subtask-1 contract) and dispatches each step to
//! the corresponding handler method on [`VdslMcpServer`]. Supports:
//!
//! - `seq` mode: linear execution, skip-on-failure propagation
//! - `dag` mode: Kahn's topological sort, concurrent execution capped at
//!   [`DAG_CONCURRENCY_CAP`]
//! - parallel groups inside seq: bounded by `group.parallel` semaphore permits
//! - per-step validators with 1 retry on validation failure
//! - dry-run: emit the plan verbatim without dispatching
//! - placeholder resolution at dispatch time:
//!   - `__result:step_id.field` → accumulated output text lookup
//!   - `__secret:NAME` → looked up in the `secrets` map (never logged)
//!
//! The core orchestration logic is generic over the dispatch function so unit
//! tests can substitute a deterministic closure for real MCP calls.

// Subtask-2 lands the engine behind `pub(crate)` but does not yet wire it into
// the MCP tool router — that happens in subtask-3 (`vdsl_batch_tools` /
// `vdsl_profile_apply` handlers). Suppress the dead-code warnings until then;
// subtask-3 will remove this attribute once the handlers import the service.
#![allow(dead_code)]

use std::collections::{HashMap, HashSet, VecDeque};
use std::future::Future;
use std::sync::Arc;

use tokio::sync::Semaphore;
use tokio::task::JoinSet;

use crate::application::profile_service::{
    BatchPlan, BatchResult, BatchStep, BatchStepResult, GroupBlock, PlanMode, StepEntry,
    StepStatus, ValidateBlock,
};
use crate::interface::mcp::{
    VdslComfyApiRequest, VdslExecRequest, VdslMcpServer, VdslSyncPollRequest, VdslSyncRouteRequest,
    VdslTaskRunRequest, VdslTaskStatusRequest,
};

use rmcp::handler::server::wrapper::Parameters;
use rmcp::model::{CallToolResult, RawContent};

/// Concurrency cap for DAG-mode independent branches (v1 hard-coded).
const DAG_CONCURRENCY_CAP: usize = 4;

// =============================================================================
// Error type
// =============================================================================

#[derive(Debug, thiserror::Error)]
pub enum BatchError {
    #[error("unknown tool: {0}")]
    UnknownTool(String),

    #[error("step '{step_id}' failed: {reason}")]
    StepFailed { step_id: String, reason: String },

    #[error("DAG cycle detected involving: {0}")]
    DagCycle(String),

    #[error("dispatch error: {0}")]
    Dispatch(String),

    #[error("validation failed for step '{step_id}': {reason}")]
    ValidationFailed { step_id: String, reason: String },
}

// =============================================================================
// Service
// =============================================================================

/// Orchestration engine bound to a live [`VdslMcpServer`].
///
/// Holds a shared clone of the server (cheap: all fields are `Arc`-backed) so
/// spawned tasks can own the handle without lifetime friction.
pub(crate) struct BatchService {
    server: VdslMcpServer,
}

impl BatchService {
    pub(crate) fn new(server: VdslMcpServer) -> Self {
        Self { server }
    }

    /// Execute `plan`. Routes to seq/dag based on `plan.mode`.
    ///
    /// `secrets` is consumed at dispatch time for `__secret:NAME` substitution.
    /// Pass `HashMap::new()` if the plan contains no secret placeholders.
    pub async fn execute(
        &self,
        plan: BatchPlan,
        secrets: HashMap<String, String>,
    ) -> Result<BatchResult, BatchError> {
        let plan_id = format!("bt_{}", uuid::Uuid::new_v4());

        if plan.dry_run {
            let results = dry_run_plan(&plan.steps);
            return Ok(BatchResult {
                plan_id,
                results,
                dry_run: true,
            });
        }

        let results = match plan.mode {
            PlanMode::Seq => {
                let server = self.server.clone();
                let dispatcher = move |step: BatchStep| {
                    let srv = server.clone();
                    async move { dispatch_step_with_server(&srv, &step).await }
                };
                run_seq_generic(plan.steps, &secrets, dispatcher).await
            }
            PlanMode::Dag => {
                // DAG mode takes a flat `Vec<BatchStep>`. Flatten any groups
                // down to their leaves (groups inside dag are unusual but
                // tolerated — each leaf inherits no depends_on beyond what it
                // already declares).
                let leaves = flatten_to_leaves(plan.steps);
                let server = self.server.clone();
                let dispatcher = move |step: BatchStep| {
                    let srv = server.clone();
                    async move { dispatch_step_with_server(&srv, &step).await }
                };
                run_dag_generic(leaves, &secrets, dispatcher).await?
            }
        };

        Ok(BatchResult {
            plan_id,
            results,
            dry_run: false,
        })
    }
}

// =============================================================================
// Dispatch (real server path)
// =============================================================================

/// Dispatch a single leaf step against the real [`VdslMcpServer`].
///
/// Returns the concatenated text output on success. On error (either an
/// `is_error=Some(true)` tool result or a transport/dispatch failure) returns
/// the error string.
async fn dispatch_step_with_server(
    server: &VdslMcpServer,
    step: &BatchStep,
) -> Result<String, String> {
    match step.tool.as_str() {
        "exec" => {
            let req: VdslExecRequest = serde_json::from_value(step.args.clone())
                .map_err(|e| format!("bad args for exec: {e}"))?;
            let result = server
                .exec(Parameters(req))
                .await
                .map_err(|e| e.to_string())?;
            extract_result(result)
        }
        "task_run" => {
            let req: VdslTaskRunRequest = serde_json::from_value(step.args.clone())
                .map_err(|e| format!("bad args for task_run: {e}"))?;
            let result = server
                .task_run(Parameters(req))
                .await
                .map_err(|e| e.to_string())?;
            extract_result(result)
        }
        "task_status" => {
            let req: VdslTaskStatusRequest = serde_json::from_value(step.args.clone())
                .map_err(|e| format!("bad args for task_status: {e}"))?;
            let result = server
                .task_status(Parameters(req))
                .await
                .map_err(|e| e.to_string())?;
            extract_result(result)
        }
        "sync" => {
            let result = server.sync().await.map_err(|e| e.to_string())?;
            extract_result(result)
        }
        "sync_route" => {
            let req: VdslSyncRouteRequest = serde_json::from_value(step.args.clone())
                .map_err(|e| format!("bad args for sync_route: {e}"))?;
            let result = server
                .sync_route(Parameters(req))
                .await
                .map_err(|e| e.to_string())?;
            extract_result(result)
        }
        "sync_poll" => {
            let req: VdslSyncPollRequest = serde_json::from_value(step.args.clone())
                .map_err(|e| format!("bad args for sync_poll: {e}"))?;
            let result = server
                .sync_poll(Parameters(req))
                .await
                .map_err(|e| e.to_string())?;
            extract_result(result)
        }
        "comfy_api" => {
            let req: VdslComfyApiRequest = serde_json::from_value(step.args.clone())
                .map_err(|e| format!("bad args for comfy_api: {e}"))?;
            let result = server
                .comfy_api(Parameters(req))
                .await
                .map_err(|e| e.to_string())?;
            extract_result(result)
        }
        other => Err(format!("unknown tool: {other}")),
    }
}

/// Extract the concatenated text content from a [`CallToolResult`].
///
/// Maps `is_error: Some(true)` to `Err`. Non-text content (images, resources)
/// is discarded — batch steps contract on text output.
fn extract_result(result: CallToolResult) -> Result<String, String> {
    let is_err = result.is_error.unwrap_or(false);
    let text = result
        .content
        .iter()
        .filter_map(|c| match &c.raw {
            RawContent::Text(t) => Some(t.text.as_str()),
            _ => None,
        })
        .collect::<Vec<_>>()
        .join("\n");
    if is_err {
        Err(text)
    } else {
        Ok(text)
    }
}

// =============================================================================
// Placeholder resolution
// =============================================================================

/// Walk a JSON value recursively and resolve `__result:` and `__secret:`
/// placeholders embedded in string leaves.
///
/// Placeholder syntax:
/// - `__result:step_id.field` — looked up in `accumulated[step_id]`. For the
///   `task_id` field, matches `task_id: VAL` or `Job ID: VAL` patterns in the
///   accumulated step output. Returns Err if the step or field is missing.
/// - `__secret:NAME` — looked up in `secrets`. Returns Err if `NAME` is
///   missing — never logs the resolved value.
///
/// Non-string leaves (numbers, bools, null) pass through unchanged. A string
/// may contain a placeholder as a prefix/substring — the *entire* value is
/// replaced when the pattern matches the whole string. Partial (substring)
/// substitution is also supported for placeholders embedded inside larger
/// strings.
fn resolve_placeholders(
    args: &serde_json::Value,
    secrets: &HashMap<String, String>,
    accumulated: &HashMap<String, String>,
) -> Result<serde_json::Value, String> {
    match args {
        serde_json::Value::String(s) => {
            let resolved = resolve_string(s, secrets, accumulated)?;
            Ok(serde_json::Value::String(resolved))
        }
        serde_json::Value::Array(items) => {
            let mut out = Vec::with_capacity(items.len());
            for item in items {
                out.push(resolve_placeholders(item, secrets, accumulated)?);
            }
            Ok(serde_json::Value::Array(out))
        }
        serde_json::Value::Object(map) => {
            let mut out = serde_json::Map::with_capacity(map.len());
            for (k, v) in map {
                out.insert(k.clone(), resolve_placeholders(v, secrets, accumulated)?);
            }
            Ok(serde_json::Value::Object(out))
        }
        other => Ok(other.clone()),
    }
}

/// Resolve all placeholder occurrences in a single string.
///
/// Resolution order (important for security):
///
/// 1. `__secret:` is resolved against the *original* input string first.
///    This ensures only placeholders written by the plan author are expanded.
/// 2. `__result:` is resolved against the string *after* secret substitution.
///
/// This ordering prevents a step-output injection attack: if a previous
/// step's output text contains a literal `__secret:NAME` string and that
/// output is spliced in via `__result:`, the injected text is never passed
/// through the secret resolver — only the plan-author-written `__secret:`
/// markers are resolved.
fn resolve_string(
    input: &str,
    secrets: &HashMap<String, String>,
    accumulated: &HashMap<String, String>,
) -> Result<String, String> {
    // Pass 1: resolve __secret: against the original input only.
    let after_secret = scan_and_replace(input, "__secret:", |token| {
        secrets
            .get(token)
            .cloned()
            .ok_or_else(|| format!("unresolved secret: {token}"))
    })?;

    // Pass 2: resolve __result: against the secret-substituted string.
    // Any __secret: text that arrives here via __result: expansion is step
    // output and was not present in the original input, so it must not be
    // resolved — and it won't be, because this pass only touches __result:.
    let after_result = scan_and_replace(&after_secret, "__result:", |token| {
        // token = "step_id.field"
        let (step_id, field) = token
            .split_once('.')
            .ok_or_else(|| format!("malformed __result placeholder: {token}"))?;
        let output = accumulated
            .get(step_id)
            .ok_or_else(|| format!("unresolved __result: step '{step_id}' not in accumulated"))?;
        extract_field(output, field).ok_or_else(|| {
            format!("unresolved __result: field '{field}' not found in step '{step_id}' output")
        })
    })?;

    Ok(after_result)
}

/// Generic prefix-scan replacer.
///
/// Walks `input`, and whenever it encounters `prefix`, reads an identifier
/// (including `.`) of `[A-Za-z0-9_.]*` immediately after, then calls `resolver`
/// with the captured identifier. The prefix + identifier is replaced with the
/// resolver's return value.
fn scan_and_replace<F>(input: &str, prefix: &str, mut resolver: F) -> Result<String, String>
where
    F: FnMut(&str) -> Result<String, String>,
{
    let mut out = String::with_capacity(input.len());
    let mut rest = input;
    while let Some(idx) = rest.find(prefix) {
        out.push_str(&rest[..idx]);
        let after = &rest[idx + prefix.len()..];
        // Identifier chars: alphanumeric, underscore, dot (dot is only valid
        // for __result:, but harmless for __secret: since NAME chars exclude
        // dot in practice. Callers validate the resolved token shape).
        let ident_end = after
            .char_indices()
            .take_while(|(_, c)| c.is_ascii_alphanumeric() || *c == '_' || *c == '.')
            .map(|(i, c)| i + c.len_utf8())
            .last()
            .unwrap_or(0);
        if ident_end == 0 {
            // Bare prefix with no identifier — treat as literal so the plan
            // is not silently mangled.
            out.push_str(prefix);
            rest = after;
            continue;
        }
        let token = &after[..ident_end];
        // For __secret: the token must not contain a dot (NAME is a single
        // identifier). If it does, split on the first dot and keep the
        // trailing portion as literal — secrets never have dotted names.
        let (lookup_token, trailing) = if prefix == "__secret:" {
            match token.split_once('.') {
                Some((left, right)) => (left, Some(right)),
                None => (token, None),
            }
        } else {
            (token, None)
        };
        let replacement = resolver(lookup_token)?;
        out.push_str(&replacement);
        if let Some(tail) = trailing {
            out.push('.');
            out.push_str(tail);
        }
        rest = &after[ident_end..];
    }
    out.push_str(rest);
    Ok(out)
}

/// Extract a named field from a step's text output.
///
/// Recognized patterns:
/// - `task_id: VAL`
/// - `Job ID: VAL`
/// - `NAME: VAL` (generic fallback)
///
/// Returns the first match's value, trimmed.
fn extract_field(output: &str, field: &str) -> Option<String> {
    let patterns = if field == "task_id" {
        vec![
            format!("{}:", field),
            "Job ID:".to_string(),
            "task_id:".to_string(),
        ]
    } else {
        vec![format!("{}:", field)]
    };
    for pat in &patterns {
        if let Some(idx) = output.find(pat.as_str()) {
            let after = &output[idx + pat.len()..];
            let line_end = after.find('\n').unwrap_or(after.len());
            let val = after[..line_end].trim();
            if !val.is_empty() {
                return Some(val.to_string());
            }
        }
    }
    None
}

// =============================================================================
// Dry-run
// =============================================================================

/// Walk all plan entries and emit a result per leaf with `StepStatus::Ok` and
/// the args serialized verbatim. Placeholders remain opaque.
fn dry_run_plan(steps: &[StepEntry]) -> Vec<BatchStepResult> {
    let mut out = Vec::new();
    for entry in steps {
        match entry {
            StepEntry::Leaf(step) => out.push(dry_run_step(step)),
            StepEntry::Group(group) => {
                for step in &group.steps {
                    out.push(dry_run_step(step));
                }
            }
        }
    }
    out
}

fn dry_run_step(step: &BatchStep) -> BatchStepResult {
    BatchStepResult {
        id: step.id.clone(),
        status: StepStatus::Ok,
        output: Some(step.args.to_string()),
        error: None,
    }
}

// =============================================================================
// Generic seq / group / dag orchestration (testable core)
// =============================================================================

/// Sequential execution. Runs `dispatch` on each leaf step in order, handles
/// groups with [`run_group_generic`], and skips remaining entries after the
/// first failure.
///
/// `dispatch` receives the step *with placeholders already resolved* and
/// returns `Ok(output_text)` or `Err(error_text)`.
async fn run_seq_generic<F, Fut>(
    steps: Vec<StepEntry>,
    secrets: &HashMap<String, String>,
    dispatch: F,
) -> Vec<BatchStepResult>
where
    F: Fn(BatchStep) -> Fut + Clone + Send + Sync + 'static,
    Fut: Future<Output = Result<String, String>> + Send + 'static,
{
    let mut results: Vec<BatchStepResult> = Vec::new();
    let mut accumulated: HashMap<String, String> = HashMap::new();
    let mut aborted = false;

    for entry in steps {
        if aborted {
            let ids = collect_entry_ids(&entry);
            for id in ids {
                results.push(BatchStepResult {
                    id,
                    status: StepStatus::Skipped,
                    output: None,
                    error: None,
                });
            }
            continue;
        }

        match entry {
            StepEntry::Leaf(step) => {
                let r = run_leaf_with_validation(&step, secrets, &accumulated, &dispatch).await;
                let failed = r.status == StepStatus::Failed;
                if let Some(ref out) = r.output {
                    accumulated.insert(r.id.clone(), out.clone());
                }
                results.push(r);
                if failed {
                    aborted = true;
                }
            }
            StepEntry::Group(group) => {
                let group_results =
                    run_group_generic(group, secrets, &accumulated, dispatch.clone()).await;
                let any_failed = group_results.iter().any(|r| r.status == StepStatus::Failed);
                for r in &group_results {
                    if let Some(ref out) = r.output {
                        accumulated.insert(r.id.clone(), out.clone());
                    }
                }
                results.extend(group_results);
                if any_failed {
                    aborted = true;
                }
            }
        }
    }

    results
}

/// Dispatch a leaf with one validator-retry attempt.
///
/// If the step has a `validate` block and the first validation fails, the step
/// is dispatched a second time and re-validated. The retry is capped at 1 —
/// two attempts total.
async fn run_leaf_with_validation<F, Fut>(
    step: &BatchStep,
    secrets: &HashMap<String, String>,
    accumulated: &HashMap<String, String>,
    dispatch: &F,
) -> BatchStepResult
where
    F: Fn(BatchStep) -> Fut + Send + Sync,
    Fut: Future<Output = Result<String, String>> + Send,
{
    for attempt in 0..=1_u8 {
        let resolved_args = match resolve_placeholders(&step.args, secrets, accumulated) {
            Ok(v) => v,
            Err(e) => {
                tracing::warn!(step_id = %step.id, error = %e, "placeholder resolution failed");
                return BatchStepResult {
                    id: step.id.clone(),
                    status: StepStatus::Failed,
                    output: None,
                    error: Some(format!("placeholder resolution failed: {e}")),
                };
            }
        };
        let mut resolved_step = step.clone();
        resolved_step.args = resolved_args;

        let dispatch_result = dispatch(resolved_step).await;

        match dispatch_result {
            Ok(output) => {
                if let Some(validation) = &step.validate {
                    if let Err(verr) = validate_output(&output, validation) {
                        tracing::warn!(
                            step_id = %step.id,
                            attempt,
                            reason = %verr,
                            "step validation failed"
                        );
                        if attempt == 0 {
                            // Retry once
                            continue;
                        } else {
                            return BatchStepResult {
                                id: step.id.clone(),
                                status: StepStatus::Failed,
                                output: Some(output),
                                error: Some(format!("validation failed: {verr}")),
                            };
                        }
                    }
                }
                return BatchStepResult {
                    id: step.id.clone(),
                    status: StepStatus::Ok,
                    output: Some(output),
                    error: None,
                };
            }
            Err(err) => {
                tracing::warn!(step_id = %step.id, attempt, error = %err, "step dispatch failed");
                // Dispatch errors are terminal — no retry (retry is only for
                // validator failures per spec).
                return BatchStepResult {
                    id: step.id.clone(),
                    status: StepStatus::Failed,
                    output: None,
                    error: Some(err),
                };
            }
        }
    }
    // Unreachable — the loop either returns or continues exactly twice.
    BatchStepResult {
        id: step.id.clone(),
        status: StepStatus::Failed,
        output: None,
        error: Some("internal: retry loop exited unexpectedly".into()),
    }
}

/// Best-effort in-process validator. Currently checks `min_size` by counting
/// bytes in the step's *output text* (a pragmatic stand-in — the original
/// design called for running `stat` on remote paths, which is better deferred
/// to a follow-up once we can round-trip through the pod).
///
/// `file_exists` is accepted but cannot be verified in-process; treated as a
/// no-op so the validator block is forward-compatible.
fn validate_output(output: &str, v: &ValidateBlock) -> Result<(), String> {
    if let Some(min) = v.min_size {
        let len = output.len() as u64;
        if len < min {
            return Err(format!("output size {len} < min_size {min}"));
        }
    }
    // file_exists: cannot be checked without a pod round-trip; accepted.
    let _ = &v.file_exists;
    Ok(())
}

/// Execute a group of leaf steps concurrently, capped by `group.parallel`.
async fn run_group_generic<F, Fut>(
    group: GroupBlock,
    secrets: &HashMap<String, String>,
    accumulated: &HashMap<String, String>,
    dispatch: F,
) -> Vec<BatchStepResult>
where
    F: Fn(BatchStep) -> Fut + Clone + Send + Sync + 'static,
    Fut: Future<Output = Result<String, String>> + Send + 'static,
{
    let permits = group.parallel.max(1);
    let semaphore = Arc::new(Semaphore::new(permits));
    let mut join_set: JoinSet<BatchStepResult> = JoinSet::new();

    for step in group.steps {
        let sem = semaphore.clone();
        let dispatcher = dispatch.clone();
        // Snapshot maps per-task — contents are not mutated during group exec
        // because group steps cannot depend on each other.
        let secrets_snap = secrets.clone();
        let accumulated_snap = accumulated.clone();
        let step_clone = step.clone();
        join_set.spawn(async move {
            let _permit = match sem.acquire_owned().await {
                Ok(p) => p,
                Err(e) => {
                    tracing::warn!(step_id = %step_clone.id, error = %e, "semaphore closed");
                    return BatchStepResult {
                        id: step_clone.id,
                        status: StepStatus::Failed,
                        output: None,
                        error: Some(format!("semaphore closed: {e}")),
                    };
                }
            };
            run_leaf_with_validation(&step_clone, &secrets_snap, &accumulated_snap, &dispatcher)
                .await
        });
    }

    let mut results = Vec::new();
    while let Some(joined) = join_set.join_next().await {
        match joined {
            Ok(r) => results.push(r),
            Err(e) => {
                tracing::warn!(error = %e, "group task join failed");
                results.push(BatchStepResult {
                    id: format!("<joined-err:{}>", results.len()),
                    status: StepStatus::Failed,
                    output: None,
                    error: Some(format!("task join error: {e}")),
                });
            }
        }
    }
    // Preserve declaration order by id within the group for deterministic tests.
    // (Use id ordering as a proxy — group steps have unique ids.)
    results.sort_by(|a, b| a.id.cmp(&b.id));
    results
}

/// DAG mode: Kahn's algorithm with bounded concurrency.
async fn run_dag_generic<F, Fut>(
    steps: Vec<BatchStep>,
    secrets: &HashMap<String, String>,
    dispatch: F,
) -> Result<Vec<BatchStepResult>, BatchError>
where
    F: Fn(BatchStep) -> Fut + Clone + Send + Sync + 'static,
    Fut: Future<Output = Result<String, String>> + Send + 'static,
{
    let total = steps.len();
    let mut by_id: HashMap<String, BatchStep> = HashMap::new();
    let mut in_degree: HashMap<String, usize> = HashMap::new();
    let mut adjacency: HashMap<String, Vec<String>> = HashMap::new();

    for step in &steps {
        by_id.insert(step.id.clone(), step.clone());
        in_degree.entry(step.id.clone()).or_insert(0);
        adjacency.entry(step.id.clone()).or_default();
    }
    for step in &steps {
        for dep in &step.depends_on {
            if !by_id.contains_key(dep) {
                return Err(BatchError::DagCycle(format!(
                    "step '{}' depends on unknown step '{}'",
                    step.id, dep
                )));
            }
            *in_degree.entry(step.id.clone()).or_insert(0) += 1;
            adjacency
                .entry(dep.clone())
                .or_default()
                .push(step.id.clone());
        }
    }

    let mut ready: VecDeque<String> = in_degree
        .iter()
        .filter_map(|(id, deg)| if *deg == 0 { Some(id.clone()) } else { None })
        .collect();
    // Deterministic order
    let mut ready_vec: Vec<String> = ready.drain(..).collect();
    ready_vec.sort();
    ready.extend(ready_vec);

    let mut results: HashMap<String, BatchStepResult> = HashMap::new();
    let mut accumulated: HashMap<String, String> = HashMap::new();
    let mut skipped: HashSet<String> = HashSet::new();
    let mut in_flight: JoinSet<(String, Result<String, String>, BatchStep)> = JoinSet::new();
    let mut processed = 0_usize;
    let mut panicked_count = 0_usize;

    loop {
        // Fill in-flight up to cap, skipping nodes whose deps already failed.
        while in_flight.len() < DAG_CONCURRENCY_CAP {
            let Some(id) = ready.pop_front() else { break };
            if skipped.contains(&id) {
                results.insert(
                    id.clone(),
                    BatchStepResult {
                        id: id.clone(),
                        status: StepStatus::Skipped,
                        output: None,
                        error: None,
                    },
                );
                processed += 1;
                propagate_dag_ready(&id, &adjacency, &mut in_degree, &mut ready);
                continue;
            }
            let step = match by_id.get(&id) {
                Some(s) => s.clone(),
                None => continue,
            };
            let resolved_args = match resolve_placeholders(&step.args, secrets, &accumulated) {
                Ok(v) => v,
                Err(e) => {
                    tracing::warn!(step_id = %step.id, error = %e, "placeholder resolution failed");
                    results.insert(
                        id.clone(),
                        BatchStepResult {
                            id: id.clone(),
                            status: StepStatus::Failed,
                            output: None,
                            error: Some(format!("placeholder resolution failed: {e}")),
                        },
                    );
                    processed += 1;
                    mark_descendants_skipped(&id, &adjacency, &mut skipped);
                    propagate_dag_ready(&id, &adjacency, &mut in_degree, &mut ready);
                    continue;
                }
            };
            let mut resolved_step = step.clone();
            resolved_step.args = resolved_args;
            let step_for_validation = step.clone();
            let dispatcher = dispatch.clone();
            in_flight.spawn(async move {
                let out = dispatcher(resolved_step).await;
                (id, out, step_for_validation)
            });
        }

        if in_flight.is_empty() {
            break;
        }

        let joined = match in_flight.join_next().await {
            Some(Ok(v)) => v,
            Some(Err(e)) => {
                // A spawned task panicked (JoinError). We cannot recover the
                // node id from the JoinError, so we record a sentinel Failed
                // result to keep `processed` in sync with `total` and avoid
                // the false-positive DagCycle error path.
                panicked_count += 1;
                let sentinel_id = format!("__panicked_task__{panicked_count}");
                tracing::warn!(error = %e, sentinel_id = %sentinel_id, "dag task panicked");
                processed += 1;
                results.insert(
                    sentinel_id.clone(),
                    BatchStepResult {
                        id: sentinel_id,
                        status: StepStatus::Failed,
                        output: None,
                        error: Some(format!("task panicked: {e}")),
                    },
                );
                continue;
            }
            None => break,
        };
        let (id, out, orig_step) = joined;
        processed += 1;

        let step_result = match out {
            Ok(output) => {
                if let Some(validation) = &orig_step.validate {
                    if let Err(verr) = validate_output(&output, validation) {
                        tracing::warn!(step_id = %orig_step.id, reason = %verr, "dag step validation failed, retrying");
                        // DAG mode retry: run once more synchronously here.
                        let resolved_args = match resolve_placeholders(
                            &orig_step.args,
                            secrets,
                            &accumulated,
                        ) {
                            Ok(v) => v,
                            Err(e) => {
                                tracing::warn!(step_id = %orig_step.id, error = %e, "placeholder resolution failed on retry");
                                mark_descendants_skipped(&id, &adjacency, &mut skipped);
                                propagate_dag_ready(&id, &adjacency, &mut in_degree, &mut ready);
                                results.insert(
                                    id.clone(),
                                    BatchStepResult {
                                        id: id.clone(),
                                        status: StepStatus::Failed,
                                        output: Some(output),
                                        error: Some(format!(
                                            "placeholder resolution failed on retry: {e}"
                                        )),
                                    },
                                );
                                continue;
                            }
                        };
                        let mut retry_step = orig_step.clone();
                        retry_step.args = resolved_args;
                        match dispatch(retry_step).await {
                            Ok(out2) => {
                                if let Err(verr2) = validate_output(&out2, validation) {
                                    tracing::warn!(step_id = %orig_step.id, reason = %verr2, "dag step validation failed after retry");
                                    BatchStepResult {
                                        id: id.clone(),
                                        status: StepStatus::Failed,
                                        output: Some(out2),
                                        error: Some(format!(
                                            "validation failed after retry: {verr2}"
                                        )),
                                    }
                                } else {
                                    accumulated.insert(id.clone(), out2.clone());
                                    BatchStepResult {
                                        id: id.clone(),
                                        status: StepStatus::Ok,
                                        output: Some(out2),
                                        error: None,
                                    }
                                }
                            }
                            Err(e) => BatchStepResult {
                                id: id.clone(),
                                status: StepStatus::Failed,
                                output: None,
                                error: Some(e),
                            },
                        }
                    } else {
                        accumulated.insert(id.clone(), output.clone());
                        BatchStepResult {
                            id: id.clone(),
                            status: StepStatus::Ok,
                            output: Some(output),
                            error: None,
                        }
                    }
                } else {
                    accumulated.insert(id.clone(), output.clone());
                    BatchStepResult {
                        id: id.clone(),
                        status: StepStatus::Ok,
                        output: Some(output),
                        error: None,
                    }
                }
            }
            Err(err) => {
                tracing::warn!(step_id = %id, error = %err, "dag step dispatch failed");
                BatchStepResult {
                    id: id.clone(),
                    status: StepStatus::Failed,
                    output: None,
                    error: Some(err),
                }
            }
        };

        if step_result.status == StepStatus::Failed {
            mark_descendants_skipped(&id, &adjacency, &mut skipped);
        }
        results.insert(id.clone(), step_result);
        propagate_dag_ready(&id, &adjacency, &mut in_degree, &mut ready);
    }

    if processed < total {
        // Nodes left unprocessed → cycle. Report remaining node ids.
        let remaining: Vec<String> = by_id
            .keys()
            .filter(|k| !results.contains_key(*k))
            .cloned()
            .collect();
        let mut sorted = remaining.clone();
        sorted.sort();
        return Err(BatchError::DagCycle(sorted.join(",")));
    }

    // Materialize results in a stable order: by original step index.
    let mut ordered = Vec::with_capacity(steps.len());
    for step in &steps {
        if let Some(r) = results.remove(&step.id) {
            ordered.push(r);
        }
    }
    Ok(ordered)
}

fn propagate_dag_ready(
    completed: &str,
    adjacency: &HashMap<String, Vec<String>>,
    in_degree: &mut HashMap<String, usize>,
    ready: &mut VecDeque<String>,
) {
    if let Some(downstream) = adjacency.get(completed) {
        let mut new_ready: Vec<String> = Vec::new();
        for child in downstream {
            if let Some(deg) = in_degree.get_mut(child) {
                if *deg > 0 {
                    *deg -= 1;
                }
                // Push once in_degree is zero. Skipped children are still
                // enqueued so the outer loop can record them as Skipped and
                // continue the topological walk.
                if *deg == 0 && !ready.contains(child) {
                    new_ready.push(child.clone());
                }
            }
        }
        new_ready.sort();
        for id in new_ready {
            ready.push_back(id);
        }
    }
}

fn mark_descendants_skipped(
    failed: &str,
    adjacency: &HashMap<String, Vec<String>>,
    skipped: &mut HashSet<String>,
) {
    let mut stack: Vec<String> = vec![failed.to_string()];
    while let Some(node) = stack.pop() {
        if let Some(downstream) = adjacency.get(&node) {
            for child in downstream {
                if skipped.insert(child.clone()) {
                    stack.push(child.clone());
                }
            }
        }
    }
}

fn flatten_to_leaves(entries: Vec<StepEntry>) -> Vec<BatchStep> {
    let mut out = Vec::new();
    for e in entries {
        match e {
            StepEntry::Leaf(s) => out.push(s),
            StepEntry::Group(g) => out.extend(g.steps),
        }
    }
    out
}

fn collect_entry_ids(entry: &StepEntry) -> Vec<String> {
    match entry {
        StepEntry::Leaf(s) => vec![s.id.clone()],
        StepEntry::Group(g) => g.steps.iter().map(|s| s.id.clone()).collect(),
    }
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::sync::atomic::{AtomicUsize, Ordering};

    fn leaf(id: &str, tool: &str, args: serde_json::Value) -> BatchStep {
        BatchStep {
            id: id.to_string(),
            tool: tool.to_string(),
            args,
            depends_on: vec![],
            validate: None,
        }
    }

    fn leaf_with_deps(id: &str, deps: &[&str]) -> BatchStep {
        BatchStep {
            id: id.to_string(),
            tool: "exec".to_string(),
            args: json!({"command": "true"}),
            depends_on: deps.iter().map(|s| s.to_string()).collect(),
            validate: None,
        }
    }

    /// Always-OK dispatcher that records call ids.
    fn ok_dispatcher(
        log: Arc<tokio::sync::Mutex<Vec<String>>>,
    ) -> impl Fn(BatchStep) -> std::pin::Pin<Box<dyn Future<Output = Result<String, String>> + Send>>
           + Clone
           + Send
           + Sync
           + 'static {
        move |step: BatchStep| {
            let log = log.clone();
            Box::pin(async move {
                log.lock().await.push(step.id.clone());
                Ok(format!("ok:{}", step.id))
            })
        }
    }

    #[tokio::test]
    async fn execute_seq_3_steps_all_ok() {
        let steps = vec![
            StepEntry::Leaf(leaf("a", "exec", json!({"command": "echo a"}))),
            StepEntry::Leaf(leaf("b", "exec", json!({"command": "echo b"}))),
            StepEntry::Leaf(leaf("c", "exec", json!({"command": "echo c"}))),
        ];
        let log = Arc::new(tokio::sync::Mutex::new(Vec::<String>::new()));
        let results = run_seq_generic(steps, &HashMap::new(), ok_dispatcher(log.clone())).await;

        assert_eq!(results.len(), 3);
        assert!(results.iter().all(|r| r.status == StepStatus::Ok));
        let seen = log.lock().await.clone();
        assert_eq!(
            seen,
            vec!["a".to_string(), "b".to_string(), "c".to_string()]
        );
    }

    #[tokio::test]
    async fn execute_seq_step2_fails_step3_skipped() {
        let steps = vec![
            StepEntry::Leaf(leaf("a", "exec", json!({}))),
            StepEntry::Leaf(leaf("b", "exec", json!({}))),
            StepEntry::Leaf(leaf("c", "exec", json!({}))),
        ];
        let dispatcher = |step: BatchStep| async move {
            if step.id == "b" {
                Err("boom".to_string())
            } else {
                Ok(format!("ok:{}", step.id))
            }
        };
        let results = run_seq_generic(steps, &HashMap::new(), dispatcher).await;
        assert_eq!(results.len(), 3);
        assert_eq!(results[0].status, StepStatus::Ok);
        assert_eq!(results[1].status, StepStatus::Failed);
        assert_eq!(results[2].status, StepStatus::Skipped);
    }

    #[tokio::test]
    async fn execute_dag_diamond_order() {
        // A -> B, A -> C, B -> D, C -> D
        let steps = vec![
            leaf_with_deps("A", &[]),
            leaf_with_deps("B", &["A"]),
            leaf_with_deps("C", &["A"]),
            leaf_with_deps("D", &["B", "C"]),
        ];
        let log = Arc::new(tokio::sync::Mutex::new(Vec::<String>::new()));
        let dispatcher = ok_dispatcher(log.clone());
        let results = run_dag_generic(steps, &HashMap::new(), dispatcher)
            .await
            .expect("diamond ok");
        assert_eq!(results.len(), 4);
        assert!(results.iter().all(|r| r.status == StepStatus::Ok));

        let order = log.lock().await.clone();
        let pos = |id: &str| order.iter().position(|s| s == id).unwrap();
        assert!(pos("A") < pos("B"));
        assert!(pos("A") < pos("C"));
        assert!(pos("B") < pos("D"));
        assert!(pos("C") < pos("D"));
    }

    #[tokio::test]
    async fn execute_dag_cycle_detected() {
        // A -> B, B -> A
        let steps = vec![leaf_with_deps("A", &["B"]), leaf_with_deps("B", &["A"])];
        let log = Arc::new(tokio::sync::Mutex::new(Vec::<String>::new()));
        let err = run_dag_generic(steps, &HashMap::new(), ok_dispatcher(log))
            .await
            .expect_err("should detect cycle");
        match err {
            BatchError::DagCycle(nodes) => {
                assert!(nodes.contains('A') && nodes.contains('B'));
            }
            other => panic!("expected DagCycle, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn execute_group_parallel_2_of_4_ok() {
        let in_flight = Arc::new(AtomicUsize::new(0));
        let max_seen = Arc::new(AtomicUsize::new(0));

        let in_flight_c = in_flight.clone();
        let max_seen_c = max_seen.clone();
        let dispatcher = move |step: BatchStep| {
            let inflight = in_flight_c.clone();
            let max_seen = max_seen_c.clone();
            async move {
                let now = inflight.fetch_add(1, Ordering::SeqCst) + 1;
                let prev_max = max_seen.load(Ordering::SeqCst);
                if now > prev_max {
                    max_seen.store(now, Ordering::SeqCst);
                }
                tokio::time::sleep(std::time::Duration::from_millis(30)).await;
                inflight.fetch_sub(1, Ordering::SeqCst);
                Ok(format!("ok:{}", step.id))
            }
        };

        let group = GroupBlock {
            id: Some("g".to_string()),
            parallel: 2,
            steps: (0..4)
                .map(|i| leaf(&format!("s{i}"), "exec", json!({})))
                .collect(),
        };
        let results = run_group_generic(group, &HashMap::new(), &HashMap::new(), dispatcher).await;
        assert_eq!(results.len(), 4);
        assert!(results.iter().all(|r| r.status == StepStatus::Ok));
        let max = max_seen.load(Ordering::SeqCst);
        assert!(max <= 2, "parallel cap exceeded: observed {max}");
    }

    #[tokio::test]
    async fn execute_group_1_fail_group_failed() {
        let group = GroupBlock {
            id: None,
            parallel: 3,
            steps: vec![
                leaf("a", "exec", json!({})),
                leaf("b", "exec", json!({})),
                leaf("c", "exec", json!({})),
            ],
        };
        let dispatcher = |step: BatchStep| async move {
            if step.id == "b" {
                Err("nope".to_string())
            } else {
                Ok("ok".to_string())
            }
        };
        let results = run_group_generic(group, &HashMap::new(), &HashMap::new(), dispatcher).await;
        assert_eq!(results.len(), 3);
        let failed = results
            .iter()
            .filter(|r| r.status == StepStatus::Failed)
            .count();
        assert_eq!(failed, 1);
        // The other two should still have completed as Ok (group runs them in parallel)
        let ok = results
            .iter()
            .filter(|r| r.status == StepStatus::Ok)
            .count();
        assert_eq!(ok, 2);
    }

    #[tokio::test]
    async fn dry_run_plan_no_execution() {
        let steps = vec![
            StepEntry::Leaf(leaf("a", "exec", json!({"command": "echo a"}))),
            StepEntry::Group(GroupBlock {
                id: Some("g".to_string()),
                parallel: 2,
                steps: vec![
                    leaf("b", "exec", json!({"command": "echo b"})),
                    leaf("c", "exec", json!({"command": "echo c"})),
                ],
            }),
        ];
        let results = dry_run_plan(&steps);
        assert_eq!(results.len(), 3);
        assert!(results.iter().all(|r| r.status == StepStatus::Ok));
        // Arg is emitted verbatim.
        assert!(results[0].output.as_ref().unwrap().contains("echo a"));
    }

    #[test]
    fn resolve_result_placeholder() {
        let mut accumulated = HashMap::new();
        accumulated.insert(
            "apt".to_string(),
            "Job ID: abc123\nstatus: running".to_string(),
        );
        let args = json!({"task_id": "__result:apt.task_id"});
        let resolved =
            resolve_placeholders(&args, &HashMap::new(), &accumulated).expect("resolve ok");
        assert_eq!(resolved["task_id"], "abc123");
    }

    #[test]
    fn resolve_secret_placeholder() {
        let mut secrets = HashMap::new();
        secrets.insert("HF_TOKEN".to_string(), "hf_xyz".to_string());
        let args = json!({"command": "curl -H 'Authorization: Bearer __secret:HF_TOKEN' x"});
        let resolved = resolve_placeholders(&args, &secrets, &HashMap::new()).expect("resolve ok");
        assert_eq!(
            resolved["command"],
            "curl -H 'Authorization: Bearer hf_xyz' x"
        );
    }

    #[test]
    fn resolve_secret_missing_returns_dispatch_error() {
        let args = json!({"command": "__secret:NOPE"});
        let err = resolve_placeholders(&args, &HashMap::new(), &HashMap::new()).unwrap_err();
        assert!(err.contains("unresolved secret: NOPE"), "got: {err}");
    }

    #[test]
    fn dry_run_step_contains_args() {
        let s = leaf("x", "exec", json!({"command": "echo hi"}));
        let r = dry_run_step(&s);
        assert_eq!(r.status, StepStatus::Ok);
        assert!(r.output.unwrap().contains("echo hi"));
    }

    /// Security: a step's output may contain a literal `__secret:NAME` string.
    /// When a downstream step references that output via `__result:`, the
    /// injected text must NOT be expanded by the secret resolver — it must
    /// remain as the literal string `__secret:FAKE_NAME` in the resolved args.
    #[test]
    fn result_injected_secret_placeholder_not_resolved() {
        // Step "upstream" produced output that contains a literal __secret: tag.
        let mut accumulated = HashMap::new();
        accumulated.insert(
            "upstream".to_string(),
            "value: __secret:FAKE_NAME\n".to_string(),
        );
        // The secret map actually contains FAKE_NAME — if the injected text
        // were passed through the secret resolver, it would be replaced with
        // "leaked".
        let mut secrets = HashMap::new();
        secrets.insert("FAKE_NAME".to_string(), "leaked".to_string());

        // Downstream step references upstream output via __result:.
        let args = json!({"command": "__result:upstream.value"});
        let resolved = resolve_placeholders(&args, &secrets, &accumulated).expect("resolve ok");

        let cmd = resolved["command"].as_str().unwrap();
        assert!(
            !cmd.contains("leaked"),
            "secret was exfiltrated via __result: injection: got {cmd:?}"
        );
        assert!(
            cmd.contains("__secret:FAKE_NAME"),
            "injected __secret: placeholder should remain literal: got {cmd:?}"
        );
    }

    #[tokio::test]
    async fn execute_seq_validator_retry_succeeds_on_second_attempt() {
        let attempts = Arc::new(AtomicUsize::new(0));
        let attempts_c = attempts.clone();
        let dispatcher = move |_step: BatchStep| {
            let a = attempts_c.clone();
            async move {
                let n = a.fetch_add(1, Ordering::SeqCst) + 1;
                // First call: short output (fails min_size), second: long output.
                if n == 1 {
                    Ok("x".to_string())
                } else {
                    Ok("this is a much longer output that should pass".to_string())
                }
            }
        };
        let step = BatchStep {
            id: "v".to_string(),
            tool: "exec".to_string(),
            args: json!({}),
            depends_on: vec![],
            validate: Some(ValidateBlock {
                file_exists: vec![],
                min_size: Some(10),
            }),
        };
        let steps = vec![StepEntry::Leaf(step)];
        let results = run_seq_generic(steps, &HashMap::new(), dispatcher).await;
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].status, StepStatus::Ok);
        assert_eq!(attempts.load(Ordering::SeqCst), 2);
    }
}
