//! Unit tests for `batch_service`. Split from the main module file to keep
//! the production source under the review-friendly line budget.
//! Activated via `#[cfg(test)] #[path] mod tests;` from `batch_service.rs`.

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
    let results = run_seq_generic(steps, &HashMap::new(), ok_dispatcher(log.clone()), None).await;

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
    let results = run_seq_generic(steps, &HashMap::new(), dispatcher, None).await;
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
    let results = run_dag_generic(steps, &HashMap::new(), dispatcher, None)
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
    let err = run_dag_generic(steps, &HashMap::new(), ok_dispatcher(log), None)
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
    let resolved = resolve_placeholders(&args, &HashMap::new(), &accumulated).expect("resolve ok");
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
async fn execute_dag_validator_retry_succeeds_on_second_attempt() {
    // DAG mode: first dispatch returns output too short for min_size,
    // second dispatch (retry) returns long enough output. Final status
    // must be Ok and the dispatcher must have been called exactly twice.
    let attempts = Arc::new(AtomicUsize::new(0));
    let attempts_c = attempts.clone();
    let dispatcher = move |_step: BatchStep| {
        let a = attempts_c.clone();
        async move {
            let n = a.fetch_add(1, Ordering::SeqCst) + 1;
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
    let results = run_dag_generic(vec![step], &HashMap::new(), dispatcher, None)
        .await
        .expect("dag ok");
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].status, StepStatus::Ok);
    assert_eq!(attempts.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn execute_dag_validator_fail_after_retry_marks_descendants_skipped() {
    // A -> B. A's validator fails both attempts. B must be Skipped.
    let dispatcher = |_step: BatchStep| async move { Ok("x".to_string()) };
    let a = BatchStep {
        id: "A".to_string(),
        tool: "exec".to_string(),
        args: json!({}),
        depends_on: vec![],
        validate: Some(ValidateBlock {
            file_exists: vec![],
            min_size: Some(100),
        }),
    };
    let b = leaf_with_deps("B", &["A"]);
    let results = run_dag_generic(vec![a, b], &HashMap::new(), dispatcher, None)
        .await
        .expect("dag ok");
    let by_id: HashMap<&str, &BatchStepResult> =
        results.iter().map(|r| (r.id.as_str(), r)).collect();
    assert_eq!(by_id["A"].status, StepStatus::Failed);
    assert_eq!(by_id["B"].status, StepStatus::Skipped);
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
    let results = run_seq_generic(steps, &HashMap::new(), dispatcher, None).await;
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].status, StepStatus::Ok);
    assert_eq!(attempts.load(Ordering::SeqCst), 2);
}

// ============================================================================
// build_exec_request (profile-shape → VdslExecRequest)
// ============================================================================

#[test]
fn build_exec_request_profile_shape_no_env() {
    let args = json!({
        "pod_id": "pod_abc",
        "script": "set -e\necho hi\n",
        "env": {}
    });
    let req = build_exec_request(&args).expect("must build");
    assert_eq!(req.pod_id.as_deref(), Some("pod_abc"));
    assert_eq!(req.command, "set -e\necho hi\n");
    assert_eq!(req.timeout, None);
}

#[test]
fn build_exec_request_profile_shape_env_prefix_is_deterministic() {
    let args = json!({
        "pod_id": "pod_abc",
        "script": "echo $FOO",
        "env": {
            "ZED": "last",
            "ALPHA": "first"
        }
    });
    let req = build_exec_request(&args).expect("must build");
    // Keys are sorted so prefix ordering is deterministic.
    assert_eq!(
        req.command,
        "export ALPHA='first'; export ZED='last'; echo $FOO"
    );
}

#[test]
fn build_exec_request_profile_shape_escapes_single_quotes_and_metachars() {
    let args = json!({
        "pod_id": "pod_abc",
        "script": "echo $X",
        "env": {
            "X": "it's a $VAR; rm -rf /"
        }
    });
    let req = build_exec_request(&args).expect("must build");
    // Single quotes are escaped via '"'"' so the value remains a single argv
    // and cannot break out of the quoted region.
    assert_eq!(
        req.command,
        r#"export X='it'"'"'s a $VAR; rm -rf /'; echo $X"#
    );
}

#[test]
fn build_exec_request_direct_shape_passes_through() {
    let args = json!({
        "pod_id": "pod_abc",
        "command": "ls /workspace",
        "timeout": 60
    });
    let req = build_exec_request(&args).expect("must build");
    assert_eq!(req.command, "ls /workspace");
    assert_eq!(req.timeout, Some(60));
}

#[test]
fn build_exec_request_rejects_non_object() {
    let err = build_exec_request(&json!("not an object")).unwrap_err();
    assert!(err.contains("must be a JSON object"));
}

#[test]
fn build_exec_request_rejects_non_string_env_value() {
    let args = json!({
        "pod_id": "p",
        "script": "echo",
        "env": { "K": 42 }
    });
    let err = build_exec_request(&args).unwrap_err();
    assert!(err.contains("env[K]"), "got: {err}");
}

// =============================================================================
// execute_with_progress — per-step progress streaming via ApplyRegistry
// =============================================================================

/// After a 3-step seq plan completes, registry must show completed_steps == 3
/// and results.len() == 3.
#[tokio::test]
async fn execute_with_progress_3steps_all_ok_registry_complete() {
    let steps = vec![
        StepEntry::Leaf(leaf("s1", "exec", json!({"command": "echo 1"}))),
        StepEntry::Leaf(leaf("s2", "exec", json!({"command": "echo 2"}))),
        StepEntry::Leaf(leaf("s3", "exec", json!({"command": "echo 3"}))),
    ];
    let registry = ApplyRegistry::new();
    let state = ApplyRunState::new("apply_test_3".into(), "pod_x".into(), 3);
    let handle = registry.insert(state);

    let dispatcher = ok_dispatcher(Arc::new(tokio::sync::Mutex::new(Vec::new())));
    run_seq_generic(steps, &HashMap::new(), dispatcher, Some(handle)).await;

    let snap = registry
        .snapshot("apply_test_3")
        .await
        .expect("state present");
    assert_eq!(snap.completed_steps, 3);
    assert_eq!(snap.results.len(), 3);
    // After the final step, current_step is None (mark_step_complete sets it to None).
    assert!(snap.current_step.is_none());
}

/// During seq execution, mark_step_started is called before each dispatch,
/// so current_step changes as each step starts. We verify this by interleaving
/// a registry snapshot inside a delayed dispatcher.
#[tokio::test]
async fn execute_with_progress_intermediate_current_step_observed() {
    let registry = Arc::new(ApplyRegistry::new());
    let state = ApplyRunState::new("apply_test_mid".into(), "pod_x".into(), 2);
    let handle = registry.insert(state);
    let registry_c = registry.clone();

    // step s1: captures the current_step snapshot while "executing"
    let observed_during_s1: Arc<tokio::sync::Mutex<Option<String>>> =
        Arc::new(tokio::sync::Mutex::new(None));
    let observed_c = observed_during_s1.clone();

    let dispatcher = move |step: BatchStep| {
        let reg = registry_c.clone();
        let obs = observed_c.clone();
        async move {
            if step.id == "s1" {
                // Snapshot the registry while s1 is "in-flight".
                // mark_step_started was already called before dispatch, so
                // current_step should be Some("s1").
                let snap = reg.snapshot("apply_test_mid").await;
                *obs.lock().await = snap.and_then(|s| s.current_step);
            }
            Ok(format!("ok:{}", step.id))
        }
    };

    let steps = vec![
        StepEntry::Leaf(leaf("s1", "exec", json!({}))),
        StepEntry::Leaf(leaf("s2", "exec", json!({}))),
    ];
    run_seq_generic(steps, &HashMap::new(), dispatcher, Some(handle)).await;

    let observed = observed_during_s1.lock().await.clone();
    assert_eq!(
        observed.as_deref(),
        Some("s1"),
        "current_step should be 's1' while s1 is dispatching"
    );

    let snap = registry
        .snapshot("apply_test_mid")
        .await
        .expect("state present");
    assert_eq!(snap.completed_steps, 2);
}

/// execute (no handle) is a thin wrapper — must not regress existing behavior.
#[tokio::test]
async fn execute_without_progress_handle_runs_normally() {
    let steps = vec![
        StepEntry::Leaf(leaf("a", "exec", json!({"command": "echo a"}))),
        StepEntry::Leaf(leaf("b", "exec", json!({"command": "echo b"}))),
    ];
    let log = Arc::new(tokio::sync::Mutex::new(Vec::<String>::new()));
    let results = run_seq_generic(steps, &HashMap::new(), ok_dispatcher(log.clone()), None).await;
    assert_eq!(results.len(), 2);
    assert!(results.iter().all(|r| r.status == StepStatus::Ok));
    let seen = log.lock().await.clone();
    assert_eq!(seen, vec!["a".to_string(), "b".to_string()]);
}

// =============================================================================
// count_leaf_steps — used by run_background to size the apply registry state
// =============================================================================

#[test]
fn count_leaf_steps_empty_plan_is_zero() {
    assert_eq!(count_leaf_steps(&[]), 0);
}

#[test]
fn count_leaf_steps_flat_leaves_counts_each() {
    let steps = vec![
        StepEntry::Leaf(leaf("a", "exec", json!({"command": "true"}))),
        StepEntry::Leaf(leaf("b", "exec_bg", json!({"command": "true"}))),
    ];
    assert_eq!(count_leaf_steps(&steps), 2);
}

#[test]
fn count_leaf_steps_groups_flatten_to_children() {
    let group = GroupBlock {
        id: Some("g".to_string()),
        parallel: 2,
        steps: vec![
            leaf("g1", "exec_bg", json!({"command": "true"})),
            leaf("g2", "exec_bg", json!({"command": "true"})),
            leaf("g3", "exec_bg", json!({"command": "true"})),
        ],
    };
    let steps = vec![
        StepEntry::Leaf(leaf("pre", "exec", json!({"command": "true"}))),
        StepEntry::Group(group),
        StepEntry::Leaf(leaf("post", "exec", json!({"command": "true"}))),
    ];
    assert_eq!(count_leaf_steps(&steps), 5);
}

// ============================================================================
// truncate_step_output (A fix)
// ============================================================================

#[test]
fn truncate_step_output_under_limit_returns_unchanged() {
    let s = "small log".to_string();
    assert_eq!(truncate_step_output(s.clone()), s);
}

#[test]
fn truncate_step_output_over_limit_truncates_with_marker() {
    let big = "x".repeat(STEP_OUTPUT_MAX_BYTES + 100);
    let out = truncate_step_output(big);
    assert!(out.ends_with("[…truncated…]"), "must mark truncation: {out}");
    assert!(
        out.len() < STEP_OUTPUT_MAX_BYTES + 50,
        "must shrink to ~ STEP_OUTPUT_MAX_BYTES; got {} bytes",
        out.len()
    );
}

#[test]
fn truncate_step_output_respects_utf8_boundary() {
    // Multibyte chars at the boundary must not produce invalid UTF-8.
    let s = "日".repeat(STEP_OUTPUT_MAX_BYTES); // each char is 3 bytes
    let out = truncate_step_output(s);
    // String::is_char_boundary is implicit — if invalid, format!() panics.
    assert!(out.ends_with("[…truncated…]"));
}
