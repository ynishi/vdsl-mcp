//! Unit tests for `profile_service`. Split from the main module file to keep
//! the production source under the review-friendly line budget.
//! Activated via `#[cfg(test)] #[path] mod tests;` from `profile_service.rs`.

use super::*;
use crate::domain::profile::{ComfyUiConfig, Hooks, PythonConfig, SecretRef, SystemConfig};

fn minimal_manifest_json() -> String {
    serde_json::json!({
        "schema": "vdsl.profile/1",
        "name": "minimal",
        "comfyui": { "ref": "v0.3.10" }
    })
    .to_string()
}

fn full_manifest() -> ProfileManifest {
    ProfileManifest {
        schema: PROFILE_SCHEMA.to_string(),
        name: "full".to_string(),
        comfyui: Some(ComfyUiConfig {
            ref_: "master".to_string(),
            repo: None,
            args: Some(vec!["--lowvram".to_string()]),
            port: Some(8188),
        }),
        system: Some(SystemConfig {
            apt: vec!["git".to_string(), "curl".to_string()],
        }),
        python: Some(PythonConfig {
            deps: vec!["numpy".to_string()],
        }),
        custom_nodes: vec![crate::domain::profile::CustomNode {
            name: "ComfyUI-Manager".to_string(),
            repo: "https://github.com/ltdrdata/ComfyUI-Manager".to_string(),
            ref_: None,
            pip: None,
        }],
        sync: Some(crate::domain::profile::SyncConfig {
            pull: vec![SyncRoute {
                src: "b2://bucket/models/".to_string(),
                dst: "/workspace/ComfyUI/models/".to_string(),
            }],
            push: vec![SyncRoute {
                src: "/workspace/ComfyUI/output/".to_string(),
                dst: "b2://bucket/output/{pod_id}/".to_string(),
            }],
        }),
        staging: Some(crate::domain::profile::StagingConfig {
            push: vec![SyncRoute {
                src: "/workspace/staging/a.safetensors".to_string(),
                dst: "b2://bucket/staging/a.safetensors".to_string(),
            }],
        }),
        models: vec![
            Model {
                src: "b2://bucket/sdxl.safetensors".to_string(),
                dst: "sdxl.safetensors".to_string(),
                kind: "checkpoint".to_string(),
                subdir: "checkpoints".to_string(),
            },
            Model {
                src: "file:///mnt/models/lora.safetensors".to_string(),
                dst: "lora.safetensors".to_string(),
                kind: "lora".to_string(),
                subdir: "loras".to_string(),
            },
        ],
        env: HashMap::new(),
        hooks: Some(Hooks {
            post_install: Some("echo done".to_string()),
        }),
    }
}

// ----- parse_manifest -----

#[test]
fn parse_manifest_accepts_valid_json() {
    let m = parse_manifest(&minimal_manifest_json()).expect("parse ok");
    assert_eq!(m.schema, PROFILE_SCHEMA);
    assert_eq!(m.name, "minimal");
    assert_eq!(m.comfyui.as_ref().expect("comfyui present").ref_, "v0.3.10");
}

#[test]
fn parse_manifest_rejects_wrong_schema() {
    let json = serde_json::json!({
        "schema": "vdsl.profile/999",
        "name": "bad",
        "comfyui": { "ref": "x" }
    })
    .to_string();
    let err = parse_manifest(&json).unwrap_err();
    match err {
        ProfileError::UnsupportedSchema { got, .. } => {
            assert_eq!(got, "vdsl.profile/999");
        }
        other => panic!("expected UnsupportedSchema, got {other:?}"),
    }
}

#[test]
fn parse_manifest_rejects_missing_required_fields() {
    let json = r#"{ "schema": "vdsl.profile/1" }"#;
    let err = parse_manifest(json).unwrap_err();
    assert!(matches!(err, ProfileError::InvalidManifest(_)));
}

#[test]
fn parse_manifest_accepts_plain_env_values() {
    let json = serde_json::json!({
        "schema": "vdsl.profile/1",
        "name": "env-test",
        "comfyui": { "ref": "x" },
        "env": {
            "DEBUG": "1",
            "COMFYUI_PORT": "8188"
        }
    })
    .to_string();
    let m = parse_manifest(&json).expect("parse ok");
    match m.env.get("DEBUG") {
        Some(EnvValue::Plain(s)) => assert_eq!(s, "1"),
        other => panic!("expected Plain, got {other:?}"),
    }
    match m.env.get("COMFYUI_PORT") {
        Some(EnvValue::Plain(s)) => assert_eq!(s, "8188"),
        other => panic!("expected Plain, got {other:?}"),
    }
}

#[test]
fn parse_manifest_rejects_user_supplied_secret_sentinel() {
    // `{"__secret": "NAME"}` is an MCP-internal emission shape. A user
    // profile that contains it is trying to smuggle a credential ref —
    // must be rejected by parse_manifest (defence-in-depth alongside
    // the Lua DSL's normalize_env).
    let json = serde_json::json!({
        "schema": "vdsl.profile/1",
        "name": "bad",
        "comfyui": { "ref": "x" },
        "env": {
            "NEUTRAL_NAME": { "__secret": "VDSL_B2_KEY" }
        }
    })
    .to_string();
    let err = parse_manifest(&json).unwrap_err();
    match err {
        ProfileError::SecretInUserEnv { key, reason } => {
            assert_eq!(key, "NEUTRAL_NAME");
            assert!(
                reason.contains("__secret"),
                "reason should name the sentinel: {reason}"
            );
        }
        other => panic!("expected SecretInUserEnv, got {other:?}"),
    }
    // Sanity-check `SecretRef` is still reachable for MCP-internal use
    // (the domain type is not deprecated, only user-facing parsing is).
    let _ = SecretRef {
        name: "x".to_string(),
    };
}

#[test]
fn parse_manifest_rejects_secret_shaped_env_key() {
    // Key matches SECRET_KEY_SUBSTRINGS (case-insensitive). Profile.env
    // is non-secret runtime config only — credentials are MCP-owned.
    for key in ["HF_TOKEN", "my_api_key", "DB_PASSWORD", "aws_secret"] {
        let json = serde_json::json!({
            "schema": "vdsl.profile/1",
            "name": "bad",
            "comfyui": { "ref": "x" },
            "env": { key: "literal_value" }
        })
        .to_string();
        let err = parse_manifest(&json).unwrap_err();
        match err {
            ProfileError::SecretInUserEnv { key: reported, .. } => {
                assert_eq!(reported, key);
            }
            other => panic!("expected SecretInUserEnv for key {key:?}, got {other:?}"),
        }
    }
}

// ----- resolve_secrets -----

#[test]
fn resolve_secrets_reads_env_var() {
    // Unique var name to avoid flakiness / cross-test interference.
    let var = "VDSL_MCP_TEST_SECRET_OK_X";
    // SAFETY: test-only; single-threaded within this #[test] fn.
    unsafe {
        std::env::set_var(var, "s3cret");
    }

    let mut manifest = ProfileManifest {
        schema: PROFILE_SCHEMA.to_string(),
        name: "s".to_string(),
        comfyui: Some(ComfyUiConfig {
            ref_: "x".to_string(),
            repo: None,
            args: None,
            port: None,
        }),
        system: None,
        python: None,
        custom_nodes: vec![],
        sync: None,
        staging: None,
        models: vec![],
        env: HashMap::new(),
        hooks: None,
    };
    manifest.env.insert(
        "FOO".to_string(),
        EnvValue::Secret(SecretRef {
            name: var.to_string(),
        }),
    );
    manifest
        .env
        .insert("BAR".to_string(), EnvValue::Plain("bar_value".to_string()));

    let resolved = resolve_secrets(&manifest).expect("resolve ok");
    assert_eq!(resolved.get("FOO").map(|s| s.as_str()), Some("s3cret"));
    assert_eq!(resolved.get("BAR").map(|s| s.as_str()), Some("bar_value"));

    unsafe {
        std::env::remove_var(var);
    }
}

#[test]
fn resolve_secrets_collects_all_missing() {
    let mut manifest = ProfileManifest {
        schema: PROFILE_SCHEMA.to_string(),
        name: "s".to_string(),
        comfyui: Some(ComfyUiConfig {
            ref_: "x".to_string(),
            repo: None,
            args: None,
            port: None,
        }),
        system: None,
        python: None,
        custom_nodes: vec![],
        sync: None,
        staging: None,
        models: vec![],
        env: HashMap::new(),
        hooks: None,
    };
    manifest.env.insert(
        "A".to_string(),
        EnvValue::Secret(SecretRef {
            name: "VDSL_MCP_TEST_MISSING_A_ZZ".to_string(),
        }),
    );
    manifest.env.insert(
        "B".to_string(),
        EnvValue::Secret(SecretRef {
            name: "VDSL_MCP_TEST_MISSING_B_ZZ".to_string(),
        }),
    );

    let err = resolve_secrets(&manifest).unwrap_err();
    match err {
        ProfileError::MissingSecrets(mut names) => {
            names.sort();
            assert_eq!(
                names,
                vec![
                    "VDSL_MCP_TEST_MISSING_A_ZZ".to_string(),
                    "VDSL_MCP_TEST_MISSING_B_ZZ".to_string(),
                ]
            );
        }
        other => panic!("expected MissingSecrets, got {other:?}"),
    }
}

// ----- expand_phases -----

fn leaf_ids(plan: &BatchPlan) -> Vec<String> {
    let mut ids = Vec::new();
    for entry in &plan.steps {
        match entry {
            StepEntry::Leaf(s) => ids.push(s.id.clone()),
            StepEntry::Group(g) => {
                if let Some(id) = &g.id {
                    ids.push(id.clone());
                }
                for s in &g.steps {
                    ids.push(s.id.clone());
                }
            }
        }
    }
    ids
}

#[test]
fn expand_phases_minimal_manifest_emits_2_9_10() {
    let m = parse_manifest(&minimal_manifest_json()).expect("parse ok");
    let plan = expand_phases(&m, "abc", false).expect("ok");
    let ids = leaf_ids(&plan);

    assert!(ids.iter().any(|i| i == "2_comfyui_install"));
    assert!(ids.iter().any(|i| i == "9_comfyui_restart"));
    assert!(ids.iter().any(|i| i == "10_health"));

    // Phases that should NOT be present with an empty manifest:
    assert!(!ids.iter().any(|i| i == "1_system_apt"));
    assert!(!ids.iter().any(|i| i == "3_python_deps"));
    assert!(!ids.iter().any(|i| i.starts_with("4_custom_node_")));
    assert!(!ids.iter().any(|i| i.starts_with("5_sync_")));
    assert!(!ids.iter().any(|i| i.starts_with("6_sync_poll_")));
    assert!(!ids.iter().any(|i| i.starts_with("7_model_")));
    assert!(!ids.iter().any(|i| i == "8_post_install"));
}

#[test]
fn expand_phases_full_manifest_emits_all_phases_and_correct_tools() {
    let m = full_manifest();
    let plan = expand_phases(&m, "abc", false).expect("ok");

    // Walk the plan and collect (id, tool) pairs for every leaf.
    let mut pairs: Vec<(String, String)> = Vec::new();
    for entry in &plan.steps {
        match entry {
            StepEntry::Leaf(s) => pairs.push((s.id.clone(), s.tool.clone())),
            StepEntry::Group(g) => {
                for s in &g.steps {
                    pairs.push((s.id.clone(), s.tool.clone()));
                }
            }
        }
    }

    let find = |id: &str| pairs.iter().find(|(i, _)| i == id).map(|(_, t)| t.clone());

    // Heavy phases (installs, git clones, pip, rclone, restart, user hooks)
    // use `exec_bg` so SSH stays held only during launch + per-poll status
    // queries; pod-side work runs detached via `task_run`. See the
    // 2026-04-22 accident — the pre-fix `exec` path regularly deadlocked
    // the SSH channel for ~1h on a pip install that had actually
    // completed on the pod. Only the marker-append `sync.push` stays on
    // plain `exec` (fast, single-line write).
    assert_eq!(find("1_system_apt"), Some("exec_bg".to_string()));
    assert_eq!(find("2_comfyui_install"), Some("exec_bg".to_string()));
    assert_eq!(find("3_python_deps"), Some("exec_bg".to_string()));
    assert_eq!(find("4_custom_node_0"), Some("exec_bg".to_string()));
    assert_eq!(find("5_sync_pull_0"), Some("exec_bg".to_string())); // rclone copyto
    assert_eq!(find("5_sync_push_0"), Some("exec".to_string())); // marker append — light
    assert_eq!(find("5_staging_push_0"), Some("exec_bg".to_string())); // eager pod → B2
    assert_eq!(find("6_sync_poll_0"), None); // Phase 6 unused
    assert_eq!(find("7_model_0"), Some("exec_bg".to_string())); // b2:// → rclone
    assert_eq!(find("7_model_1"), Some("exec_bg".to_string())); // file:// cp
    assert_eq!(find("8_post_install"), Some("exec_bg".to_string()));
    assert_eq!(find("9_comfyui_restart"), Some("exec_bg".to_string()));
    assert_eq!(find("10_health"), Some("comfy_api".to_string()));
}

#[test]
fn expand_phases_rejects_pull_with_non_b2_src() {
    let mut m = full_manifest();
    m.sync.as_mut().unwrap().pull[0].src = "s3://bucket/x".to_string();
    let err = expand_phases(&m, "abc", false).unwrap_err();
    assert!(matches!(err, ProfileError::InvalidManifest(_)));
}

#[test]
fn expand_phases_rejects_pull_with_relative_dst() {
    let mut m = full_manifest();
    m.sync.as_mut().unwrap().pull[0].dst = "relative/path".to_string();
    let err = expand_phases(&m, "abc", false).unwrap_err();
    assert!(matches!(err, ProfileError::InvalidManifest(_)));
}

#[test]
fn expand_phases_rejects_push_with_non_b2_dst() {
    let mut m = full_manifest();
    m.sync.as_mut().unwrap().push[0].dst = "s3://bucket/x".to_string();
    let err = expand_phases(&m, "abc", false).unwrap_err();
    assert!(matches!(err, ProfileError::InvalidManifest(_)));
}

#[test]
fn expand_phases_rejects_push_with_relative_src() {
    let mut m = full_manifest();
    m.sync.as_mut().unwrap().push[0].src = "workspace/output".to_string();
    let err = expand_phases(&m, "abc", false).unwrap_err();
    assert!(matches!(err, ProfileError::InvalidManifest(_)));
}

#[test]
fn expand_phases_rejects_path_traversal_in_pull_dst() {
    let mut m = full_manifest();
    m.sync.as_mut().unwrap().pull[0].dst = "/workspace/../etc".to_string();
    let err = expand_phases(&m, "abc", false).unwrap_err();
    assert!(matches!(err, ProfileError::InvalidManifest(_)));
}

// ----- staging.push -----

#[test]
fn expand_phases_rejects_staging_push_with_non_b2_dst() {
    let mut m = full_manifest();
    m.staging.as_mut().unwrap().push[0].dst = "https://example.com/a".to_string();
    let err = expand_phases(&m, "abc", false).unwrap_err();
    assert!(matches!(err, ProfileError::InvalidManifest(_)));
}

#[test]
fn expand_phases_rejects_staging_push_with_relative_src() {
    let mut m = full_manifest();
    m.staging.as_mut().unwrap().push[0].src = "staging/a".to_string();
    let err = expand_phases(&m, "abc", false).unwrap_err();
    assert!(matches!(err, ProfileError::InvalidManifest(_)));
}

#[test]
fn expand_phases_rejects_staging_push_path_traversal() {
    let mut m = full_manifest();
    m.staging.as_mut().unwrap().push[0].src = "/workspace/../etc/a".to_string();
    let err = expand_phases(&m, "abc", false).unwrap_err();
    assert!(matches!(err, ProfileError::InvalidManifest(_)));
}

#[test]
fn phase5_staging_push_emits_rclone_copyto_and_secret_sentinels() {
    let m = full_manifest();
    let plan = expand_phases(&m, "abc", false).expect("ok");
    let script = find_script(&plan, "5_staging_push_0").expect("staging push step present");
    assert!(
        script.contains("rclone copyto"),
        "expected rclone copyto; got: {script}"
    );
    assert!(
        script.contains("/workspace/staging/a.safetensors"),
        "expected pod src in staging script; got: {script}"
    );
    assert!(
        script.contains("b2:bucket/staging/a.safetensors"),
        "expected b2 dst in staging script; got: {script}"
    );

    // __secret sentinels must be present in the step env.
    for entry in &plan.steps {
        if let StepEntry::Group(g) = entry {
            for s in &g.steps {
                if s.id == "5_staging_push_0" {
                    let env = s.args.get("env").expect("env object").clone();
                    let key_id = env
                        .get("VDSL_B2_KEY_ID")
                        .and_then(|v| v.as_str())
                        .unwrap_or("");
                    let key = env
                        .get("VDSL_B2_KEY")
                        .and_then(|v| v.as_str())
                        .unwrap_or("");
                    assert_eq!(key_id, "__secret:VDSL_B2_KEY_ID");
                    assert_eq!(key, "__secret:VDSL_B2_KEY");
                    return;
                }
            }
        }
    }
    panic!("5_staging_push_0 not found in plan");
}

#[test]
fn phase5_staging_push_substitutes_pod_id_placeholder() {
    let mut m = full_manifest();
    m.staging.as_mut().unwrap().push[0].dst =
        "b2://bucket/staging/{pod_id}/a.safetensors".to_string();
    let plan = expand_phases(&m, "abc", false).expect("ok");
    let script = find_script(&plan, "5_staging_push_0").expect("staging push step present");
    assert!(
        script.contains("b2:bucket/staging/abc/a.safetensors"),
        "expected {{pod_id}} → abc substitution; got: {script}"
    );
    assert!(
        !script.contains("{pod_id}"),
        "raw placeholder must be gone; got: {script}"
    );
}

#[test]
fn phase5_pull_emits_rclone_copyto() {
    let m = full_manifest();
    let plan = expand_phases(&m, "abc", false).expect("ok");
    let script = find_script(&plan, "5_sync_pull_0").expect("pull step present");
    assert!(
        script.contains("rclone"),
        "expected rclone invocation; got: {script}"
    );
    assert!(
        script.contains("b2:bucket/models"),
        "expected b2 remote 'b2:bucket/models' in pull script; got: {script}"
    );
    assert!(
        script.contains("/workspace/ComfyUI/models"),
        "expected pod dest in pull script; got: {script}"
    );
}

#[test]
fn phase5_push_emits_marker_append() {
    let m = full_manifest();
    let plan = expand_phases(&m, "abc", false).expect("ok");
    let script = find_script(&plan, "5_sync_push_0").expect("push step present");
    assert!(
        script.contains("/workspace/.vdsl/push_routes.jsonl"),
        "expected marker file append; got: {script}"
    );
    assert!(
        !script.contains("rclone"),
        "push at apply time must NOT run rclone; got: {script}"
    );
}

#[test]
fn phase5_push_substitutes_pod_id_placeholder() {
    let m = full_manifest();
    let plan = expand_phases(&m, "abc", false).expect("ok");
    let script = find_script(&plan, "5_sync_push_0").expect("push step present");
    assert!(
        script.contains("b2://bucket/output/abc/"),
        "expected {{pod_id}} → 'abc' substitution in push dst; got: {script}"
    );
    assert!(
        !script.contains("{pod_id}"),
        "placeholder must be substituted; got: {script}"
    );
}

#[test]
fn expand_phases_rejects_unsupported_model_scheme() {
    let mut m = full_manifest();
    m.models[0].src = "http://example.com/x.safetensors".to_string();

    let err = expand_phases(&m, "abc", false).unwrap_err();
    assert!(matches!(err, ProfileError::InvalidManifest(_)));
}

#[test]
fn expand_phases_forwards_dry_run_flag() {
    let m = parse_manifest(&minimal_manifest_json()).expect("parse ok");
    let plan = expand_phases(&m, "abc", true).expect("ok");
    assert!(plan.dry_run);
    assert_eq!(plan.mode, PlanMode::Seq);
}

/// Collect all `env` maps carried by exec-style step args.
fn collect_env_objects(plan: &BatchPlan) -> Vec<serde_json::Value> {
    let mut envs: Vec<serde_json::Value> = Vec::new();
    let collect_step = |s: &BatchStep, envs: &mut Vec<serde_json::Value>| {
        if let Some(env) = s.args.get("env") {
            envs.push(env.clone());
        }
    };
    for entry in &plan.steps {
        match entry {
            StepEntry::Leaf(s) => collect_step(s, &mut envs),
            StepEntry::Group(g) => {
                for s in &g.steps {
                    collect_step(s, &mut envs);
                }
            }
        }
    }
    envs
}

#[test]
fn expand_phases_emits_secret_placeholders() {
    let mut m = full_manifest();
    m.env.insert(
        "HF_TOKEN".to_string(),
        EnvValue::Secret(SecretRef {
            name: "HF_TOKEN".to_string(),
        }),
    );
    m.env.insert(
        "LOG_LEVEL".to_string(),
        EnvValue::Plain("debug".to_string()),
    );

    let plan = expand_phases(&m, "abc", false).expect("ok");

    // The plan serializes cleanly and contains the placeholder
    // string — never the secret env var's real value.
    let serialized = serde_json::to_string(&plan).expect("serialize ok");
    assert!(
        serialized.contains("__secret:HF_TOKEN"),
        "expected placeholder '__secret:HF_TOKEN' in serialized plan: {serialized}"
    );

    // Every exec-style step's env carries the placeholder for the
    // secret key and the plain value for the plain key.
    let envs = collect_env_objects(&plan);
    assert!(!envs.is_empty(), "expected at least one env object");
    for env in &envs {
        assert_eq!(
            env.get("HF_TOKEN").and_then(|v| v.as_str()),
            Some("__secret:HF_TOKEN"),
            "HF_TOKEN must be emitted as placeholder, got: {env}"
        );
        assert_eq!(
            env.get("LOG_LEVEL").and_then(|v| v.as_str()),
            Some("debug"),
            "plain env value must pass through verbatim, got: {env}"
        );
    }
}

#[test]
fn expand_phases_no_secrets_param_needed() {
    // Build a manifest referencing a secret whose env var is
    // deliberately NOT set in this process — expand_phases must
    // still succeed because secret resolution is deferred.
    let var = "VDSL_MCP_TEST_UNSET_AT_EXPAND_TIME_QQ";
    // Sanity: the var is unset. We do not touch the env either
    // way — proving that expand_phases never reads it.
    assert!(
        std::env::var(var).is_err(),
        "test precondition: '{var}' must be unset"
    );

    let mut m = full_manifest();
    m.env.insert(
        "TOKEN".to_string(),
        EnvValue::Secret(SecretRef {
            name: var.to_string(),
        }),
    );

    let plan = expand_phases(&m, "abc", false)
        .expect("expand_phases must not require the secret env var to be set");

    // Placeholder reaches the env object for every exec step.
    let envs = collect_env_objects(&plan);
    assert!(!envs.is_empty());
    for env in &envs {
        assert_eq!(
            env.get("TOKEN").and_then(|v| v.as_str()),
            Some(format!("__secret:{var}").as_str()),
        );
    }
}

// ----- shell safety -----

#[test]
fn shell_safe_accepts_normal_values() {
    assert!(is_shell_safe("git"));
    assert!(is_shell_safe("v0.3.10"));
    assert!(is_shell_safe("https://github.com/user/repo"));
    assert!(is_shell_safe("numpy==1.24"));
    assert!(is_shell_safe("checkpoints"));
    assert!(is_shell_safe("--lowvram"));
}

#[test]
fn shell_safe_rejects_spaces() {
    assert!(!is_shell_safe("--lowvram --preview-method auto"));
    assert!(!is_shell_safe("foo bar"));
}

#[test]
fn shell_safe_with_spaces_accepts_spaced_args() {
    assert!(is_shell_safe_with_spaces("--lowvram --preview-method auto"));
    assert!(is_shell_safe_with_spaces("--lowvram"));
}

#[test]
fn shell_safe_with_spaces_rejects_double_spaces() {
    assert!(!is_shell_safe_with_spaces("foo  bar"));
}

#[test]
fn shell_safe_rejects_injection() {
    assert!(!is_shell_safe("curl; rm -rf /"));
    assert!(!is_shell_safe("foo && whoami"));
    assert!(!is_shell_safe("$(evil)"));
    assert!(!is_shell_safe("`evil`"));
    assert!(!is_shell_safe("foo|bar"));
    assert!(!is_shell_safe("foo\nbar"));
    assert!(!is_shell_safe("foo'bar"));
    assert!(!is_shell_safe("foo\"bar"));
    assert!(!is_shell_safe(""));
}

#[test]
fn expand_phases_rejects_unsafe_apt_package() {
    let mut m = full_manifest();
    m.system.as_mut().unwrap().apt = vec!["curl; rm -rf /".to_string()];
    let err = expand_phases(&m, "abc", false).unwrap_err();
    assert!(matches!(err, ProfileError::InvalidManifest(_)));
}

#[test]
fn expand_phases_rejects_unsafe_custom_node_name() {
    let mut m = full_manifest();
    m.custom_nodes[0].name = "foo && whoami".to_string();
    let err = expand_phases(&m, "abc", false).unwrap_err();
    assert!(matches!(err, ProfileError::InvalidManifest(_)));
}

/// Helper: find a leaf step's `script` arg by step id.
fn find_script<'a>(plan: &'a BatchPlan, step_id: &str) -> Option<&'a str> {
    for entry in &plan.steps {
        match entry {
            StepEntry::Leaf(s) if s.id == step_id => {
                return s.args.get("script").and_then(|v| v.as_str());
            }
            StepEntry::Group(g) => {
                for s in &g.steps {
                    if s.id == step_id {
                        return s.args.get("script").and_then(|v| v.as_str());
                    }
                }
            }
            _ => {}
        }
    }
    None
}

#[test]
fn custom_node_pip_false_omits_pip_install() {
    let mut m = full_manifest();
    m.custom_nodes[0].pip = None;
    let plan = expand_phases(&m, "abc", false).expect("ok");
    let script = find_script(&plan, "4_custom_node_0").expect("step present");
    assert!(
        !script.contains("pip install"),
        "pip=None should NOT emit pip install; got: {script}"
    );
}

#[test]
fn custom_node_pip_true_installs_requirements_with_torch_filter() {
    let mut m = full_manifest();
    m.custom_nodes[0].pip = Some(true);
    let plan = expand_phases(&m, "abc", false).expect("ok");
    let script = find_script(&plan, "4_custom_node_0").expect("step present");

    // Must invoke pip install via the ComfyUI venv on a filtered stream.
    assert!(
        script.contains("/workspace/ComfyUI/.venv/bin/pip install -r /dev/stdin"),
        "expected venv pip install from stdin; got: {script}"
    );
    // Must reference the node's requirements.txt.
    assert!(
        script.contains("custom_nodes/ComfyUI-Manager/requirements.txt"),
        "expected requirements.txt reference; got: {script}"
    );
    // Must guard every torch-family package the pod's driver is pinned to.
    for pkg in [
        "torch",
        "torchvision",
        "torchaudio",
        "xformers",
        "bitsandbytes",
        "triton",
    ] {
        assert!(
            script.contains(pkg),
            "torch-filter regex missing {pkg}; got: {script}"
        );
    }
    // Must tolerate absent requirements.txt (test -f guard).
    assert!(
        script.contains("-f /workspace/ComfyUI/custom_nodes/ComfyUI-Manager/requirements.txt"),
        "expected test -f guard; got: {script}"
    );
}

#[test]
fn restart_script_kills_port_listener_and_self_excludes() {
    let m = full_manifest();
    let plan = expand_phases(&m, "abc", false).expect("ok");
    let script = find_script(&plan, "9_comfyui_restart").expect("step present");

    // Kill-set must be derived from the ACTUAL listener on $PORT
    // (via `ss -ltnpH`), not from an argv pattern like `.venv/bin/
    // python main.py` — pod images often start ComfyUI via system
    // python which the argv pattern misses, and apply would silently
    // leave the wrong ComfyUI bound to the port. See the 2026-04-21
    // runpod-slim incident in the project CLAUDE.md.
    assert!(
        script.contains("ss -ltnpH"),
        "expected ss -ltnpH for listener PID discovery; got: {script}"
    );
    assert!(
        script.contains("pid="),
        "expected pid= extraction from ss output; got: {script}"
    );
    assert!(
        !script.contains("lsof"),
        "lsof must not appear; got: {script}"
    );
    assert!(
        !script.contains("pgrep -f"),
        "argv-pattern pgrep was the bug; must not reappear. got: {script}"
    );

    // The kill loop must exclude $$ / $PPID so the wrapper shell
    // and its ssh-level parent are never targeted.
    assert!(
        script.contains("[ \"$pid\" = \"$$\" ] && continue"),
        "expected $$ self-exclude; got: {script}"
    );
    assert!(
        script.contains("[ \"$pid\" = \"$PPID\" ] && continue"),
        "expected $PPID self-exclude; got: {script}"
    );

    // An escalation to SIGKILL must happen before the 30s timeout,
    // to survive pod supervisors that respawn the listener.
    assert!(
        script.contains("kill -KILL"),
        "expected SIGKILL escalation; got: {script}"
    );

    // iproute2 auto-install must be present so bases lacking `ss`
    // don't silently no-op the kill loop. This was the 2026-04-21
    // runpod-slim bug root cause.
    assert!(
        script.contains("apt-get install -y -q iproute2"),
        "expected iproute2 auto-install fallback; got: {script}"
    );
    assert!(
        script.contains("command -v ss"),
        "expected gating on `command -v ss`; got: {script}"
    );
}

#[test]
fn comfyui_install_filters_torch_and_runs_cuda_smoke() {
    let m = full_manifest();
    let plan = expand_phases(&m, "abc", false).expect("ok");
    let script = find_script(&plan, "2_comfyui_install").expect("step present");

    // Phase 2 must apply the same torch-family filter as Phase 4 to the
    // ComfyUI requirements.txt before pip-installing — upstream pins
    // (e.g. torch>=2.7) otherwise shadow the system cu124 wheel inside
    // the venv and break CUDA at Phase 9 silently. 2026-04-25 incident.
    for pkg in [
        "torch",
        "torchvision",
        "torchaudio",
        "xformers",
        "bitsandbytes",
        "triton",
    ] {
        assert!(
            script.contains(pkg),
            "torch-family filter missing {pkg}; got: {script}"
        );
    }
    assert!(
        script.contains("grep -viE") && script.contains("requirements.txt"),
        "expected requirements.txt filtered through grep -viE; got: {script}"
    );
    assert!(
        script.contains(".venv/bin/pip install -r /dev/stdin"),
        "expected piped pip install -r /dev/stdin; got: {script}"
    );
    // CUDA smoke check must surface driver mismatch at Phase 2 (loud)
    // rather than at Phase 9 restart (silent).
    assert!(
        script.contains("torch.cuda.is_available()"),
        "expected torch.cuda.is_available() smoke check; got: {script}"
    );
}

#[test]
fn comfyui_install_script_env_override_replaces_body() {
    use std::io::Write;
    let dir = std::env::temp_dir().join(format!("vdsl-mcp-script-override-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("install.sh");
    let mut f = std::fs::File::create(&path).unwrap();
    writeln!(f, "#!/bin/bash\necho repo=${{REPO_URL}} ref=${{GIT_REF}}\n").unwrap();

    // Set env, expand phases, then unset to keep test isolation.
    // SAFETY: env mutation in tests is single-threaded inside Rust's
    // default test harness only when `--test-threads=1`. This test
    // therefore tolerates parallel execution by using a unique tempfile
    // and restoring the env at the end.
    let prev = std::env::var("VDSL_SCRIPT_COMFYUI_INSTALL").ok();
    // SAFETY: see above — single-threaded mutation.
    unsafe {
        std::env::set_var("VDSL_SCRIPT_COMFYUI_INSTALL", &path);
    }

    let m = full_manifest();
    let plan = expand_phases(&m, "abc", false).expect("ok");
    let script = find_script(&plan, "2_comfyui_install").expect("step present");

    // SAFETY: see above.
    unsafe {
        match prev {
            Some(v) => std::env::set_var("VDSL_SCRIPT_COMFYUI_INSTALL", v),
            None => std::env::remove_var("VDSL_SCRIPT_COMFYUI_INSTALL"),
        }
    }
    let _ = std::fs::remove_file(&path);
    let _ = std::fs::remove_dir(&dir);

    assert!(
        script.contains("repo=https://github.com/") && script.contains("echo repo="),
        "override body should be used verbatim with placeholders; got: {script}"
    );
    // Built-in pip-install line must NOT appear (override fully replaces).
    assert!(
        !script.contains(".venv/bin/pip install"),
        "override should replace built-in script; got: {script}"
    );
}

#[test]
fn expand_phases_rejects_unsafe_comfyui_args() {
    let mut m = full_manifest();
    m.comfyui.as_mut().expect("comfyui present").args = Some(vec!["; rm -rf /".to_string()]);
    let err = expand_phases(&m, "abc", false).unwrap_err();
    assert!(matches!(err, ProfileError::InvalidManifest(_)));
}

#[test]
fn expand_phases_without_comfyui_skips_phase_2_9_10() {
    // Evacuation / staging-only profile: no comfyui block means no
    // install, no restart, no health check. Only the phases whose
    // source data is declared elsewhere in the manifest get emitted.
    // Regression guard for the 2026-04-24 design flaw where an
    // evacuation profile forced a Phase 2 git fetch that could not
    // run on a disk-quota-exhausted volume, deadlocking the apply.
    let m = ProfileManifest {
        schema: PROFILE_SCHEMA.to_string(),
        name: "evac".to_string(),
        comfyui: None,
        system: None,
        python: None,
        custom_nodes: vec![],
        sync: None,
        staging: Some(crate::domain::profile::StagingConfig {
            push: vec![SyncRoute {
                src: "/workspace/data/".to_string(),
                dst: "b2://bucket/archive/data/".to_string(),
            }],
        }),
        models: vec![],
        env: HashMap::new(),
        hooks: None,
    };
    let plan = expand_phases(&m, "abc", false).expect("ok");
    let mut ids = Vec::new();
    for entry in &plan.steps {
        match entry {
            StepEntry::Leaf(s) => ids.push(s.id.clone()),
            StepEntry::Group(g) => {
                for s in &g.steps {
                    ids.push(s.id.clone());
                }
            }
        }
    }
    assert!(
        ids.iter().any(|i| i == "5_staging_push_0"),
        "staging step must fire; got: {ids:?}"
    );
    for forbidden in ["2_comfyui_install", "9_comfyui_restart", "10_health"] {
        assert!(
            !ids.iter().any(|i| i == forbidden),
            "{forbidden} must NOT fire when comfyui is None; got: {ids:?}"
        );
    }
}

// ----- compute_profile_hash -----

#[test]
fn compute_profile_hash_is_deterministic() {
    let a = compute_profile_hash("hello");
    let b = compute_profile_hash("hello");
    assert_eq!(a, b);
    assert_eq!(a.len(), 64); // SHA-256 hex
}

#[test]
fn compute_profile_hash_differs_for_different_input() {
    let a = compute_profile_hash("hello");
    let b = compute_profile_hash("hellO");
    assert_ne!(a, b);
}
