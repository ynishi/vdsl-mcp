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
        comfyui: ComfyUiConfig {
            ref_: "master".to_string(),
            repo: None,
            args: Some(vec!["--lowvram".to_string()]),
            port: Some(8188),
        },
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
                src: "cloud".to_string(),
                dst: "pod-abc".to_string(),
            }],
            push: vec![SyncRoute {
                src: "pod-abc".to_string(),
                dst: "cloud".to_string(),
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

fn edges_for_pod(pod: &str) -> Vec<(LocationId, LocationId)> {
    let local = LocationId::local();
    let cloud = LocationId::new("cloud").unwrap();
    let pod = LocationId::new(format!("pod-{pod}")).unwrap();
    vec![
        (local.clone(), cloud.clone()),
        (cloud.clone(), local.clone()),
        (cloud.clone(), pod.clone()),
        (pod.clone(), cloud.clone()),
        (local, pod),
    ]
}

// ----- parse_manifest -----

#[test]
fn parse_manifest_accepts_valid_json() {
    let m = parse_manifest(&minimal_manifest_json()).expect("parse ok");
    assert_eq!(m.schema, PROFILE_SCHEMA);
    assert_eq!(m.name, "minimal");
    assert_eq!(m.comfyui.ref_, "v0.3.10");
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
fn parse_manifest_deserializes_env_value_variants() {
    let json = serde_json::json!({
        "schema": "vdsl.profile/1",
        "name": "env-test",
        "comfyui": { "ref": "x" },
        "env": {
            "PLAIN_KEY": "plain_value",
            "SECRET_KEY": { "__secret": "MY_SECRET" }
        }
    })
    .to_string();
    let m = parse_manifest(&json).expect("parse ok");
    match m.env.get("PLAIN_KEY") {
        Some(EnvValue::Plain(s)) => assert_eq!(s, "plain_value"),
        other => panic!("expected Plain, got {other:?}"),
    }
    match m.env.get("SECRET_KEY") {
        Some(EnvValue::Secret(SecretRef { name })) => assert_eq!(name, "MY_SECRET"),
        other => panic!("expected Secret, got {other:?}"),
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
        comfyui: ComfyUiConfig {
            ref_: "x".to_string(),
            repo: None,
            args: None,
            port: None,
        },
        system: None,
        python: None,
        custom_nodes: vec![],
        sync: None,
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
        comfyui: ComfyUiConfig {
            ref_: "x".to_string(),
            repo: None,
            args: None,
            port: None,
        },
        system: None,
        python: None,
        custom_nodes: vec![],
        sync: None,
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
    let plan = expand_phases(&m, "abc", &edges_for_pod("abc"), false).expect("ok");
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
    let plan = expand_phases(&m, "abc", &edges_for_pod("abc"), false).expect("ok");

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

    assert_eq!(find("1_system_apt"), Some("exec".to_string()));
    assert_eq!(find("2_comfyui_install"), Some("exec".to_string()));
    assert_eq!(find("3_python_deps"), Some("exec".to_string()));
    assert_eq!(find("4_custom_node_0"), Some("exec".to_string()));
    assert_eq!(find("5_sync_pull_0"), Some("sync_route".to_string()));
    assert_eq!(
        find("5_sync_push_0"),
        Some("sync_route_register".to_string())
    );
    assert_eq!(find("6_sync_poll_0"), Some("sync_poll".to_string()));
    assert_eq!(find("7_model_0"), Some("exec".to_string())); // b2:// → rclone exec
    assert_eq!(find("7_model_1"), Some("exec".to_string())); // file://
    assert_eq!(find("8_post_install"), Some("exec".to_string()));
    assert_eq!(find("9_comfyui_restart"), Some("exec".to_string()));
    assert_eq!(find("10_health"), Some("comfy_api".to_string()));
}

#[test]
fn expand_phases_rejects_unknown_edge() {
    let mut m = full_manifest();
    // Replace valid pull with one referencing a pod that is not in the topology.
    m.sync.as_mut().unwrap().pull[0].dst ="pod-other".to_string();

    let err = expand_phases(&m, "abc", &edges_for_pod("abc"), false).unwrap_err();
    assert!(matches!(err, ProfileError::InvalidManifest(_)));
}

#[test]
fn expand_phases_rejects_invalid_location_string() {
    let mut m = full_manifest();
    // Uppercase rejected by LocationId::new.
    m.sync.as_mut().unwrap().pull[0].src = "CLOUD".to_string();

    let err = expand_phases(&m, "abc", &edges_for_pod("abc"), false).unwrap_err();
    assert!(matches!(err, ProfileError::InvalidManifest(_)));
}

#[test]
fn expand_phases_rejects_unsupported_model_scheme() {
    let mut m = full_manifest();
    m.models[0].src = "http://example.com/x.safetensors".to_string();

    let err = expand_phases(&m, "abc", &edges_for_pod("abc"), false).unwrap_err();
    assert!(matches!(err, ProfileError::InvalidManifest(_)));
}

#[test]
fn expand_phases_forwards_dry_run_flag() {
    let m = parse_manifest(&minimal_manifest_json()).expect("parse ok");
    let plan = expand_phases(&m, "abc", &edges_for_pod("abc"), true).expect("ok");
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

    let plan = expand_phases(&m, "abc", &edges_for_pod("abc"), false).expect("ok");

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

    let plan = expand_phases(&m, "abc", &edges_for_pod("abc"), false)
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
    let err = expand_phases(&m, "abc", &edges_for_pod("abc"), false).unwrap_err();
    assert!(matches!(err, ProfileError::InvalidManifest(_)));
}

#[test]
fn expand_phases_rejects_unsafe_custom_node_name() {
    let mut m = full_manifest();
    m.custom_nodes[0].name = "foo && whoami".to_string();
    let err = expand_phases(&m, "abc", &edges_for_pod("abc"), false).unwrap_err();
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
    let plan = expand_phases(&m, "abc", &edges_for_pod("abc"), false).expect("ok");
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
    let plan = expand_phases(&m, "abc", &edges_for_pod("abc"), false).expect("ok");
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
    for pkg in ["torch", "torchvision", "torchaudio", "xformers", "bitsandbytes", "triton"] {
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
fn expand_phases_rejects_unsafe_comfyui_args() {
    let mut m = full_manifest();
    m.comfyui.args = Some(vec!["; rm -rf /".to_string()]);
    let err = expand_phases(&m, "abc", &edges_for_pod("abc"), false).unwrap_err();
    assert!(matches!(err, ProfileError::InvalidManifest(_)));
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
