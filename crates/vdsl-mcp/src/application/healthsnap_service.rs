//! Health-check single shot generation.
//!
//! Produces 1 image using a minimal SDXL workflow (CheckpointLoader →
//! CLIPTextEncode × 2 → EmptyLatentImage → KSampler → VAEDecode → SaveImage).
//! No custom nodes, no VDSL Lua DSL, no catalog.
//!
//! Smoke verification primitive for ComfyUI endpoint readiness — answers
//! "is this pod actually producing images right now?" in a single tool call.
//! Designed to be Stop-on-Error friendly: every step maps to a distinct error
//! variant so callers can decide whether to wait (warmup) or bail (genuinely
//! broken) without retry / dependency install / pod state mutation.

use std::path::PathBuf;

use crate::infra::comfyui_client::ComfyUiClient;

const DEFAULT_PROMPT: &str = "a single flower in a vase, simple background";
const DEFAULT_NEGATIVE: &str = "blurry, low quality, watermark, text";
const DEFAULT_WIDTH: u32 = 1024;
const DEFAULT_HEIGHT: u32 = 1024;
const DEFAULT_STEPS: u32 = 20;
const DEFAULT_SEED: u64 = 1;
const DEFAULT_TIMEOUT_SECS: u64 = 90;
const DEFAULT_ATTEMPTS: u32 = 3;
const POLL_INTERVAL_SECS: u64 = 1;
const RETRY_BACKOFF_SECS: u64 = 2;
const HEALTHSNAP_FILENAME_PREFIX: &str = "ComfyUI_healthsnap";

/// Structured failure mode. Each variant maps to one Driver Loop step so
/// callers can route on the cause without parsing free text. Variants are
/// split into "retriable" (transient ComfyUI / pod state) and "fatal"
/// (caller error or pod state that won't change between attempts); the
/// retry loop in [`run_healthsnap`] uses [`HealthsnapError::is_retriable`].
#[derive(Debug, thiserror::Error)]
pub enum HealthsnapError {
    #[error("system_stats fail: {0}")]
    SystemStatsFail(String),
    #[error("object_info fail: {0}")]
    ObjectInfoFail(String),
    #[error("no checkpoint available on pod")]
    NoCheckpoint,
    #[error("checkpoint '{requested}' not on pod (available: [{available}])")]
    CheckpointNotFound {
        requested: String,
        available: String,
    },
    #[error("post /prompt fail: {0}")]
    WorkflowFail(String),
    #[error("history poll fail: {0}")]
    HistoryPollFail(String),
    #[error("generate timeout after {0}s")]
    GenerateTimeout(u64),
    #[error("generate execution error: {0}")]
    GenerateError(String),
    #[error("no output image found in history")]
    NoOutputImage,
    #[error("download fail: {0}")]
    DownloadFail(String),
    #[error("save_dir create fail: {0}")]
    SaveDirFail(String),
    #[error("all {attempts} attempts failed; last error: {last}")]
    AllAttemptsFailed {
        attempts: u32,
        last: Box<HealthsnapError>,
    },
}

impl HealthsnapError {
    /// Whether this error should be retried on a subsequent attempt.
    ///
    /// Retriable: transient ComfyUI / pod state (timeout, HTTP fail, mid-run
    /// exceptions like BrokenPipeError in tqdm).
    ///
    /// Fatal: caller error or pod state that won't change between attempts —
    /// checkpoint not found, no checkpoints on pod, save_dir creation fail,
    /// or already an `AllAttemptsFailed` (don't double-wrap).
    pub fn is_retriable(&self) -> bool {
        matches!(
            self,
            HealthsnapError::SystemStatsFail(_)
                | HealthsnapError::ObjectInfoFail(_)
                | HealthsnapError::WorkflowFail(_)
                | HealthsnapError::HistoryPollFail(_)
                | HealthsnapError::GenerateTimeout(_)
                | HealthsnapError::GenerateError(_)
                | HealthsnapError::NoOutputImage
                | HealthsnapError::DownloadFail(_)
        )
    }
}

/// Parameters for one health-check shot, with all defaults resolved.
#[derive(Debug, Clone)]
pub struct HealthsnapParams {
    pub prompt: String,
    pub negative: String,
    pub checkpoint: Option<String>,
    pub save_dir: PathBuf,
    pub seed: u64,
    pub width: u32,
    pub height: u32,
    pub steps: u32,
    /// Per-attempt timeout (queue accept → generate → download).
    pub timeout_secs: u64,
    /// Total number of attempts. `1` disables retry. Default `3`.
    pub attempts: u32,
}

impl HealthsnapParams {
    /// Build params from MCP request, applying defaults for any None.
    #[allow(clippy::too_many_arguments)]
    pub fn build(
        prompt: Option<String>,
        negative: Option<String>,
        checkpoint: Option<String>,
        save_dir: Option<String>,
        seed: Option<u64>,
        width: Option<u32>,
        height: Option<u32>,
        steps: Option<u32>,
        timeout_secs: Option<u64>,
        attempts: Option<u32>,
    ) -> Self {
        Self {
            prompt: prompt.unwrap_or_else(|| DEFAULT_PROMPT.to_string()),
            negative: negative.unwrap_or_else(|| DEFAULT_NEGATIVE.to_string()),
            checkpoint,
            save_dir: save_dir
                .map(PathBuf::from)
                .unwrap_or_else(|| std::env::temp_dir().join("vdsl_healthsnap")),
            seed: seed.unwrap_or(DEFAULT_SEED),
            width: width.unwrap_or(DEFAULT_WIDTH),
            height: height.unwrap_or(DEFAULT_HEIGHT),
            steps: steps.unwrap_or(DEFAULT_STEPS),
            timeout_secs: timeout_secs.unwrap_or(DEFAULT_TIMEOUT_SECS),
            attempts: attempts.unwrap_or(DEFAULT_ATTEMPTS).max(1),
        }
    }
}

/// Successful healthsnap outcome.
#[derive(Debug)]
pub struct HealthsnapResult {
    pub checkpoint: String,
    pub prompt: String,
    pub image_path: PathBuf,
    pub duration_ms: u64,
    pub comfyui_version: String,
}

/// Build the minimal SDXL workflow JSON. 7 native ComfyUI nodes, no custom
/// nodes. SDXL / Illustrious / pony / waiIllustrious checkpoints all run on
/// this shape.
pub fn build_workflow_json(
    checkpoint: &str,
    prompt: &str,
    negative: &str,
    width: u32,
    height: u32,
    steps: u32,
    seed: u64,
) -> serde_json::Value {
    serde_json::json!({
        "1": {
            "class_type": "CheckpointLoaderSimple",
            "inputs": { "ckpt_name": checkpoint }
        },
        "2": {
            "class_type": "CLIPTextEncode",
            "inputs": { "text": prompt, "clip": ["1", 1] }
        },
        "3": {
            "class_type": "CLIPTextEncode",
            "inputs": { "text": negative, "clip": ["1", 1] }
        },
        "4": {
            "class_type": "EmptyLatentImage",
            "inputs": { "width": width, "height": height, "batch_size": 1 }
        },
        "5": {
            "class_type": "KSampler",
            "inputs": {
                "seed": seed,
                "steps": steps,
                "cfg": 7.0,
                "sampler_name": "euler",
                "scheduler": "normal",
                "denoise": 1.0,
                "model": ["1", 0],
                "positive": ["2", 0],
                "negative": ["3", 0],
                "latent_image": ["4", 0]
            }
        },
        "6": {
            "class_type": "VAEDecode",
            "inputs": { "samples": ["5", 0], "vae": ["1", 2] }
        },
        "7": {
            "class_type": "SaveImage",
            "inputs": {
                "filename_prefix": HEALTHSNAP_FILENAME_PREFIX,
                "images": ["6", 0]
            }
        }
    })
}

/// Extract the checkpoint name list from `/object_info` response.
///
/// ComfyUI shape: `object_info["CheckpointLoaderSimple"]["input"]["required"]["ckpt_name"]`
/// is a 2-tuple `[[<name>, ...], {<meta>}]`. Tolerates `optional` placement
/// too in case of node variants.
pub fn extract_checkpoints(info: &serde_json::Value) -> Vec<String> {
    info.get("CheckpointLoaderSimple")
        .and_then(|n| n.get("input"))
        .and_then(|i| {
            i.get("required")
                .or_else(|| i.get("optional"))
                .and_then(|r| r.get("ckpt_name"))
        })
        .and_then(|c| c.get(0))
        .and_then(|enum_val| enum_val.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|v| v.as_str().map(|s| s.to_string()))
                .collect()
        })
        .unwrap_or_default()
}

/// Pick a checkpoint: explicit request wins if present on pod, else first
/// available, else `NoCheckpoint`.
pub fn pick_checkpoint(
    requested: Option<&str>,
    available: &[String],
) -> Result<String, HealthsnapError> {
    match requested {
        Some(name) if available.iter().any(|c| c == name) => Ok(name.to_string()),
        Some(name) => Err(HealthsnapError::CheckpointNotFound {
            requested: name.to_string(),
            available: available.join(", "),
        }),
        None => available
            .first()
            .cloned()
            .ok_or(HealthsnapError::NoCheckpoint),
    }
}

/// Pull `(filename, subfolder)` of the first output image from a history entry.
pub fn first_output_image(entry: &serde_json::Value) -> Option<(String, String)> {
    entry
        .get("outputs")
        .and_then(|o| o.as_object())
        .and_then(|outputs| {
            outputs.values().find_map(|node_out| {
                node_out
                    .get("images")
                    .and_then(|imgs| imgs.as_array())
                    .and_then(|arr| arr.first())
                    .map(|img| {
                        (
                            img.get("filename")
                                .and_then(|v| v.as_str())
                                .unwrap_or("")
                                .to_string(),
                            img.get("subfolder")
                                .and_then(|v| v.as_str())
                                .unwrap_or("")
                                .to_string(),
                        )
                    })
            })
        })
        .filter(|(name, _)| !name.is_empty())
}

/// Inspect a history `status` block for an `execution_error` message.
///
/// The message tuple is `[event_name, data]` per ComfyUI `add_message` calls
/// in `execution.py`. We extract the data half so callers see `node_id`,
/// `exception_type`, `exception_message`, and `traceback`.
pub fn extract_execution_error(status: &serde_json::Value) -> Option<String> {
    status
        .get("messages")
        .and_then(|m| m.as_array())
        .and_then(|arr| {
            arr.iter().find_map(|m| {
                let name = m.get(0).and_then(|n| n.as_str())?;
                if name == "execution_error" {
                    Some(format!("{m}"))
                } else {
                    None
                }
            })
        })
}

/// Decision per ComfyUI history entry's `status` block. Returns
/// `Some(Ok(()))` for terminal success, `Some(Err(_))` for terminal failure,
/// `None` when still in flight (caller keeps polling).
///
/// Canonical authority: ComfyUI `execution.py` `ExecutionStatus` —
/// `status_str: Literal['success', 'error']`. The `completed` boolean is
/// false on error, so callers that key on `completed` alone silently miss
/// terminal failures (root cause of the previous false-GenerateTimeout
/// behavior). Unknown `status_str` values are treated as not-yet-terminal so
/// a future ComfyUI extension doesn't crash older clients.
pub fn status_terminal(status: &serde_json::Value) -> Option<Result<(), HealthsnapError>> {
    let status_str = status.get("status_str").and_then(|s| s.as_str())?;
    match status_str {
        "success" => Some(Ok(())),
        "error" => {
            let detail = extract_execution_error(status).unwrap_or_else(|| {
                format!("status_str=error (no execution_error message); status: {status}")
            });
            Some(Err(HealthsnapError::GenerateError(detail)))
        }
        _ => None,
    }
}

/// Run one healthsnap end-to-end with retry semantics.
///
/// Pre-checks (system_stats, object_info, checkpoint pick) run **once** and
/// their failures short-circuit without retry (they're caller error or pod
/// configuration — they won't change between attempts). Queue → poll →
/// download runs up to `params.attempts` times with `RETRY_BACKOFF_SECS`
/// between attempts. Retry is governed by [`HealthsnapError::is_retriable`].
/// A non-retriable failure surfaces immediately; an exhausted retriable
/// failure is wrapped in [`HealthsnapError::AllAttemptsFailed`] so callers
/// can distinguish "0 ≤ N attempts succeeded" from "first attempt fatal".
pub async fn run_healthsnap(
    client: &ComfyUiClient,
    params: &HealthsnapParams,
) -> Result<HealthsnapResult, HealthsnapError> {
    let started = std::time::Instant::now();

    // Pre-checks (no retry).
    let stats = client
        .system_stats()
        .await
        .map_err(|e| HealthsnapError::SystemStatsFail(e.to_string()))?;
    let comfyui_version = stats
        .get("system")
        .and_then(|s| s.get("comfyui_version"))
        .and_then(|v| v.as_str())
        .unwrap_or("unknown")
        .to_string();

    let info = client
        .object_info()
        .await
        .map_err(|e| HealthsnapError::ObjectInfoFail(e.to_string()))?;
    let available = extract_checkpoints(&info);
    let checkpoint = pick_checkpoint(params.checkpoint.as_deref(), &available)?;

    let workflow = build_workflow_json(
        &checkpoint,
        &params.prompt,
        &params.negative,
        params.width,
        params.height,
        params.steps,
        params.seed,
    );

    // Retry loop over queue → poll → download.
    for attempt in 1..=params.attempts {
        match run_attempt(client, &workflow, params).await {
            Ok((image_path, _)) => {
                return Ok(HealthsnapResult {
                    checkpoint,
                    prompt: params.prompt.clone(),
                    image_path,
                    duration_ms: started.elapsed().as_millis() as u64,
                    comfyui_version,
                });
            }
            Err(e) => {
                if !e.is_retriable() {
                    return Err(e);
                }
                if attempt == params.attempts {
                    return Err(HealthsnapError::AllAttemptsFailed {
                        attempts: params.attempts,
                        last: Box::new(e),
                    });
                }
                tokio::time::sleep(std::time::Duration::from_secs(RETRY_BACKOFF_SECS)).await;
            }
        }
    }

    // Unreachable: `attempts >= 1` per HealthsnapParams::build.
    Err(HealthsnapError::WorkflowFail(
        "internal: retry loop exited without verdict".into(),
    ))
}

/// One attempt: POST /prompt → poll history → download image.
///
/// Returns `(image_path, prompt_id)` on success. Each call gets its own
/// prompt_id (ComfyUI doesn't share state between re-submissions); after
/// the first attempt the checkpoint should be warm-cached and the second
/// attempt typically completes in <30 s on A40-class hardware.
async fn run_attempt(
    client: &ComfyUiClient,
    workflow: &serde_json::Value,
    params: &HealthsnapParams,
) -> Result<(PathBuf, String), HealthsnapError> {
    // Queue.
    let resp = client
        .post_prompt(workflow)
        .await
        .map_err(|e| HealthsnapError::WorkflowFail(e.to_string()))?;
    let prompt_id = resp["prompt_id"]
        .as_str()
        .ok_or_else(|| HealthsnapError::WorkflowFail(format!("no prompt_id in response: {resp}")))?
        .to_string();

    // Poll history.
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(params.timeout_secs);
    let interval = std::time::Duration::from_secs(POLL_INTERVAL_SECS);
    let entry = loop {
        let history = client
            .history(&prompt_id)
            .await
            .map_err(|e| HealthsnapError::HistoryPollFail(e.to_string()))?;
        if let Some(entry) = history.get(&prompt_id) {
            if let Some(status) = entry.get("status") {
                if let Some(verdict) = status_terminal(status) {
                    verdict?;
                    break entry.clone();
                }
            }
        }
        if std::time::Instant::now() >= deadline {
            return Err(HealthsnapError::GenerateTimeout(params.timeout_secs));
        }
        tokio::time::sleep(interval).await;
    };

    // Output image.
    let (filename, subfolder) = first_output_image(&entry).ok_or(HealthsnapError::NoOutputImage)?;

    // Save dir + download.
    tokio::fs::create_dir_all(&params.save_dir)
        .await
        .map_err(|e| HealthsnapError::SaveDirFail(e.to_string()))?;
    let ts = chrono::Utc::now().format("%Y%m%dT%H%M%SZ").to_string();
    let safe_name = filename.replace(['/', '\\'], "_");
    let dest = params.save_dir.join(format!("healthsnap_{ts}_{safe_name}"));
    client
        .download_image(&filename, &subfolder, &dest)
        .await
        .map_err(|e| HealthsnapError::DownloadFail(e.to_string()))?;

    Ok((dest, prompt_id))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn build_workflow_json_has_7_nodes_with_expected_classes() {
        let wf = build_workflow_json("ckpt.safetensors", "p", "n", 1024, 1024, 20, 42);
        let obj = wf.as_object().expect("workflow is JSON object");
        assert_eq!(obj.len(), 7);
        assert_eq!(
            wf["1"]["class_type"].as_str().unwrap(),
            "CheckpointLoaderSimple"
        );
        assert_eq!(
            wf["1"]["inputs"]["ckpt_name"].as_str().unwrap(),
            "ckpt.safetensors"
        );
        assert_eq!(wf["5"]["inputs"]["seed"].as_u64().unwrap(), 42);
        assert_eq!(wf["5"]["inputs"]["steps"].as_u64().unwrap(), 20);
        assert_eq!(wf["7"]["class_type"].as_str().unwrap(), "SaveImage");
        assert_eq!(
            wf["7"]["inputs"]["filename_prefix"].as_str().unwrap(),
            HEALTHSNAP_FILENAME_PREFIX
        );
    }

    #[test]
    fn build_workflow_json_uses_only_native_nodes() {
        let wf = build_workflow_json("ckpt", "p", "n", 1024, 1024, 20, 1);
        let obj = wf.as_object().unwrap();
        let allowed: std::collections::HashSet<&str> = [
            "CheckpointLoaderSimple",
            "CLIPTextEncode",
            "EmptyLatentImage",
            "KSampler",
            "VAEDecode",
            "SaveImage",
        ]
        .iter()
        .copied()
        .collect();
        for (id, node) in obj {
            let ct = node["class_type"].as_str().unwrap();
            assert!(
                allowed.contains(ct),
                "non-standard node introduced at id={id}: {ct}"
            );
        }
    }

    #[test]
    fn extract_checkpoints_parses_object_info_shape() {
        let info = serde_json::json!({
            "CheckpointLoaderSimple": {
                "input": {
                    "required": {
                        "ckpt_name": [["one.safetensors", "two.safetensors"], {}]
                    }
                }
            }
        });
        let list = extract_checkpoints(&info);
        assert_eq!(list, vec!["one.safetensors", "two.safetensors"]);
    }

    #[test]
    fn extract_checkpoints_tolerates_optional_placement() {
        let info = serde_json::json!({
            "CheckpointLoaderSimple": {
                "input": {
                    "optional": {
                        "ckpt_name": [["only.safetensors"], {}]
                    }
                }
            }
        });
        let list = extract_checkpoints(&info);
        assert_eq!(list, vec!["only.safetensors"]);
    }

    #[test]
    fn extract_checkpoints_empty_when_node_missing() {
        let info = serde_json::json!({});
        let list = extract_checkpoints(&info);
        assert!(list.is_empty());
    }

    #[test]
    fn pick_checkpoint_picks_first_when_unspecified() {
        let avail = vec!["a".to_string(), "b".to_string()];
        let picked = pick_checkpoint(None, &avail).unwrap();
        assert_eq!(picked, "a");
    }

    #[test]
    fn pick_checkpoint_respects_explicit_request_when_available() {
        let avail = vec!["a".to_string(), "b".to_string()];
        let picked = pick_checkpoint(Some("b"), &avail).unwrap();
        assert_eq!(picked, "b");
    }

    #[test]
    fn pick_checkpoint_rejects_unknown_explicit_request() {
        let avail = vec!["a".to_string()];
        let err = pick_checkpoint(Some("missing"), &avail).unwrap_err();
        match err {
            HealthsnapError::CheckpointNotFound { requested, .. } => {
                assert_eq!(requested, "missing")
            }
            other => panic!("expected CheckpointNotFound, got {other:?}"),
        }
    }

    #[test]
    fn pick_checkpoint_errors_when_none_available() {
        let avail: Vec<String> = vec![];
        let err = pick_checkpoint(None, &avail).unwrap_err();
        assert!(matches!(err, HealthsnapError::NoCheckpoint));
    }

    #[test]
    fn first_output_image_extracts_filename_and_subfolder() {
        let entry = serde_json::json!({
            "outputs": {
                "7": {
                    "images": [
                        { "filename": "ComfyUI_healthsnap_00001_.png", "subfolder": "" }
                    ]
                }
            }
        });
        let (name, sub) = first_output_image(&entry).unwrap();
        assert_eq!(name, "ComfyUI_healthsnap_00001_.png");
        assert_eq!(sub, "");
    }

    #[test]
    fn first_output_image_none_when_outputs_absent() {
        let entry = serde_json::json!({ "status": { "completed": true } });
        assert!(first_output_image(&entry).is_none());
    }

    #[test]
    fn first_output_image_none_when_filename_empty() {
        let entry = serde_json::json!({
            "outputs": { "7": { "images": [{ "filename": "", "subfolder": "" }] } }
        });
        assert!(first_output_image(&entry).is_none());
    }

    #[test]
    fn extract_execution_error_picks_up_execution_error_message() {
        let status = serde_json::json!({
            "completed": true,
            "messages": [
                ["execution_start", {}],
                ["execution_error", { "node_id": "5", "exception_message": "boom" }]
            ]
        });
        let err = extract_execution_error(&status).unwrap();
        assert!(err.contains("execution_error"));
        assert!(err.contains("boom"));
    }

    #[test]
    fn extract_execution_error_none_on_clean_status() {
        let status = serde_json::json!({
            "completed": true,
            "messages": [["execution_start", {}], ["execution_success", {}]]
        });
        assert!(extract_execution_error(&status).is_none());
    }

    #[test]
    fn params_build_applies_all_defaults_when_none() {
        let p = HealthsnapParams::build(None, None, None, None, None, None, None, None, None, None);
        assert_eq!(p.prompt, DEFAULT_PROMPT);
        assert_eq!(p.negative, DEFAULT_NEGATIVE);
        assert!(p.checkpoint.is_none());
        assert_eq!(p.seed, DEFAULT_SEED);
        assert_eq!(p.width, DEFAULT_WIDTH);
        assert_eq!(p.height, DEFAULT_HEIGHT);
        assert_eq!(p.steps, DEFAULT_STEPS);
        assert_eq!(p.timeout_secs, DEFAULT_TIMEOUT_SECS);
        assert_eq!(p.attempts, DEFAULT_ATTEMPTS);
        assert!(p.save_dir.ends_with("vdsl_healthsnap"));
    }

    #[test]
    fn params_build_honors_explicit_overrides() {
        let p = HealthsnapParams::build(
            Some("custom prompt".into()),
            Some("custom negative".into()),
            Some("ckpt.safetensors".into()),
            Some("/tmp/custom".into()),
            Some(999),
            Some(512),
            Some(768),
            Some(10),
            Some(60),
            Some(5),
        );
        assert_eq!(p.prompt, "custom prompt");
        assert_eq!(p.negative, "custom negative");
        assert_eq!(p.checkpoint.as_deref(), Some("ckpt.safetensors"));
        assert_eq!(p.save_dir, std::path::PathBuf::from("/tmp/custom"));
        assert_eq!(p.seed, 999);
        assert_eq!(p.width, 512);
        assert_eq!(p.height, 768);
        assert_eq!(p.steps, 10);
        assert_eq!(p.timeout_secs, 60);
        assert_eq!(p.attempts, 5);
    }

    #[test]
    fn params_build_clamps_zero_attempts_to_one() {
        // attempts == 0 is meaningless (the retry loop would never run) and
        // would unreachable-panic; build() floors at 1.
        let p = HealthsnapParams::build(
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            Some(0),
        );
        assert_eq!(p.attempts, 1);
    }

    #[test]
    fn status_terminal_success_returns_ok() {
        let status = serde_json::json!({
            "status_str": "success",
            "completed": true,
            "messages": [["execution_start", {}], ["execution_success", {}]]
        });
        let verdict = status_terminal(&status).expect("terminal");
        assert!(verdict.is_ok());
    }

    #[test]
    fn status_terminal_error_returns_generate_error_with_detail() {
        // ComfyUI sets completed=false on error; we must still classify as terminal.
        let status = serde_json::json!({
            "status_str": "error",
            "completed": false,
            "messages": [
                ["execution_start", {}],
                ["execution_error", {
                    "prompt_id": "abc",
                    "node_id": "5",
                    "node_type": "KSampler",
                    "exception_type": "BrokenPipeError",
                    "exception_message": "[Errno 32] Broken pipe"
                }]
            ]
        });
        let verdict = status_terminal(&status).expect("terminal");
        match verdict {
            Err(HealthsnapError::GenerateError(msg)) => {
                assert!(msg.contains("execution_error"));
                assert!(msg.contains("BrokenPipeError"));
                assert!(msg.contains("KSampler"));
            }
            other => panic!("expected GenerateError, got {other:?}"),
        }
    }

    #[test]
    fn status_terminal_error_without_message_falls_back_to_status_snapshot() {
        let status = serde_json::json!({
            "status_str": "error",
            "completed": false,
            "messages": []
        });
        let verdict = status_terminal(&status).expect("terminal");
        match verdict {
            Err(HealthsnapError::GenerateError(msg)) => {
                assert!(msg.contains("status_str=error"));
            }
            other => panic!("expected GenerateError, got {other:?}"),
        }
    }

    #[test]
    fn status_terminal_in_flight_returns_none() {
        let status = serde_json::json!({ "completed": false });
        assert!(status_terminal(&status).is_none());
    }

    #[test]
    fn status_terminal_unknown_status_str_returns_none() {
        // Forward-compat: an unfamiliar status_str (e.g. ComfyUI adds a
        // 'cancelled' variant in the future) should not crash older clients;
        // we keep polling and let the deadline catch it.
        let status = serde_json::json!({ "status_str": "cancelled", "completed": false });
        assert!(status_terminal(&status).is_none());
    }

    #[test]
    fn is_retriable_classifies_each_variant_correctly() {
        // Retriable: transient pod / ComfyUI state.
        assert!(HealthsnapError::SystemStatsFail("x".into()).is_retriable());
        assert!(HealthsnapError::ObjectInfoFail("x".into()).is_retriable());
        assert!(HealthsnapError::WorkflowFail("x".into()).is_retriable());
        assert!(HealthsnapError::HistoryPollFail("x".into()).is_retriable());
        assert!(HealthsnapError::GenerateTimeout(90).is_retriable());
        assert!(HealthsnapError::GenerateError("x".into()).is_retriable());
        assert!(HealthsnapError::NoOutputImage.is_retriable());
        assert!(HealthsnapError::DownloadFail("x".into()).is_retriable());

        // Fatal: caller error / pod config — retrying won't help.
        assert!(!HealthsnapError::NoCheckpoint.is_retriable());
        assert!(!HealthsnapError::CheckpointNotFound {
            requested: "x".into(),
            available: "".into()
        }
        .is_retriable());
        assert!(!HealthsnapError::SaveDirFail("x".into()).is_retriable());
        assert!(!HealthsnapError::AllAttemptsFailed {
            attempts: 3,
            last: Box::new(HealthsnapError::NoOutputImage)
        }
        .is_retriable());
    }
}
