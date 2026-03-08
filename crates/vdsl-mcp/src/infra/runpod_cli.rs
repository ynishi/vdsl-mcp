//! RunPod CLI wrapper.
//!
//! Executes `runpod-cli -o json <args>` with the API key set via
//! environment variable. Parses JSON output from stdout.
//!
//! Matches the Lua reference implementation in `lua/vdsl/runtime/runpod.lua`.

use crate::domain::error::DomainError;

/// Wrapper around the `runpod-cli` binary.
#[derive(Clone)]
pub struct RunPodCli {
    api_key: String,
}

impl std::fmt::Debug for RunPodCli {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RunPodCli")
            .field("api_key", &"[REDACTED]")
            .finish()
    }
}

impl RunPodCli {
    pub fn new(api_key: String) -> Self {
        Self { api_key }
    }

    /// Execute runpod-cli with arbitrary args and return parsed JSON.
    ///
    /// Public entry point for passthrough usage. Automatically injects
    /// `RUNPOD_API_KEY` and `-o json` flags.
    pub async fn raw_exec(&self, args: &[String]) -> Result<serde_json::Value, DomainError> {
        let refs: Vec<&str> = args.iter().map(String::as_str).collect();
        self.exec(&refs).await
    }

    /// Execute runpod-cli and return parsed JSON.
    ///
    /// Equivalent to the Lua `cli(args, api_key)` helper in runpod.lua L56-82.
    async fn exec(&self, args: &[&str]) -> Result<serde_json::Value, DomainError> {
        let output = tokio::process::Command::new("runpod-cli")
            .env("RUNPOD_API_KEY", &self.api_key)
            .arg("-o")
            .arg("json")
            .args(args)
            .output()
            .await
            .map_err(|e| DomainError::CliExecution(format!("failed to execute runpod-cli: {e}")))?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            let stdout = String::from_utf8_lossy(&output.stdout);
            return Err(DomainError::CliError {
                code: output.status.code().unwrap_or(-1),
                message: format!("{stderr}{stdout}"),
            });
        }

        let stdout = String::from_utf8_lossy(&output.stdout);
        parse_cli_output(&stdout)
    }

    /// List all pods.
    ///
    /// Equivalent to Lua `M.pods(opts)` in runpod.lua L587-594.
    pub async fn list_pods(&self) -> Result<Vec<serde_json::Value>, DomainError> {
        let result = self
            .exec(&["pods", "list-pods", "--includeMachine"])
            .await?;
        match result {
            serde_json::Value::Array(pods) => Ok(pods),
            // Single pod returned as object — wrap in array
            other => Ok(vec![other]),
        }
    }

    /// Start (or resume) a pod.
    ///
    /// Equivalent to Lua `Pod:start()` in runpod.lua L114-116.
    pub async fn start_pod(&self, pod_id: &str) -> Result<serde_json::Value, DomainError> {
        self.exec(&["pods", "start-pod", pod_id]).await
    }

    /// Stop a pod.
    ///
    /// Equivalent to Lua `Pod:stop()` in runpod.lua L119-122.
    pub async fn stop_pod(&self, pod_id: &str) -> Result<serde_json::Value, DomainError> {
        self.exec(&["pods", "stop-pod", pod_id]).await
    }

    /// Delete a pod permanently.
    ///
    /// Equivalent to Lua `Pod:delete()` in runpod.lua L126-128.
    pub async fn delete_pod(&self, pod_id: &str) -> Result<serde_json::Value, DomainError> {
        self.exec(&["pods", "delete-pod", pod_id]).await
    }

    /// Create a new pod.
    ///
    /// Equivalent to Lua `M.create_pod(spec, opts)` in runpod.lua L521-546.
    pub async fn create_pod(&self, spec_json: &str) -> Result<serde_json::Value, DomainError> {
        self.exec(&["pods", "create-pod", "-j", spec_json]).await
    }

    /// Queue a background download on a pod.
    ///
    /// Equivalent to Lua `Pod:download_add()` in runpod.lua L181-192.
    pub async fn download_add(
        &self,
        pod_id: &str,
        url: &str,
        dest: Option<&str>,
        ssh_key: &str,
    ) -> Result<serde_json::Value, DomainError> {
        let mut args = vec!["download", "add", "-i", ssh_key, pod_id, url];
        if let Some(d) = dest {
            args.push("-d");
            args.push(d);
        }
        self.exec(&args).await
    }

    /// Check download progress on a pod.
    ///
    /// Equivalent to Lua `Pod:download_status()` in runpod.lua L198-206.
    pub async fn download_status(
        &self,
        pod_id: &str,
        job_id: &str,
        ssh_key: &str,
    ) -> Result<serde_json::Value, DomainError> {
        self.exec(&["download", "status", "-i", ssh_key, pod_id, job_id])
            .await
    }

    /// List all downloads on a pod.
    ///
    /// Equivalent to Lua `Pod:download_list()` in runpod.lua L211-218.
    pub async fn download_list(
        &self,
        pod_id: &str,
        ssh_key: &str,
    ) -> Result<serde_json::Value, DomainError> {
        self.exec(&["download", "list", "-i", ssh_key, pod_id])
            .await
    }

    /// Execute a command on a running pod via SSH exec channel.
    ///
    /// Wraps `runpod-cli exec <pod_id> -- <command...>`.
    /// Returns raw stdout/stderr (not JSON-parsed).
    ///
    /// `timeout_secs` controls the **total execution timeout** (Rust-side).
    /// The SSH connection timeout (`-t`) is fixed at 10s (runpod-cli default).
    pub async fn pod_exec(
        &self,
        pod_id: &str,
        command: &[&str],
        ssh_key: Option<&str>,
        timeout_secs: Option<u64>,
    ) -> Result<PodExecOutput, DomainError> {
        let mut args = vec!["exec"];
        if let Some(key) = ssh_key {
            args.push("-i");
            args.push(key);
        }
        // -t is SSH *connection* timeout, not execution timeout.
        // Keep it at the default (10s). Execution timeout is handled below.
        args.push(pod_id);
        args.push("--");
        args.extend(command);

        let child = tokio::process::Command::new("runpod-cli")
            .env("RUNPOD_API_KEY", &self.api_key)
            .args(&args)
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .spawn()
            .map_err(|e| {
                DomainError::CliExecution(format!("failed to spawn runpod-cli exec: {e}"))
            })?;

        let timeout_dur = std::time::Duration::from_secs(timeout_secs.unwrap_or(30));
        // wait_with_output takes ownership, so grab the PID first for kill on timeout.
        let pid = child.id();
        match tokio::time::timeout(timeout_dur, child.wait_with_output()).await {
            Ok(Ok(output)) => Ok(PodExecOutput {
                stdout: String::from_utf8_lossy(&output.stdout).to_string(),
                stderr: String::from_utf8_lossy(&output.stderr).to_string(),
                success: output.status.success(),
                exit_code: output.status.code().unwrap_or(-1),
            }),
            Ok(Err(e)) => Err(DomainError::CliExecution(format!(
                "runpod-cli exec failed: {e}"
            ))),
            Err(_elapsed) => {
                // Timeout: kill the child process to avoid orphans.
                // child is consumed by wait_with_output, so use kill(1) via PID.
                if let Some(id) = pid {
                    let _ = tokio::process::Command::new("kill")
                        .args(["-9", &id.to_string()])
                        .status()
                        .await;
                }
                Err(DomainError::ExecTimeout {
                    seconds: timeout_dur.as_secs(),
                })
            }
        }
    }

    /// Start a background task on a pod.
    ///
    /// Wraps `runpod-cli task run -i <key> <pod_id> -- <command...>`.
    pub async fn task_run(
        &self,
        pod_id: &str,
        command: &[&str],
        ssh_key: &str,
    ) -> Result<serde_json::Value, DomainError> {
        let mut args = vec!["task", "run", "-i", ssh_key, pod_id, "--"];
        args.extend(command);
        self.exec(&args).await
    }

    /// Check task status.
    ///
    /// Wraps `runpod-cli task status -i <key> <pod_id> <job_id>`.
    pub async fn task_status(
        &self,
        pod_id: &str,
        job_id: &str,
        ssh_key: &str,
    ) -> Result<serde_json::Value, DomainError> {
        self.exec(&["task", "status", "-i", ssh_key, pod_id, job_id])
            .await
    }

    /// List all tasks on a pod.
    ///
    /// Wraps `runpod-cli task list -i <key> <pod_id>`.
    pub async fn task_list(
        &self,
        pod_id: &str,
        ssh_key: &str,
    ) -> Result<serde_json::Value, DomainError> {
        self.exec(&["task", "list", "-i", ssh_key, pod_id]).await
    }

    /// View task log output.
    ///
    /// Wraps `runpod-cli task log -i <key> <pod_id> <job_id> [-n <lines>]`.
    pub async fn task_log(
        &self,
        pod_id: &str,
        job_id: &str,
        ssh_key: &str,
        lines: Option<u64>,
    ) -> Result<serde_json::Value, DomainError> {
        let mut args = vec!["task", "log", "-i", ssh_key, pod_id, job_id];
        let lines_str;
        if let Some(n) = lines {
            lines_str = n.to_string();
            args.push("-n");
            args.push(&lines_str);
        }
        self.exec(&args).await
    }

    /// List network volumes.
    ///
    /// Equivalent to Lua `M.volumes(opts)` in runpod.lua L626-633.
    pub async fn list_volumes(&self) -> Result<Vec<serde_json::Value>, DomainError> {
        let result = self
            .exec(&["network-volumes", "list-network-volumes"])
            .await?;
        match result {
            serde_json::Value::Array(vols) => Ok(vols),
            other => Ok(vec![other]),
        }
    }
}

/// Output from a remote command executed on a pod via SSH.
#[derive(Debug, Clone)]
pub struct PodExecOutput {
    pub stdout: String,
    pub stderr: String,
    pub success: bool,
    pub exit_code: i32,
}

/// Parse CLI stdout into JSON value.
///
/// Handles empty output (returns `{}`) and JSON parse errors.
/// Matches Lua `cli()` behavior: empty → `{}`, parse failure → error.
fn parse_cli_output(stdout: &str) -> Result<serde_json::Value, DomainError> {
    let trimmed = stdout.trim();
    if trimmed.is_empty() {
        return Ok(serde_json::Value::Object(serde_json::Map::new()));
    }
    serde_json::from_str(trimmed).map_err(|e| DomainError::ParseError {
        reason: e.to_string(),
        raw: trimmed.to_string(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_empty_output() {
        let result = parse_cli_output("").unwrap();
        assert!(result.is_object());
        assert_eq!(result.as_object().unwrap().len(), 0);
    }

    #[test]
    fn parse_whitespace_only() {
        let result = parse_cli_output("  \n  ").unwrap();
        assert!(result.is_object());
    }

    #[test]
    fn parse_pod_array() {
        let json = r#"[{"id":"abc","name":"test"}]"#;
        let result = parse_cli_output(json).unwrap();
        assert!(result.is_array());
        assert_eq!(result.as_array().unwrap().len(), 1);
        assert_eq!(result[0]["id"], "abc");
    }

    #[test]
    fn parse_single_object() {
        let json = r#"{"id":"abc","status":"RUNNING"}"#;
        let result = parse_cli_output(json).unwrap();
        assert!(result.is_object());
        assert_eq!(result["id"], "abc");
    }

    #[test]
    fn parse_invalid_json() {
        let result = parse_cli_output("not json at all");
        assert!(result.is_err());
        let err = result.unwrap_err();
        match err {
            DomainError::ParseError { reason, raw } => {
                assert!(reason.contains("expected"));
                assert_eq!(raw, "not json at all");
            }
            other => panic!("unexpected error: {other}"),
        }
    }

    #[test]
    fn parse_json_with_trailing_newline() {
        let json = "[{\"id\":\"pod1\"}]\n";
        let result = parse_cli_output(json).unwrap();
        assert!(result.is_array());
    }
}
