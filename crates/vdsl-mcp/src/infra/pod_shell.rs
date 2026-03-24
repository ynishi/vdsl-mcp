//! RemoteShell adapter for RunPod pod execution.
//!
//! Bridges [`RunPodCli::pod_exec`] to the [`RemoteShell`] trait defined in `vdsl-sync`,
//! enabling route-based transfers where the source or destination is a GPU pod.
//!
//! # Architecture
//!
//! `vdsl-sync` defines the `RemoteShell` trait (shell abstraction).
//! `vdsl-mcp` owns `RunPodCli` (RunPod infrastructure).
//! This adapter lives in `vdsl-mcp` to avoid circular dependencies.

use async_trait::async_trait;

use vdsl_sync::{FileInspection, InfraError, RemoteShell, ShellOutput, SyncError};

use super::runpod_cli::RunPodCli;

/// RemoteShell implementation that executes commands on a RunPod GPU pod via SSH.
///
/// Wraps [`RunPodCli::pod_exec`] to satisfy the [`RemoteShell`] trait contract.
/// Each instance is bound to a specific pod (identified by `pod_id`).
pub struct RunPodCliShell {
    cli: RunPodCli,
    pod_id: String,
    ssh_key: Option<String>,
}

impl RunPodCliShell {
    /// Create a new shell bound to a specific pod.
    ///
    /// - `cli`: RunPod CLI wrapper (holds API key internally)
    /// - `pod_id`: Target pod identifier
    /// - `ssh_key`: Optional path to SSH private key for pod access
    pub fn new(cli: RunPodCli, pod_id: String, ssh_key: Option<String>) -> Self {
        Self {
            cli,
            pod_id,
            ssh_key,
        }
    }
}

impl std::fmt::Debug for RunPodCliShell {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RunPodCliShell")
            .field("pod_id", &self.pod_id)
            .field("ssh_key", &self.ssh_key.as_deref().map(|_| "[SET]"))
            .finish()
    }
}

/// Max time to wait for a batch_inspect task to complete.
const BATCH_INSPECT_TIMEOUT_SECS: u64 = 600;
/// Initial poll interval (doubles each iteration, capped at 10s).
const BATCH_INSPECT_POLL_INITIAL_SECS: u64 = 2;
/// Maximum poll interval.
const BATCH_INSPECT_POLL_MAX_SECS: u64 = 10;

#[async_trait]
impl RemoteShell for RunPodCliShell {
    async fn exec(
        &self,
        args: &[&str],
        timeout_secs: Option<u64>,
    ) -> Result<ShellOutput, SyncError> {
        if args.is_empty() {
            return Err(InfraError::Transfer {
                reason: "empty command".into(),
            }
            .into());
        }

        let result = self
            .cli
            .pod_exec(&self.pod_id, args, self.ssh_key.as_deref(), timeout_secs)
            .await
            .map_err(|e| -> SyncError {
                InfraError::Transfer {
                    reason: format!(
                        "pod exec failed on pod={}, cmd={}: {e}",
                        self.pod_id,
                        args.join(" ")
                    ),
                }
                .into()
            })?;

        Ok(ShellOutput {
            stdout: result.stdout,
            stderr: result.stderr,
            success: result.success,
            exit_code: Some(result.exit_code),
        })
    }

    /// Override: use RunPod task_run_script (SCP + exec) to avoid shell escaping issues.
    ///
    /// Flow:
    /// 1. Upload script via `task_run_script` (SCP to Pod, then `sh`)
    /// 2. Poll `task_status` with exponential backoff
    /// 3. On "done", fetch output via `task_log`
    /// 4. Return ShellOutput with stdout/stderr and exit code
    async fn exec_script(
        &self,
        script: &str,
        timeout_secs: Option<u64>,
    ) -> Result<ShellOutput, SyncError> {
        let ssh_key = self.ssh_key.as_deref().ok_or_else(|| -> SyncError {
            InfraError::Transfer {
                reason: "exec_script requires ssh_key for task execution".into(),
            }
            .into()
        })?;

        let task_result = self
            .cli
            .task_run_script(&self.pod_id, script, ssh_key)
            .await
            .map_err(|e| -> SyncError {
                InfraError::Transfer {
                    reason: format!("exec_script task_run_script failed: {e}"),
                }
                .into()
            })?;

        let job_id = task_result["id"]
            .as_str()
            .ok_or_else(|| -> SyncError {
                InfraError::Transfer {
                    reason: format!("exec_script task_run returned no job id: {task_result:?}"),
                }
                .into()
            })?
            .to_string();

        let timeout = timeout_secs.unwrap_or(BATCH_INSPECT_TIMEOUT_SECS);
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(timeout);
        let mut interval = BATCH_INSPECT_POLL_INITIAL_SECS;

        let exit_code;
        loop {
            tokio::time::sleep(std::time::Duration::from_secs(interval)).await;

            let status = self
                .cli
                .task_status(&self.pod_id, &job_id, ssh_key)
                .await
                .map_err(|e| -> SyncError {
                    InfraError::Transfer {
                        reason: format!("exec_script task_status failed: {e}"),
                    }
                    .into()
                })?;

            let state = status["state"].as_str().unwrap_or("unknown");

            if state == "done" {
                exit_code = status["exit_code"]
                    .as_i64()
                    .or_else(|| status["exit_code"].as_str().and_then(|s| s.parse().ok()))
                    .unwrap_or(-1) as i32;
                break;
            }

            if std::time::Instant::now() >= deadline {
                return Err(InfraError::Transfer {
                    reason: format!(
                        "exec_script timed out after {timeout}s (job_id={job_id}, last state={state})"
                    ),
                }
                .into());
            }

            interval = (interval * 2).min(BATCH_INSPECT_POLL_MAX_SECS);
        }

        // Fetch output
        let log_result = self
            .cli
            .task_log(&self.pod_id, &job_id, ssh_key, None)
            .await
            .map_err(|e| -> SyncError {
                InfraError::Transfer {
                    reason: format!("exec_script task_log failed: {e}"),
                }
                .into()
            })?;

        let stdout = log_result["log"].as_str().unwrap_or("").to_string();

        Ok(ShellOutput {
            stdout,
            stderr: String::new(),
            success: exit_code == 0,
            exit_code: Some(exit_code),
        })
    }

    /// Override: use RunPod task_run (background task) + poll instead of blocking exec.
    ///
    /// Flow:
    /// 1. Build single shell script (sha256sum + stat for all files)
    /// 2. Start as background task via `task_run`
    /// 3. Poll `task_status` with exponential backoff (2s → 4s → 8s → 10s cap)
    /// 4. On "done", fetch output via `task_log`
    /// 5. Parse `<sha256> <size> <relative_path>` lines
    ///
    /// Deadline: 600s. On timeout → error (not infinite loop).
    async fn batch_inspect(
        &self,
        root: &str,
        relative_paths: &[String],
    ) -> Result<Vec<FileInspection>, SyncError> {
        if relative_paths.is_empty() {
            return Ok(Vec::new());
        }

        let ssh_key = self.ssh_key.as_deref().ok_or_else(|| -> SyncError {
            InfraError::Transfer {
                reason: "batch_inspect requires ssh_key for task execution".into(),
            }
            .into()
        })?;

        // Build the inspection script: one sh that processes all files.
        // Uses task_run_script (SCP upload) to avoid shell escaping issues.
        let mut script = format!(
            "cd '{}' && while IFS= read -r f; do \
             h=$(sha256sum \"$f\" 2>/dev/null | cut -d' ' -f1); \
             s=$(stat --format=%s \"$f\" 2>/dev/null || echo 0); \
             [ -n \"$h\" ] && printf '%s %s %s\\n' \"$h\" \"$s\" \"$f\"; \
             done <<'__VDSL_FILELIST__'\n",
            root
        );
        for rel in relative_paths {
            script.push_str(rel);
            script.push('\n');
        }
        script.push_str("__VDSL_FILELIST__");

        // --- 1. Start as background task (SCP + exec, no shell escaping) ---
        let task_result = self
            .cli
            .task_run_script(&self.pod_id, &script, ssh_key)
            .await
            .map_err(|e| -> SyncError {
                InfraError::Transfer {
                    reason: format!("batch_inspect task_run failed: {e}"),
                }
                .into()
            })?;

        let job_id = task_result["id"]
            .as_str()
            .ok_or_else(|| -> SyncError {
                InfraError::Transfer {
                    reason: format!("batch_inspect task_run returned no job id: {task_result:?}"),
                }
                .into()
            })?
            .to_string();

        tracing::info!(
            pod_id = %self.pod_id,
            job_id = %job_id,
            file_count = relative_paths.len(),
            "batch_inspect: task started"
        );

        // --- 2. Poll task_status with exponential backoff ---
        let deadline =
            std::time::Instant::now() + std::time::Duration::from_secs(BATCH_INSPECT_TIMEOUT_SECS);
        let mut interval = BATCH_INSPECT_POLL_INITIAL_SECS;

        loop {
            tokio::time::sleep(std::time::Duration::from_secs(interval)).await;

            let status = self
                .cli
                .task_status(&self.pod_id, &job_id, ssh_key)
                .await
                .map_err(|e| -> SyncError {
                    InfraError::Transfer {
                        reason: format!("batch_inspect task_status failed: {e}"),
                    }
                    .into()
                })?;

            let state = status["state"].as_str().unwrap_or("unknown");

            if state == "done" {
                let exit_code = status["exit_code"]
                    .as_i64()
                    .or_else(|| status["exit_code"].as_str().and_then(|s| s.parse().ok()))
                    .unwrap_or(-1);

                if exit_code != 0 {
                    let log_snippet = status["log"].as_str().unwrap_or("");
                    return Err(InfraError::Transfer {
                        reason: format!(
                            "batch_inspect task exited with code {exit_code}: {log_snippet}"
                        ),
                    }
                    .into());
                }
                break;
            }

            if std::time::Instant::now() >= deadline {
                return Err(InfraError::Transfer {
                    reason: format!(
                        "batch_inspect timed out after {BATCH_INSPECT_TIMEOUT_SECS}s \
                         (job_id={job_id}, last state={state}, files={})",
                        relative_paths.len()
                    ),
                }
                .into());
            }

            // Exponential backoff capped at BATCH_INSPECT_POLL_MAX_SECS
            interval = (interval * 2).min(BATCH_INSPECT_POLL_MAX_SECS);
        }

        // --- 3. Fetch output via task_log ---
        let log_result = self
            .cli
            .task_log(&self.pod_id, &job_id, ssh_key, None)
            .await
            .map_err(|e| -> SyncError {
                InfraError::Transfer {
                    reason: format!("batch_inspect task_log failed: {e}"),
                }
                .into()
            })?;

        let stdout = log_result["log"].as_str().unwrap_or("");

        tracing::info!(
            pod_id = %self.pod_id,
            job_id = %job_id,
            output_lines = stdout.lines().count(),
            "batch_inspect: task completed, parsing results"
        );

        // --- 4. Parse output ---
        let mut results = Vec::with_capacity(relative_paths.len());
        for line in stdout.lines() {
            // Format: <sha256_hex> <size> <relative_path>
            let mut parts = line.splitn(3, ' ');
            let sha256 = match parts.next() {
                Some(h) if h.len() == 64 => h.to_string(),
                _ => continue,
            };
            let size = parts
                .next()
                .and_then(|s| s.parse::<u64>().ok())
                .unwrap_or(0);
            let relative_path = match parts.next() {
                Some(p) if !p.is_empty() => p.to_string(),
                _ => continue,
            };
            results.push(FileInspection {
                relative_path,
                sha256,
                size,
            });
        }

        Ok(results)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn debug_redacts_ssh_key() {
        let shell = RunPodCliShell::new(
            RunPodCli::new("test-key".into()),
            "pod-123".into(),
            Some("/home/user/.ssh/id_ed25519".into()),
        );
        let debug = format!("{:?}", shell);
        assert!(debug.contains("pod-123"));
        assert!(debug.contains("[SET]"));
        assert!(!debug.contains("id_ed25519"));
    }

    #[test]
    fn debug_without_ssh_key() {
        let shell = RunPodCliShell::new(RunPodCli::new("test-key".into()), "pod-456".into(), None);
        let debug = format!("{:?}", shell);
        assert!(debug.contains("pod-456"));
        assert!(debug.contains("None"));
    }

    #[tokio::test]
    async fn empty_args_returns_error() {
        let shell = RunPodCliShell::new(RunPodCli::new("test-key".into()), "pod-789".into(), None);
        let result = shell.exec(&[], None).await;
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("empty command"));
    }
}
