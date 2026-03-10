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

use vdsl_sync::{RemoteShell, ShellOutput, SyncError};

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

#[async_trait]
impl RemoteShell for RunPodCliShell {
    async fn exec(
        &self,
        args: &[&str],
        timeout_secs: Option<u64>,
    ) -> Result<ShellOutput, SyncError> {
        if args.is_empty() {
            return Err(SyncError::TransferFailed("empty command".into()));
        }

        let result = self
            .cli
            .pod_exec(&self.pod_id, args, self.ssh_key.as_deref(), timeout_secs)
            .await
            .map_err(|e| {
                SyncError::TransferFailed(format!(
                    "pod exec failed on pod={}, cmd={}: {e}",
                    self.pod_id,
                    args.join(" ")
                ))
            })?;

        Ok(ShellOutput {
            stdout: result.stdout,
            stderr: result.stderr,
            success: result.success,
            exit_code: Some(result.exit_code),
        })
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
