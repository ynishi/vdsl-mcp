//! Remote shell abstraction for executing commands on different hosts.
//!
//! - [`LocalShell`]: runs via `tokio::process::Command` on the local machine
//! - `PodShell`: runs via RunPod exec API on a GPU pod (downstream crate)
//! - `SshShell`: runs via SSH (future)
//!
//! [`StorageBackend`](super::backend::StorageBackend) implementations compose
//! a `RemoteShell` to run transfer commands (rclone, rsync, etc.) on the
//! appropriate host.

use async_trait::async_trait;

use crate::domain::error::SyncError;

/// Output from a shell command execution.
#[derive(Debug, Clone)]
pub struct ShellOutput {
    pub stdout: String,
    pub stderr: String,
    pub success: bool,
    pub exit_code: i32,
}

/// Abstract shell for executing commands on a location's host.
#[async_trait]
pub trait RemoteShell: Send + Sync {
    /// Execute a command on this host.
    ///
    /// `args[0]` is the program name, `args[1..]` are arguments.
    async fn exec(
        &self,
        args: &[&str],
        timeout_secs: Option<u64>,
    ) -> Result<ShellOutput, SyncError>;
}

/// Execute commands on the local machine via `tokio::process::Command`.
pub struct LocalShell;

const LOCAL_DEFAULT_TIMEOUT_SECS: u64 = 600;

#[async_trait]
impl RemoteShell for LocalShell {
    async fn exec(
        &self,
        args: &[&str],
        timeout_secs: Option<u64>,
    ) -> Result<ShellOutput, SyncError> {
        if args.is_empty() {
            return Err(SyncError::TransferFailed("empty command".into()));
        }

        let mut cmd = tokio::process::Command::new(args[0]);
        if args.len() > 1 {
            cmd.args(&args[1..]);
        }

        let timeout =
            std::time::Duration::from_secs(timeout_secs.unwrap_or(LOCAL_DEFAULT_TIMEOUT_SECS));

        let output = tokio::time::timeout(timeout, cmd.output())
            .await
            .map_err(|_| {
                SyncError::TransferFailed(format!(
                    "command timed out after {}s: {}",
                    timeout.as_secs(),
                    args.join(" ")
                ))
            })?
            .map_err(|e| {
                SyncError::TransferFailed(format!("exec failed ({}): {e}", args[0]))
            })?;

        Ok(ShellOutput {
            stdout: String::from_utf8_lossy(&output.stdout).to_string(),
            stderr: String::from_utf8_lossy(&output.stderr).to_string(),
            success: output.status.success(),
            exit_code: output.status.code().unwrap_or(-1),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn local_shell_echo() {
        let shell = LocalShell;
        let output = shell.exec(&["echo", "hello"], None).await.unwrap();
        assert!(output.success);
        assert_eq!(output.stdout.trim(), "hello");
        assert_eq!(output.exit_code, 0);
    }

    #[tokio::test]
    async fn local_shell_empty_args() {
        let shell = LocalShell;
        let result = shell.exec(&[], None).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn local_shell_nonexistent_command() {
        let shell = LocalShell;
        let result = shell
            .exec(&["__nonexistent_command_12345__"], None)
            .await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn local_shell_exit_code() {
        let shell = LocalShell;
        let output = shell.exec(&["false"], None).await.unwrap();
        assert!(!output.success);
        assert_ne!(output.exit_code, 0);
    }
}
