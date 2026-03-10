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

/// Mock shell for testing — returns configurable responses.
#[cfg(any(test, feature = "test-utils"))]
pub mod mock {
    use super::*;
    use std::collections::HashMap;
    use tokio::sync::Mutex;

    /// Mock file entry with optional hash and size.
    #[derive(Clone)]
    pub struct MockFile {
        pub sha256: String,
        pub size: u64,
    }

    impl MockFile {
        pub fn new(sha256: impl Into<String>, size: u64) -> Self {
            Self {
                sha256: sha256.into(),
                size,
            }
        }
    }

    /// A mock RemoteShell that simulates file operations on a remote host.
    ///
    /// Supports:
    /// - `test -f <path>` — file existence check
    /// - `sha256sum <path>` — returns configured hash
    /// - `stat --format=%s <path>` — returns configured size
    ///
    /// - `exec_log`: records all commands executed (for assertions)
    pub struct MockShell {
        files: Mutex<HashMap<String, MockFile>>,
        pub exec_log: Mutex<Vec<Vec<String>>>,
    }

    impl MockShell {
        /// Create with a set of files (path → MockFile).
        pub fn with_files(files: impl IntoIterator<Item = (impl Into<String>, MockFile)>) -> Self {
            Self {
                files: Mutex::new(files.into_iter().map(|(k, v)| (k.into(), v)).collect()),
                exec_log: Mutex::new(Vec::new()),
            }
        }

        /// Create with paths only (existence checks only, no hash/size).
        pub fn new(existing: impl IntoIterator<Item = impl Into<String>>) -> Self {
            Self::with_files(
                existing
                    .into_iter()
                    .map(|p| (p, MockFile::new("0000000000000000", 0))),
            )
        }
    }

    #[async_trait]
    impl RemoteShell for MockShell {
        async fn exec(
            &self,
            args: &[&str],
            _timeout_secs: Option<u64>,
        ) -> Result<ShellOutput, SyncError> {
            let owned: Vec<String> = args.iter().map(|s| s.to_string()).collect();
            self.exec_log.lock().await.push(owned);

            // Simulate `test -f <path>`
            if args.len() >= 3 && args[0] == "test" && args[1] == "-f" {
                let path = args[2];
                let exists = self.files.lock().await.contains_key(path);
                return Ok(ShellOutput {
                    stdout: String::new(),
                    stderr: String::new(),
                    success: exists,
                    exit_code: if exists { 0 } else { 1 },
                });
            }

            // Simulate `sha256sum <path>`
            if args.len() >= 2 && args[0] == "sha256sum" {
                let path = args[1];
                let files = self.files.lock().await;
                if let Some(f) = files.get(path) {
                    return Ok(ShellOutput {
                        stdout: format!("{}  {}\n", f.sha256, path),
                        stderr: String::new(),
                        success: true,
                        exit_code: 0,
                    });
                }
                return Ok(ShellOutput {
                    stdout: String::new(),
                    stderr: format!("sha256sum: {path}: No such file or directory\n"),
                    success: false,
                    exit_code: 1,
                });
            }

            // Simulate `stat --format=%s <path>` (GNU) or `stat -f%z <path>` (BSD)
            if args.len() >= 3 && args[0] == "stat" {
                let path = args.last().expect("args is non-empty");
                let files = self.files.lock().await;
                if let Some(f) = files.get(*path) {
                    return Ok(ShellOutput {
                        stdout: format!("{}\n", f.size),
                        stderr: String::new(),
                        success: true,
                        exit_code: 0,
                    });
                }
                return Ok(ShellOutput {
                    stdout: String::new(),
                    stderr: format!("stat: cannot stat '{path}': No such file or directory\n"),
                    success: false,
                    exit_code: 1,
                });
            }

            // Default: success with empty output
            Ok(ShellOutput {
                stdout: String::new(),
                stderr: String::new(),
                success: true,
                exit_code: 0,
            })
        }
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
