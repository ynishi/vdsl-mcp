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

use crate::application::error::SyncError;
use crate::infra::error::InfraError;

/// Output from a shell command execution.
#[derive(Debug, Clone)]
pub struct ShellOutput {
    pub stdout: String,
    pub stderr: String,
    pub success: bool,
    pub exit_code: Option<i32>,
}

/// Per-file inspection result from batch_inspect.
#[derive(Debug, Clone)]
pub struct FileInspection {
    /// Relative path (same key as input).
    pub relative_path: String,
    /// SHA-256 hex hash of the file content.
    pub sha256: String,
    /// File size in bytes.
    pub size: u64,
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

    /// Execute a shell script on this host.
    ///
    /// Default: `exec(&["sh", "-c", script])`.
    /// Remote shells may override to use file-based transfer (SCP)
    /// to avoid shell escaping issues with SSH.
    async fn exec_script(
        &self,
        script: &str,
        timeout_secs: Option<u64>,
    ) -> Result<ShellOutput, SyncError> {
        self.exec(&["sh", "-c", script], timeout_secs).await
    }

    /// Batch inspect files: get sha256 + size for ALL paths in one exec call.
    ///
    /// Constructs a single shell script that processes every file in the list
    /// and outputs `<sha256> <size> <relative_path>` per line. Parsed on return.
    ///
    /// Timeout scales with file count: base 30s + 2s per file.
    async fn batch_inspect(
        &self,
        root: &str,
        relative_paths: &[String],
    ) -> Result<Vec<FileInspection>, SyncError> {
        if relative_paths.is_empty() {
            return Ok(Vec::new());
        }

        // Build heredoc file list embedded in a single sh -c script.
        // Each file is read line-by-line, sha256sum + stat in one pass.
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

        let timeout = 30 + (relative_paths.len() as u64 * 2);
        let output = self.exec(&["sh", "-c", &script], Some(timeout)).await?;

        if !output.success {
            return Err(InfraError::Transfer {
                reason: format!("batch_inspect failed: {}", output.stderr.trim()),
            }
            .into());
        }

        let mut results = Vec::with_capacity(relative_paths.len());
        for line in output.stdout.lines() {
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
            return Err(InfraError::Transfer {
                reason: "empty command".into(),
            }
            .into());
        }

        let mut cmd = tokio::process::Command::new(args[0]);
        if args.len() > 1 {
            cmd.args(&args[1..]);
        }

        let timeout =
            std::time::Duration::from_secs(timeout_secs.unwrap_or(LOCAL_DEFAULT_TIMEOUT_SECS));

        let output = tokio::time::timeout(timeout, cmd.output())
            .await
            .map_err(|_| -> SyncError {
                InfraError::Transfer {
                    reason: format!(
                        "command timed out after {}s: {}",
                        timeout.as_secs(),
                        args.join(" ")
                    ),
                }
                .into()
            })?
            .map_err(|e| -> SyncError {
                InfraError::Transfer {
                    reason: format!("exec failed ({}): {e}", args[0]),
                }
                .into()
            })?;

        Ok(ShellOutput {
            stdout: String::from_utf8_lossy(&output.stdout).to_string(),
            stderr: String::from_utf8_lossy(&output.stderr).to_string(),
            success: output.status.success(),
            exit_code: output.status.code(),
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
                    exit_code: Some(if exists { 0 } else { 1 }),
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
                        exit_code: Some(0),
                    });
                }
                return Ok(ShellOutput {
                    stdout: String::new(),
                    stderr: format!("sha256sum: {path}: No such file or directory\n"),
                    success: false,
                    exit_code: Some(1),
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
                        exit_code: Some(0),
                    });
                }
                return Ok(ShellOutput {
                    stdout: String::new(),
                    stderr: format!("stat: cannot stat '{path}': No such file or directory\n"),
                    success: false,
                    exit_code: Some(1),
                });
            }

            // Default: success with empty output
            Ok(ShellOutput {
                stdout: String::new(),
                stderr: String::new(),
                success: true,
                exit_code: Some(0),
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
        assert_eq!(output.exit_code, Some(0));
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
        let result = shell.exec(&["__nonexistent_command_12345__"], None).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn local_shell_exit_code() {
        let shell = LocalShell;
        let output = shell.exec(&["false"], None).await.unwrap();
        assert!(!output.success);
        assert_ne!(output.exit_code, Some(0));
    }
}
