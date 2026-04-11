//! syncd プロセスの起動管理。
//!
//! mcp 側から sync 操作を委譲する前に syncd が稼働しているかを確認し、
//! 未起動であれば fork/exec で起動する。
//!
//! # 経路
//!
//! 1. probe で生存確認 → 200 OK なら `Running`
//! 2. PID file 確認 → 存在してプロセス生存中なら起動待ち (300ms × 3)
//! 3. PID file なし / stale → setsid fork で `vdsl-mcp syncd --spawned-by-mcp` を起動
//! 4. healthz 2 秒待機 → 成功なら `Running`
//! 5. spawn 失敗 / healthz タイムアウト → `SpawnFailed` / `SpawnTimeout`

use std::path::Path;
use std::time::Duration;

use tracing::warn;

use crate::infra::config::SyncdConfig;
use crate::infra::syncd_client::SyncdClient;

// =============================================================================
// SyncdStatus
// =============================================================================

/// `ensure_syncd_running` の戻り値。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SyncdStatus {
    /// syncd は稼働中であり HTTP 委譲が可能。
    Running,
    /// PID file はあるがプロセスが応答しない (起動途中または stuck)。
    Stuck,
    /// spawn はできたが healthz タイムアウト (2 秒以内に起動しなかった)。
    SpawnTimeout,
    /// spawn 自体が失敗した (exec エラー等)。
    SpawnFailed,
}

// =============================================================================
// ensure_syncd_running
// =============================================================================

/// syncd が稼働していることを確認し、未起動なら起動を試みる。
///
/// # 経路
///
/// 1. probe (300ms timeout) → 成功なら即 `Running` を返す
/// 2. PID file 確認
///    - ファイルあり + プロセス生存中 → 起動待ち (300ms × 3 retry)
///    - ファイルなし / stale → 次ステップへ
/// 3. spawn (`vdsl-mcp syncd --spawned-by-mcp`)
///    - healthz 2 秒待機 → 成功なら `Running`
///    - タイムアウト → `SpawnTimeout`
/// 4. spawn 自体が失敗 → `SpawnFailed`
pub async fn ensure_syncd_running(
    cfg: &SyncdConfig,
    client: &SyncdClient,
    pod_id: Option<&str>,
) -> SyncdStatus {
    // 1. probe — 既存 syncd が pod_id mismatch の場合は SIGTERM で落として再起動する (Bug #4)。
    if let Some(health) = client.fetch_health().await {
        let running_pod = health.pod_id.as_deref();
        if running_pod == pod_id {
            return SyncdStatus::Running;
        }
        warn!(
            running_pod = ?running_pod,
            requested_pod = ?pod_id,
            "syncd: pod_id mismatch — restarting syncd to rebuild SDK with new pod"
        );
        if let Some(pid) = read_pid_file(&cfg.pid_file) {
            terminate_and_wait(pid, &cfg.pid_file).await;
        }
        // fall through to spawn
    }

    // 2. PID file 確認
    if let Some(pid) = read_pid_file(&cfg.pid_file) {
        if process_alive(pid) {
            // プロセスはいるが HTTP 未応答 → 起動途中の可能性。少し待つ。
            for _ in 0..3 {
                tokio::time::sleep(Duration::from_millis(300)).await;
                if client.probe().await {
                    return SyncdStatus::Running;
                }
            }
            warn!(pid, "syncd: process alive but not responding to healthz");
            return SyncdStatus::Stuck;
        }
        // stale PID file はそのまま spawn に進む (syncd が上書きする)
    }

    // 3. spawn
    let exe = match std::env::current_exe() {
        Ok(p) => p,
        Err(e) => {
            warn!(error = %e, "syncd: current_exe failed, using 'vdsl-mcp'");
            std::path::PathBuf::from("vdsl-mcp")
        }
    };

    #[cfg(unix)]
    {
        let args = ["syncd", "--spawned-by-mcp"];
        // frontend 側で resolve した work_dir を env で伝播する。
        // これをしないと syncd は自プロセスの CWD から current_dir() で解決し、
        // frontend と別の .vdsl/sync.db を開いてしまう (Bug #1)。
        let work_dir = match cfg.resolved_work_dir() {
            Ok(p) => Some(p),
            Err(e) => {
                warn!(error = %e, "syncd: failed to resolve work_dir for env propagation");
                None
            }
        };
        let work_dir_str = work_dir.as_ref().map(|p| p.to_string_lossy().into_owned());
        let mut envs: Vec<(&str, &str)> = Vec::new();
        if let Some(s) = work_dir_str.as_deref() {
            envs.push(("VDSL_WORK_DIR", s));
        }
        // pod_id を VDSL_SYNCD_POD_ID で伝播する。syncd 側はこれを読んで SDK
        // 初期化時に pod Location / pod routes を登録する (Bug #4)。
        // 現状は spawn 時固定: syncd 稼働中の pod 切替は反映されない。
        if let Some(pid) = pod_id {
            envs.push(("VDSL_SYNCD_POD_ID", pid));
        }
        match crate::interface::syncd::spawn_detached(&exe, &args, &envs) {
            Ok(child_pid) => {
                tracing::info!(child_pid, "syncd: spawned detached process");
                // healthz 2 秒待機 (100ms × 20)
                for _ in 0..20 {
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    if client.probe().await {
                        return SyncdStatus::Running;
                    }
                }
                warn!("syncd: spawned but healthz timed out after 2s");
                SyncdStatus::SpawnTimeout
            }
            Err(e) => {
                warn!(error = %e, exe = %exe.display(), "syncd: spawn_detached failed");
                SyncdStatus::SpawnFailed
            }
        }
    }

    #[cfg(not(unix))]
    {
        // Windows では setsid による orphan 化ができないため常に SpawnFailed。
        // Phase 1 は Mac のみが対象。
        let _ = exe;
        warn!("syncd: spawn_detached is not supported on non-unix platforms");
        SyncdStatus::SpawnFailed
    }
}

// =============================================================================
// PID file helpers
// =============================================================================

/// PID file の先頭行から PID (i32) を読み取る。
///
/// ファイルが存在しない / 読み取れない / パース失敗の場合は `None`。
pub fn read_pid_file(path: &Path) -> Option<i32> {
    let contents = std::fs::read_to_string(path).ok()?;
    contents.lines().next()?.trim().parse().ok()
}

/// 既存 syncd を SIGTERM で停止し、プロセスが終了するまで待機する。
///
/// pod_id mismatch による再起動用。graceful shutdown (SIGTERM → PID file 削除) を期待し、
/// 最大 3 秒待つ。タイムアウトしたらそのまま spawn に進む (既存 PID file は syncd が上書き)。
#[cfg(unix)]
async fn terminate_and_wait(pid: i32, pid_file: &Path) {
    use nix::sys::signal::{kill, Signal};
    use nix::unistd::Pid;

    match kill(Pid::from_raw(pid), Signal::SIGTERM) {
        Ok(()) => tracing::info!(pid, "syncd: SIGTERM sent for pod_id mismatch restart"),
        Err(e) => {
            warn!(pid, error = %e, "syncd: SIGTERM failed, proceeding to spawn anyway");
            return;
        }
    }
    for _ in 0..30 {
        tokio::time::sleep(Duration::from_millis(100)).await;
        if !process_alive(pid) {
            tracing::info!(pid, "syncd: previous process exited");
            // PID file が残っていれば syncd spawn 側で上書きされる
            let _ = pid_file;
            return;
        }
    }
    warn!(
        pid,
        "syncd: SIGTERM timed out after 3s, spawn will still proceed"
    );
}

#[cfg(not(unix))]
async fn terminate_and_wait(_pid: i32, _pid_file: &Path) {
    // 非 unix は phase 1 対象外。
}

/// プロセスが生存しているかを確認する。
///
/// Unix: `kill(pid, 0)`
/// - `Ok(())` → 生存
/// - `Err(ESRCH)` → 不在 → false
/// - `Err(EPERM)` → 他ユーザーのプロセス → 安全側に倒して true
#[cfg(unix)]
pub fn process_alive(pid: i32) -> bool {
    use nix::errno::Errno;
    use nix::sys::signal::kill;
    use nix::unistd::Pid;

    match kill(Pid::from_raw(pid), None) {
        Ok(()) => true,
        Err(Errno::ESRCH) => false,
        Err(_) => true,
    }
}

#[cfg(not(unix))]
pub fn process_alive(_pid: i32) -> bool {
    // Windows では PID 存在確認の安全な方法がないため、常に生存とみなす。
    true
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::infra::config::SyncdConfig;
    use std::path::PathBuf;
    use tempfile::TempDir;

    fn test_config(port: u16, pid_file: PathBuf) -> SyncdConfig {
        SyncdConfig {
            port,
            pid_file,
            token_file: PathBuf::from("/tmp/test_syncd.token"),
            work_dir: None,
            debounce_ms: 500,
            log_level: "info".to_string(),
        }
    }

    /// stale PID file (存在しない PID) がある場合に ensure_syncd_running が spawn に進むことを確認。
    /// spawn 自体は実際には試みるが、テスト環境では syncd は起動しないので SpawnTimeout/SpawnFailed になる。
    #[tokio::test]
    async fn ensure_syncd_running_with_stale_pid_progresses_to_spawn() {
        let dir = TempDir::new().unwrap();
        let pid_path = dir.path().join("syncd.pid");

        // 存在しない PID を書く (stale)
        std::fs::write(&pid_path, "99999999\n2024-01-01T00:00:00Z\n19998\n").unwrap();

        // ポート 19998 は使われていない前提
        let cfg = test_config(19998, pid_path.clone());
        let client = SyncdClient::from_config(&cfg).expect("client build should succeed");

        let status = ensure_syncd_running(&cfg, &client, None).await;

        // spawn に進んだ結果として SpawnTimeout か SpawnFailed になるはず
        // (テスト環境で syncd が実際に起動することはない)
        assert!(
            matches!(status, SyncdStatus::SpawnTimeout | SyncdStatus::SpawnFailed),
            "expected SpawnTimeout or SpawnFailed, got {:?}",
            status
        );
    }

    #[test]
    fn read_pid_file_valid() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("syncd.pid");
        std::fs::write(&path, "12345\n2024-01-01T00:00:00Z\n7823\n").unwrap();
        assert_eq!(read_pid_file(&path), Some(12345));
    }

    #[test]
    fn read_pid_file_missing_returns_none() {
        let path = PathBuf::from("/nonexistent/syncd.pid");
        assert_eq!(read_pid_file(&path), None);
    }

    #[cfg(unix)]
    #[test]
    fn process_alive_current_process() {
        let pid = std::process::id() as i32;
        assert!(process_alive(pid), "current process should be alive");
    }

    #[cfg(unix)]
    #[test]
    fn process_alive_nonexistent_pid() {
        // PID 99999999 は通常存在しない
        assert!(
            !process_alive(99999999),
            "nonexistent pid should not be alive"
        );
    }
}
