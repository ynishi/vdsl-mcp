use crate::infra::config::SyncdConfig;
use crate::infra::sync_db::SyncDb;
use crate::infra::sync_tasks::SyncTaskManager;
use crate::infra::syncd_token;
use crate::interface::syncd_http::{router, SyncdState};
use crate::interface::syncd_watcher::spawn_watcher;
use std::path::{Path, PathBuf};
use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;
use tracing::{info, warn};

/// syncd デーモンのエントリポイント。
///
/// Subtask 2 で HTTP server を、Subtask 3 で watcher を追加する。
/// 本 subtask では PID file 競合検知 → SIGINT/SIGTERM 待機 → graceful shutdown のみ。
pub async fn run(cfg: SyncdConfig, spawned_by_mcp: bool) -> anyhow::Result<()> {
    info!(
        port = cfg.port,
        pid_file = %cfg.pid_file.display(),
        spawned_by_mcp,
        "syncd: starting"
    );

    let work_dir = cfg.resolved_work_dir()?;
    info!(work_dir = %work_dir.display(), "syncd: work_dir resolved");

    // PID file 取得 (競合検知 + 書き込み)
    let _pid_guard = PidFile::acquire(&cfg.pid_file, cfg.port)?;

    // HTTP auth token: 既存があれば読み、なければ生成 (0600)
    let auth_token = syncd_token::load_or_generate(&cfg.token_file)
        .map_err(|e| anyhow::anyhow!("syncd: failed to prepare token file: {e}"))?;
    info!(token_file = %cfg.token_file.display(), "syncd: auth token ready");

    // DB 構築 (syncd が単独所有)
    let sync_db = Arc::new(SyncDb::new(&work_dir));
    let persistence = sync_db.ensure().await?;

    // SDK 構築
    let (sdk, _) = crate::interface::mcp::build_sdk(&sync_db, None, &persistence).await?;

    // TaskManager 構築 (syncd 専用 — recover を実行する)
    let task_mgr = Arc::new(SyncTaskManager::new());
    task_mgr.set_store_for_syncd(persistence).await;

    let state = Arc::new(SyncdState {
        cfg: cfg.clone(),
        sdk,
        task_mgr,
        started_at: Instant::now(),
        auto_sync_running: Arc::new(AtomicBool::new(false)),
        auto_sync_pending: Arc::new(AtomicBool::new(false)),
        auth_token,
    });

    let listener = tokio::net::TcpListener::bind(("127.0.0.1", cfg.port)).await?;
    info!(port = cfg.port, "syncd: HTTP server listening");

    let app = router(state.clone());

    // watcher 起動 (Subtask 3)
    let excludes = build_default_excludes()?;
    let watcher_handle = spawn_watcher(
        work_dir.clone(),
        Duration::from_millis(cfg.debounce_ms),
        excludes,
        state.clone(),
    )?;
    info!(work_dir = %work_dir.display(), "syncd: watcher started");

    // graceful shutdown: SIGINT / SIGTERM まで待機
    axum::serve(listener, app)
        .with_graceful_shutdown(wait_shutdown())
        .await?;

    info!("syncd: HTTP server shut down");

    // watcher を停止する (Debouncer drop で notify スレッドが停止する)
    drop(watcher_handle);
    info!("syncd: watcher stopped");

    info!("syncd: shutting down");
    // _pid_guard の Drop で PID file を削除する
    Ok(())
}

/// SIGINT / SIGTERM を待機する。
async fn wait_shutdown() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{signal, SignalKind};

        // SAFETY: SignalKind::terminate / interrupt は標準シグナル。
        // signal() が Err を返すのはシグナルハンドラ登録数上限に達した場合など極めて稀。
        // ここでは起動直後に 1 度だけ呼ぶため、expect を使用する (library code ではなく bin edge)。
        let mut term = signal(SignalKind::terminate()).expect("failed to register SIGTERM handler");
        let mut int = signal(SignalKind::interrupt()).expect("failed to register SIGINT handler");

        tokio::select! {
            _ = term.recv() => info!("syncd: received SIGTERM"),
            _ = int.recv() => info!("syncd: received SIGINT"),
        }
    }
    #[cfg(not(unix))]
    {
        // Windows 向け: Ctrl-C のみ待機。
        tokio::signal::ctrl_c()
            .await
            .expect("failed to register Ctrl-C handler");
        info!("syncd: received Ctrl-C");
    }
}

/// PID file を管理する RAII ガード。
/// `acquire` でファイルを取得し、`Drop` で削除する。
pub struct PidFile {
    path: PathBuf,
}

impl PidFile {
    /// PID file を取得する。
    ///
    /// - 既存ファイルが存在しプロセスが生存中であれば `Err` を返す。
    /// - 既存ファイルが stale (プロセス不在) であれば警告ログを出して上書きする。
    /// - parent ディレクトリが存在しない場合は自動作成する。
    pub fn acquire(path: &Path, port: u16) -> anyhow::Result<Self> {
        if path.exists() {
            match std::fs::read_to_string(path) {
                Ok(contents) => {
                    if let Some(pid) = parse_pid(&contents) {
                        if process_alive(pid) {
                            anyhow::bail!(
                                "syncd is already running: pid={pid} ({}). \
                                 Stop the existing process before starting a new one.",
                                path.display()
                            );
                        }
                        warn!(pid, pid_file = %path.display(), "syncd: stale pid file detected, overwriting");
                    }
                }
                Err(e) => {
                    warn!(err = %e, pid_file = %path.display(), "syncd: failed to read pid file, overwriting");
                }
            }
        }

        // parent ディレクトリを作成 (~/.vdsl/ が初回存在しないケース対応)
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }

        let now = chrono::Utc::now().to_rfc3339();
        let contents = format!("{}\n{}\n{}\n", std::process::id(), now, port);
        std::fs::write(path, &contents)?;

        info!(
            pid = std::process::id(),
            pid_file = %path.display(),
            "syncd: pid file written"
        );

        Ok(Self {
            path: path.to_path_buf(),
        })
    }
}

impl Drop for PidFile {
    fn drop(&mut self) {
        if self.path.exists() {
            if let Err(e) = std::fs::remove_file(&self.path) {
                warn!(err = %e, pid_file = %self.path.display(), "syncd: failed to remove pid file");
            } else {
                info!(pid_file = %self.path.display(), "syncd: pid file removed");
            }
        }
    }
}

/// PID file の先頭行から PID を取り出す。
fn parse_pid(s: &str) -> Option<i32> {
    s.lines().next()?.trim().parse().ok()
}

/// プロセスが生存しているかを確認する。
///
/// Unix では `kill(pid, 0)` を使用する。
/// - `Ok(())` → プロセス存在
/// - `Err(ESRCH)` → プロセス不在 → stale とみなす
/// - `Err(EPERM)` → 他ユーザーのプロセス → 生存とみなす (安全側に倒す)
#[cfg(unix)]
fn process_alive(pid: i32) -> bool {
    use nix::errno::Errno;
    use nix::sys::signal::kill;
    use nix::unistd::Pid;

    match kill(Pid::from_raw(pid), None) {
        Ok(()) => true,
        Err(Errno::ESRCH) => false, // プロセス不在
        Err(_) => true,             // EPERM 等 → 生存とみなす
    }
}

#[cfg(not(unix))]
fn process_alive(_pid: i32) -> bool {
    // Windows では PID 存在確認の安全な方法がないため、常に生存とみなす。
    // Phase 1 は Mac のみが対象のため、この分岐は実運用では通らない。
    true
}

/// 子プロセスを分離して起動する (setsid orphan 化)。
///
/// mcp 側から syncd を fork/exec する際に使用する (Subtask 2 で呼び出し元を追加)。
/// stdin/stdout/stderr を `/dev/null` にリダイレクトし、setsid で新セッションを作成して
/// 親プロセス (mcp) の終了から独立させる。
///
/// # Safety
/// `pre_exec` は fork 後・exec 前に呼ばれるため、async-signal-safe な関数のみ使用可。
/// `setsid()` は POSIX で async-signal-safe として定義されている。
/// SDK ビルド時と同一のデフォルト exclude パターンを返す。
///
/// watcher 側でも `LocalScanner` と同じ exclude フィルタを適用するために使用する。
/// `SdkImplBuilder::exclude(...)` に設定したパターンと一致させること。
fn build_default_excludes() -> anyhow::Result<Vec<glob::Pattern>> {
    let patterns = [
        ".git",
        ".git/**",
        ".vdsl",
        ".vdsl/**",
        ".*",
        "**/.*",
        "**/*.partial",
    ];

    patterns
        .iter()
        .map(|p| {
            glob::Pattern::new(p).map_err(|e| anyhow::anyhow!("invalid exclude pattern {p:?}: {e}"))
        })
        .collect()
}

#[cfg(unix)]
pub fn spawn_detached(exe: &Path, args: &[&str], envs: &[(&str, &str)]) -> anyhow::Result<u32> {
    use std::os::unix::process::CommandExt;
    use std::process::{Command, Stdio};

    let mut cmd = Command::new(exe);
    cmd.args(args)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    for (k, v) in envs {
        cmd.env(k, v);
    }

    // SAFETY: `pre_exec` closure 内では async-signal-safe な setsid() のみを呼ぶ。
    let child = unsafe {
        cmd.pre_exec(|| {
            nix::unistd::setsid().map_err(std::io::Error::from)?;
            Ok(())
        })
        .spawn()?
    };

    Ok(child.id())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn tmp_pid_path(dir: &TempDir) -> PathBuf {
        dir.path().join("syncd.pid")
    }

    #[test]
    fn pid_file_acquire_creates_file() {
        let dir = TempDir::new().unwrap();
        let path = tmp_pid_path(&dir);

        let guard = PidFile::acquire(&path, 7823).expect("acquire should succeed");
        assert!(path.exists(), "pid file should exist after acquire");

        let contents = std::fs::read_to_string(&path).unwrap();
        let pid: u32 = contents.lines().next().unwrap().trim().parse().unwrap();
        assert_eq!(pid, std::process::id());

        drop(guard);
        assert!(!path.exists(), "pid file should be removed after drop");
    }

    #[test]
    fn pid_file_drop_removes_file() {
        let dir = TempDir::new().unwrap();
        let path = tmp_pid_path(&dir);

        {
            let _guard = PidFile::acquire(&path, 7823).unwrap();
            assert!(path.exists());
        }
        assert!(
            !path.exists(),
            "pid file should be gone after guard is dropped"
        );
    }

    #[test]
    fn pid_file_acquire_twice_returns_error() {
        let dir = TempDir::new().unwrap();
        let path = tmp_pid_path(&dir);

        let _guard = PidFile::acquire(&path, 7823).expect("first acquire should succeed");

        // 同じ path で 2 回目を試みる → 現プロセスが生存中なので Err になるはず
        let result = PidFile::acquire(&path, 7823);
        assert!(
            result.is_err(),
            "second acquire should fail while first is alive"
        );
    }

    #[test]
    fn pid_file_stale_overwrite() {
        let dir = TempDir::new().unwrap();
        let path = tmp_pid_path(&dir);

        // 存在しない PID (99999999) を書いておく → stale とみなして上書きされること
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(&path, "99999999\n2024-01-01T00:00:00Z\n7823\n").unwrap();

        // process_alive(99999999) が false を返すことを前提とする。
        // 万が一 PID 99999999 が実在する環境では本テストは skip せず失敗するが、
        // 通常環境では問題ない。
        let result = PidFile::acquire(&path, 7823);
        // stale なので成功するはず
        assert!(
            result.is_ok(),
            "acquire on stale pid file should succeed: {:?}",
            result.err()
        );
    }

    #[test]
    fn pid_file_creates_parent_dir() {
        let dir = TempDir::new().unwrap();
        // 存在しないサブディレクトリを指定
        let path = dir.path().join("nested").join("deep").join("syncd.pid");
        assert!(!path.parent().unwrap().exists());

        let _guard = PidFile::acquire(&path, 7823).expect("acquire should create parent dirs");
        assert!(path.exists());
    }

    #[test]
    fn parse_pid_valid() {
        assert_eq!(parse_pid("12345\n2024-01-01\n7823\n"), Some(12345));
    }

    #[test]
    fn parse_pid_invalid() {
        assert_eq!(parse_pid("not_a_number\n"), None);
    }

    #[test]
    fn parse_pid_empty() {
        assert_eq!(parse_pid(""), None);
    }
}
