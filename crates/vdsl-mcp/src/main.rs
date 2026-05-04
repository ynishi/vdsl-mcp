use clap::{Parser, Subcommand};
use std::path::PathBuf;
use tracing_appender::rolling;
use tracing_subscriber::{fmt, EnvFilter};
use vdsl_mcp::infra::config::{AppConfig, SyncdCliOverrides};

/// Load environment variables from .env files into process env.
///
/// Priority (highest first; first writer wins because dotenvy::from_path
/// does not override existing variables):
///   1. OS env (already set in the process)
///   2. `$VDSL_ENV_FILE` (explicit override path)
///   3. `~/.config/vdsl-mcp/.env` (XDG-ish user config)
///   4. `$CWD/.env` (project-local fallback)
///
/// Missing files are silently ignored. Parse errors are logged via stderr
/// because tracing is not yet initialised at this point.
fn load_env_files() {
    let mut candidates: Vec<PathBuf> = Vec::new();

    if let Ok(p) = std::env::var("VDSL_ENV_FILE") {
        candidates.push(PathBuf::from(p));
    }
    if let Some(home) = dirs::home_dir() {
        candidates.push(home.join(".config/vdsl-mcp/.env"));
    }
    if let Ok(cwd) = std::env::current_dir() {
        candidates.push(cwd.join(".env"));
    }

    for path in candidates {
        if !path.exists() {
            continue;
        }
        match dotenvy::from_path(&path) {
            Ok(()) => {
                eprintln!("vdsl-mcp: loaded env from {}", path.display());
            }
            Err(e) => {
                eprintln!("vdsl-mcp: failed to load env from {}: {e}", path.display());
            }
        }
    }
}

/// Initialize file-based tracing for MCP stdio server.
///
/// MCP stdio transport uses stdout for JSON-RPC protocol messages.
/// All application logging MUST go to a file, not stdout/stderr.
///
/// # Log directory resolution
///
/// `vdsl-mcp` is **mount-status agnostic**: it does not care whether the
/// configured `VDSL_LOG_DIR` lives on a mounted external volume. Instead,
/// it probes each candidate for write access and falls back through three
/// tiers so a stale `VDSL_LOG_DIR` (e.g. an unmounted external drive) never
/// panics the MCP server at startup.
///
/// Resolution order (first writable wins):
/// 1. **Primary** — `$VDSL_LOG_DIR` if set, otherwise `~/.vdsl/logs`.
///    Typical use case: User points this at an external volume
///    (`/Volumes/<drive>/vdsl/logs`) for bulk image-generation projects.
/// 2. **Local stable fallback** — `~/.vdsl/logs`. Persistent across reboots,
///    survives external volume unmounts. Used when the primary is configured
///    to an external location that is currently unavailable.
/// 3. **Last resort** — `/tmp/vdsl-mcp-logs`. Volatile (cleared on reboot).
///    Used only when even `~/.vdsl/logs` cannot be written (e.g. HOME
///    unreadable, restricted sandbox).
///
/// When tier 2 or tier 3 is selected, a `WARN`-level event is emitted *after*
/// tracing initialisation so the operator can grep the log for unexpected
/// fallbacks. Pre-init diagnostics also go to stderr.
///
/// Configuration:
/// - `VDSL_LOG_DIR`: Log directory (default: `~/.vdsl/logs`)
/// - `RUST_LOG`: Filter directives (overrides `default_level`)
/// - `default_level`: Used when `RUST_LOG` is not set (e.g. "info", "debug")
fn init_tracing(default_level: &str) -> tracing_appender::non_blocking::WorkerGuard {
    let home = std::env::var("HOME").unwrap_or_else(|_| "/tmp".into());
    let primary = std::env::var("VDSL_LOG_DIR").unwrap_or_else(|_| format!("{home}/.vdsl/logs"));
    let local_stable = format!("{home}/.vdsl/logs");
    let last_resort = "/tmp/vdsl-mcp-logs".to_string();

    // Probe each candidate for actual write access (existence is not enough
    // because an unmounted /Volumes path may pass create_dir_all on some
    // platforms but reject the subsequent file create).
    fn dir_writable(dir: &str) -> std::io::Result<()> {
        std::fs::create_dir_all(dir)?;
        let probe = std::path::Path::new(dir).join(".vdsl-write-probe");
        std::fs::write(&probe, b"ok")?;
        std::fs::remove_file(&probe).ok();
        Ok(())
    }

    // Track which tier was selected so we can emit a structured WARN after
    // tracing is up. We cannot log via tracing here because the subscriber
    // is not yet installed.
    let mut fallback_reason: Option<(String, String, String)> = None; // (from, to, error)

    let resolved_log_dir = match dir_writable(&primary) {
        Ok(()) => primary,
        Err(e1) => {
            eprintln!("vdsl-mcp: log dir {primary} not writable: {e1}; trying {local_stable}");
            if primary != local_stable {
                match dir_writable(&local_stable) {
                    Ok(()) => {
                        fallback_reason =
                            Some((primary.clone(), local_stable.clone(), e1.to_string()));
                        local_stable.clone()
                    }
                    Err(e2) => {
                        eprintln!(
                            "vdsl-mcp: local stable {local_stable} not writable: {e2}; \
                             falling back to {last_resort}"
                        );
                        let _ = dir_writable(&last_resort);
                        fallback_reason = Some((
                            primary.clone(),
                            last_resort.clone(),
                            format!("{e1}; then {e2}"),
                        ));
                        last_resort.clone()
                    }
                }
            } else {
                let _ = dir_writable(&last_resort);
                fallback_reason = Some((primary.clone(), last_resort.clone(), e1.to_string()));
                last_resort.clone()
            }
        }
    };

    let file_appender = rolling::daily(&resolved_log_dir, "vdsl-mcp.log");
    let (non_blocking, guard) = tracing_appender::non_blocking(file_appender);

    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| {
        EnvFilter::new(format!(
            "vdsl_mcp={default_level},vdsl_sync={default_level}"
        ))
    });

    fmt()
        .with_writer(non_blocking)
        .with_env_filter(filter)
        .with_ansi(false)
        .with_target(true)
        .with_thread_ids(false)
        .init();

    // Now that tracing is up, surface any fallback that occurred so it lands
    // in the rolling log file (operators can grep for "log_dir_fallback").
    if let Some((from, to, reason)) = fallback_reason {
        tracing::warn!(
            event = "log_dir_fallback",
            primary = %from,
            resolved = %to,
            reason = %reason,
            "VDSL_LOG_DIR primary was not writable; logs are being written to the fallback path"
        );
    } else {
        tracing::info!(log_dir = %resolved_log_dir, "tracing initialised");
    }

    guard
}

#[derive(Parser)]
#[command(name = "vdsl-mcp", version, about = "VDSL MCP server / sync daemon")]
struct Cli {
    /// Path to config file (default: ~/.vdsl/config.toml)
    #[arg(long, global = true)]
    config: Option<PathBuf>,

    #[command(subcommand)]
    command: Option<Command>,
}

#[derive(Subcommand)]
enum Command {
    /// Run as MCP server (stdio transport). This is the default when no subcommand is given.
    Mcp,
    /// Run as sync daemon (watcher + HTTP server).
    Syncd(SyncdArgs),
}

#[derive(clap::Args)]
struct SyncdArgs {
    /// HTTP listen port (default: 7823)
    #[arg(long)]
    port: Option<u16>,
    /// Working directory for sync operations
    #[arg(long)]
    work_dir: Option<PathBuf>,
    /// Path to PID file (default: ~/.vdsl/syncd.pid)
    #[arg(long)]
    pid_file: Option<PathBuf>,
    /// Debounce interval in milliseconds (default: 500)
    #[arg(long)]
    debounce_ms: Option<u64>,
    /// Log level (default: info)
    #[arg(long)]
    log_level: Option<String>,
    /// Internal flag: set when spawned by mcp process
    #[arg(long, hide = true)]
    spawned_by_mcp: bool,
}

impl From<&SyncdArgs> for SyncdCliOverrides {
    fn from(a: &SyncdArgs) -> Self {
        Self {
            port: a.port,
            work_dir: a.work_dir.clone(),
            pid_file: a.pid_file.clone(),
            debounce_ms: a.debounce_ms,
            log_level: a.log_level.clone(),
        }
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    load_env_files();

    let cli = Cli::parse();

    // config を先にロードして log_level を決定する (init_tracing はグローバル設定のため 1 度のみ呼ぶ)
    let cfg = AppConfig::load(cli.config.as_deref())?;

    let default_log_level = match &cli.command {
        Some(Command::Syncd(args)) => args
            .log_level
            .as_deref()
            .unwrap_or(&cfg.syncd.log_level)
            .to_string(),
        _ => cfg.syncd.log_level.clone(),
    };

    // _guard must live for the entire program lifetime.
    // Dropping it flushes pending log writes.
    let _guard = init_tracing(&default_log_level);

    tracing::info!("vdsl-mcp starting");

    match cli.command.unwrap_or(Command::Mcp) {
        Command::Mcp => vdsl_mcp::interface::mcp::run().await?,
        Command::Syncd(args) => {
            let spawned_by_mcp = args.spawned_by_mcp;
            let syncd_cfg = cfg.syncd.merge_cli(SyncdCliOverrides::from(&args));
            vdsl_mcp::interface::syncd::run(syncd_cfg, spawned_by_mcp).await?;
        }
    }

    Ok(())
}
