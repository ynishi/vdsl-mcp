use clap::{Parser, Subcommand};
use std::path::PathBuf;
use tracing_appender::rolling;
use tracing_subscriber::{fmt, EnvFilter};
use vdsl_mcp::infra::config::{AppConfig, SyncdCliOverrides};

/// Initialize file-based tracing for MCP stdio server.
///
/// MCP stdio transport uses stdout for JSON-RPC protocol messages.
/// All application logging MUST go to a file, not stdout/stderr.
///
/// Configuration:
/// - `VDSL_LOG_DIR`: Log directory (default: `~/.vdsl/logs`)
/// - `RUST_LOG`: Filter directives (overrides `default_level`)
/// - `default_level`: Used when `RUST_LOG` is not set (e.g. "info", "debug")
fn init_tracing(default_level: &str) -> tracing_appender::non_blocking::WorkerGuard {
    let log_dir = std::env::var("VDSL_LOG_DIR").unwrap_or_else(|_| {
        let home = std::env::var("HOME").unwrap_or_else(|_| "/tmp".into());
        format!("{home}/.vdsl/logs")
    });

    // Ensure log directory exists
    let _ = std::fs::create_dir_all(&log_dir);

    let file_appender = rolling::daily(&log_dir, "vdsl-mcp.log");
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
