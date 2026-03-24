use tracing_appender::rolling;
use tracing_subscriber::{fmt, EnvFilter};

/// Initialize file-based tracing for MCP stdio server.
///
/// MCP stdio transport uses stdout for JSON-RPC protocol messages.
/// All application logging MUST go to a file, not stdout/stderr.
///
/// Configuration:
/// - `VDSL_LOG_DIR`: Log directory (default: `~/.vdsl/logs`)
/// - `RUST_LOG`: Filter directives (default: `vdsl_mcp=info,vdsl_sync=info`)
fn init_tracing() -> tracing_appender::non_blocking::WorkerGuard {
    let log_dir = std::env::var("VDSL_LOG_DIR").unwrap_or_else(|_| {
        let home = std::env::var("HOME").unwrap_or_else(|_| "/tmp".into());
        format!("{home}/.vdsl/logs")
    });

    // Ensure log directory exists
    let _ = std::fs::create_dir_all(&log_dir);

    let file_appender = rolling::daily(&log_dir, "vdsl-mcp.log");
    let (non_blocking, guard) = tracing_appender::non_blocking(file_appender);

    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("vdsl_mcp=info,vdsl_sync=info"));

    fmt()
        .with_writer(non_blocking)
        .with_env_filter(filter)
        .with_ansi(false)
        .with_target(true)
        .with_thread_ids(false)
        .init();

    guard
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // _guard must live for the entire program lifetime.
    // Dropping it flushes pending log writes.
    let _guard = init_tracing();

    tracing::info!("vdsl-mcp starting");

    vdsl_mcp::interface::mcp::run().await
}
