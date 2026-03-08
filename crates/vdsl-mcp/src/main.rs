#[tokio::main]
async fn main() -> anyhow::Result<()> {
    vdsl_mcp::interface::mcp::run().await
}
