use anyhow::Result;
use pcli2_mcp::{run, setup_logging};

#[tokio::main]
async fn main() -> Result<()> {
    setup_logging(None);
    run().await
}
