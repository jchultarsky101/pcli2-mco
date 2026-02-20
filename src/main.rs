use anyhow::Result;
use pcli2_mcp::run;

#[tokio::main]
async fn main() -> Result<()> {
    run().await
}
