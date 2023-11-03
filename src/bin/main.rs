use clap::Parser;
use eth_logs::command::EthLog;
use eyre::Result;

#[tokio::main]
async fn main() -> Result<()> {
    EthLog::parse().run().await?;

    Ok(())
}
