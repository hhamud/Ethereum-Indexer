use clap::Parser;
use eyre::Result;

use crate::decode::initialise;
use crate::types::generate_types;

#[derive(Parser, Debug)]
pub enum EthLog {
    #[clap(about = "Start the indexing.")]
    Run,

    #[clap(about = "Generate types for the pool contract.")]
    Generate,
}

impl EthLog {
    pub async fn run(self) -> Result<()> {
        match self {
            Self::Run => Ok(initialise().await?),
            Self::Generate => Ok(generate_types()?),
        }
    }
}
