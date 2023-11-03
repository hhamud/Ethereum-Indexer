use env_logger::Builder;
use ethers::{
    providers::{Provider, StreamExt, Ws},
    types::Address,
};
use eyre::Result;
use std::sync::Arc;

use crate::constants::URL;
use crate::database::DB;
use crate::types::pool_types::usdc_weth_pool::*;

/// Initializes the Ethereum event log indexer.
///
/// This function performs the following tasks:
///
/// 1. Sets up the logger with a specified log level.
/// 2. Establishes a WebSocket connection to an Ethereum node.
/// 3. Initializes a smart contract instance for event retrieval.
/// 4. Initializes a PostgreSQL database connection.
/// 5. Starts the event log decoding and indexing process.
///
/// # Returns
///
/// Returns a `Result` indicating the success or failure of the initialization.
pub async fn initialise() -> Result<()> {
    // Setup logger
    Builder::new()
        .filter_level(log::LevelFilter::Info)
        .parse_env("ETH_LOG")
        .init();

    // Setup WebSocket
    let address: Address = "0x88e6A0c2dDD26FEEb64F039a2c41296FcB3f5640".parse()?;
    let provider = Arc::new(Provider::<Ws>::connect(URL).await?);
    let contract = USDC_WETH_POOL::new(address, provider);

    // Initialize database connection
    let db = DB::new().await?;
    db.create_table().await?;

    // Run the event indexer
    decode_events(&contract, &db).await?;

    Ok(())
}

/// Decodes Ethereum event logs and indexes them in the database.
///
/// This function continuously listens for Ethereum event logs from a smart contract using a
/// WebSocket connection. It decodes various event types and inserts them into a PostgreSQL database.
///
/// # Arguments
///
/// - `contract`: A reference to the USDC_WETH_POOL smart contract instance.
/// - `db`: A reference to the PostgreSQL database instance.
///
/// # Returns
///
/// Returns a `Result` indicating the success or failure of the event decoding and indexing process.
async fn decode_events(contract: &USDC_WETH_POOL<Provider<Ws>>, db: &DB) -> Result<()> {
    // Retrieve the contract's events
    let events = contract.events();

    // Create a stream of events with metadata
    let mut stream = events.stream().await?.with_meta();

    while let Some(Ok((event, meta))) = stream.next().await {
        // Insert transaction logs into the database
        db.insert_transaction_logs(meta).await?;

        // Match and insert specific event types into the database
        match event {
            USDC_WETH_POOLEvents::SwapFilter(f) => db.insert_swap_event(f).await?,
            USDC_WETH_POOLEvents::BurnFilter(f) => db.insert_burn_event(f).await?,
            USDC_WETH_POOLEvents::MintFilter(f) => db.insert_mint_event(f).await?,
            USDC_WETH_POOLEvents::FlashFilter(f) => db.insert_flash_event(f).await?,
            _ => {}
        }
    }


    Ok(())
}

