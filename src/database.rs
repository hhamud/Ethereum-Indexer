use ethers::prelude::LogMeta;
use eyre::Result;
use log::{error, info};
use tokio_postgres::{Client, Config, NoTls};

use crate::config::Settings;
use crate::types::pool_types::usdc_weth_pool::*;
use crate::types::{Address, Wu128, WI256, WU256};

/// Represents a PostgreSQL database client and provides methods for database operations.
#[derive(Debug)]
pub struct DB {
    /// The PostgreSQL database client.
    pub client: Client,
}

impl DB {
    /// Creates a new `DB` instance and establishes a connection to the PostgreSQL database.
    ///
    /// This function reads database connection settings from the configuration file and initializes
    /// the database client.
    ///
    /// # Returns
    ///
    /// Returns a `Result` indicating the success or failure of the database initialization.
    pub async fn new() -> Result<Self> {
        // Read database connection settings from the configuration file.
        let Settings {
            username,
            password,
            host,
            port,
            name,
        } = Settings::new().expect("failed to read config");


        //builder pattern
        // builder.config,
        // set the parts, .host,
        // test cases good for and different config
        // spawn bckground and tokio test

        // Configure the database connection.
        let mut config = Config::new();
        config
            .host(&host)
            .port(port)
            .user(&username)
            .password(&password)
            .dbname(&name);

        // Establish a connection to the database.
        let (client, connection) = config.connect(NoTls).await?;

        // Spawn a tokio task to handle potential connection errors.
        tokio::spawn(async move {
            if let Err(e) = connection.await {
                error!("Database connection error: {}", e);
            }
        });

        // Log a successful database connection.
        info!("Database connection established successfully.");

        Ok(Self { client })
    }

    /// Creates database tables if they do not already exist.
    ///
    /// This method creates tables for various event logs, such as Ethereum transaction logs, swap logs,
    /// burn logs, mint logs, and flash logs. If the tables already exist, this operation is a no-op.
    ///
    /// # Returns
    ///
    /// Returns a `Result` indicating the success or failure of the table creation.
    pub async fn create_table(&self) -> Result<()> {
        self.client
            .batch_execute(
                "
            CREATE TABLE IF NOT EXISTS ethereum_logs (
                id SERIAL PRIMARY KEY,
                transaction_hash BYTEA NOT NULL,
                block_number BYTEA NOT NULL,
                address BYTEA NOT NULL,
                timestamp TIMESTAMPTZ DEFAULT NOW() NOT NULL
            );

            CREATE TABLE IF NOT EXISTS swap_logs (
                id SERIAL PRIMARY KEY,
                ethereum_log_id INT REFERENCES ethereum_logs(id) ON DELETE CASCADE,
                sender_address BYTEA NOT NULL,
                receiver_address BYTEA NOT NULL,
                amount0 BYTEA NOT NULL,
                amount1 BYTEA NOT NULL,
                sqrt_price_x96 BYTEA NOT NULL,
                liquidity BYTEA NOT NULL,
                tick INT NOT NULL,
                timestamp TIMESTAMPTZ DEFAULT NOW() NOT NULL
            );

            CREATE TABLE IF NOT EXISTS burn_logs (
                id SERIAL PRIMARY KEY,
                ethereum_log_id INT REFERENCES ethereum_logs(id) ON DELETE CASCADE,
                owner_address BYTEA NOT NULL,
                tick_lower INT NOT NULL,
                tick_upper INT NOT NULL,
                amount BYTEA NOT NULL,
                amount0 BYTEA NOT NULL,
                amount1 BYTEA NOT NULL,
                timestamp TIMESTAMPTZ DEFAULT NOW() NOT NULL
            );

            CREATE TABLE IF NOT EXISTS mint_logs (
                id SERIAL PRIMARY KEY,
                ethereum_log_id INT REFERENCES ethereum_logs(id) ON DELETE CASCADE,
                sender_address BYTEA NOT NULL,
                owner_address BYTEA NOT NULL,
                tick_lower INT NOT NULL,
                tick_upper INT NOT NULL,
                amount BYTEA NOT NULL,
                amount0 BYTEA NOT NULL,
                amount1 BYTEA NOT NULL,
                timestamp TIMESTAMPTZ DEFAULT NOW() NOT NULL
            );

            CREATE TABLE IF NOT EXISTS flash_logs (
                id SERIAL PRIMARY KEY,
                ethereum_log_id INT REFERENCES ethereum_logs(id) ON DELETE CASCADE,
                sender_address BYTEA NOT NULL,
                receiver_address BYTEA NOT NULL,
                amount0 BYTEA NOT NULL,
                amount1 BYTEA NOT NULL,
                paid0 BYTEA NOT NULL,
                paid1 BYTEA NOT NULL,
                timestamp TIMESTAMPTZ DEFAULT NOW() NOT NULL
            );

            ",
            )
            .await?;

        // Log a message indicating successful table creation.
        info!("Tables created successfully");

        Ok(())
    }

    /// Inserts Ethereum transaction logs into the database.
    ///
    /// This method inserts transaction logs, including transaction hash, block number, and address,
    /// into the `ethereum_logs` table.
    ///
    /// # Arguments
    ///
    /// - `meta`: LogMeta containing Ethereum transaction log data.
    ///
    /// # Returns
    ///
    /// Returns a `Result` indicating the success or failure of the insertion.
    pub async fn insert_transaction_logs(&self, meta: LogMeta) -> Result<()> {
        let address: Address = meta.address.into();

        self.client
            .execute(
                "
               INSERT INTO ethereum_logs (
                   transaction_hash,
                   block_number,
                   address
               )
               VALUES ($1, $2, $3)
               ",
                &[
                    &meta.transaction_hash.as_bytes(),
                    &&meta.block_number.as_u64().to_be_bytes()[..],
                    &address,
                ],
            )
            .await?;

        // Log a message indicating the insertion of the transaction event.
        info!("Inserting transaction event: {:?}", meta);

        Ok(())
    }

    /// Inserts swap event logs into the database.
    ///
    /// This method inserts swap event logs, including sender address, receiver address, amounts, and other data,
    /// into the `swap_logs` table.
    ///
    /// # Arguments
    ///
    /// - `events`: SwapFilter containing swap event log data.
    ///
    /// # Returns
    ///
    /// Returns a `Result` indicating the success or failure of the insertion.
    pub async fn insert_swap_event(&self, events: SwapFilter) -> Result<()> {
        let sender: Address = events.sender.into();
        let rec: Address = events.recipient.into();
        let amount0: WI256 = events.amount_0.into();
        let amount1: WI256 = events.amount_1.into();
        let sqrt: WU256 = events.sqrt_price_x96.into();
        let liq: Wu128 = events.liquidity.into();

        self.client
            .execute(
                "
               INSERT INTO swap_logs (
                   sender_address,
                   receiver_address,
                   amount0,
                   amount1,
                   sqrt_price_x96,
                   liquidity,
                   tick
               )
               VALUES ($1, $2, $3, $4, $5, $6, $7)
               ",
                &[&sender, &rec, &amount0, &amount1, &sqrt, &liq, &events.tick],
            )
            .await?;

        // Log a message indicating the insertion of the Swap event.
        info!("Inserting Swap event: {:?}", events);

        Ok(())
    }
    // ...

    /// Inserts burn event logs into the database.
    ///
    /// This method inserts burn event logs, including owner address, tick boundaries, amounts, and other data,
    /// into the `burn_logs` table.
    ///
    /// # Arguments
    ///
    /// - `events`: BurnFilter containing burn event log data.
    ///
    /// # Returns
    ///
    /// Returns a `Result` indicating the success or failure of the insertion.
    pub async fn insert_burn_event(&self, events: BurnFilter) -> Result<()> {
        let owner: Address = events.owner.into();
        let amount: Wu128 = events.amount.into();
        let amount0: WU256 = events.amount_0.into();
        let amount1: WU256 = events.amount_1.into();

        self.client
            .execute(
                "
               INSERT INTO burn_logs (
                   owner_address,
                   tick_lower,
                   tick_upper,
                   amount,
                   amount0,
                   amount1
               )
               VALUES ($1, $2, $3, $4, $5, $6)
               ",
                &[
                    &owner,
                    &events.tick_lower,
                    &events.tick_upper,
                    &amount,
                    &amount0,
                    &amount1,
                ],
            )
            .await?;

        // Log a message indicating the insertion of the Burn event.
        info!("Inserting Burn event: {:?}", events);

        Ok(())
    }

    /// Inserts mint event logs into the database.
    ///
    /// This method inserts mint event logs, including sender address, owner address, tick boundaries,
    /// amounts, and other data, into the `mint_logs` table.
    ///
    /// # Arguments
    ///
    /// - `events`: MintFilter containing mint event log data.
    ///
    /// # Returns
    ///
    /// Returns a `Result` indicating the success or failure of the insertion.
    pub async fn insert_mint_event(&self, events: MintFilter) -> Result<()> {
        let sender: Address = events.sender.into();
        let owner: Address = events.owner.into();
        let amount: Wu128 = events.amount.into();
        let amount0: WU256 = events.amount_0.into();
        let amount1: WU256 = events.amount_1.into();
        self.client
            .execute(
                "
               INSERT INTO mint_logs (
                   sender_address,
                   owner_address,
                   tick_lower,
                   tick_upper,
                   amount,
                   amount0,
                   amount1
               )
               VALUES ($1, $2, $3, $4, $5, $6, $7)
               ",
                &[
                    &sender,
                    &owner,
                    &events.tick_lower,
                    &events.tick_upper,
                    &amount,
                    &amount0,
                    &amount1,
                ],
            )
            .await?;

        // Log a message indicating the insertion of the Mint event.
        info!("Inserting Mint event: {:?}", events);

        Ok(())
    }

    /// Inserts flash event logs into the database.
    ///
    /// This method inserts flash event logs, including sender address, receiver address, amounts paid,
    /// and other data, into the `flash_logs` table.
    ///
    /// # Arguments
    ///
    /// - `events`: FlashFilter containing flash event log data.
    ///
    /// # Returns
    ///
    /// Returns a `Result` indicating the success or failure of the insertion.
    pub async fn insert_flash_event(&self, events: FlashFilter) -> Result<()> {
        let sender: Address = events.sender.into();
        let rec: Address = events.recipient.into();
        let amount0: WU256 = events.amount_0.into();
        let amount1: WU256 = events.amount_1.into();
        let paid0: WU256 = events.amount_0.into();
        let paid1: WU256 = events.amount_1.into();

        self.client
            .execute(
                "
               INSERT INTO flash_logs (
                   sender_address,
                   receiver_address,
                   amount0,
                   amount1,
                   paid0,
                   paid1
               )
               VALUES ($1, $2, $3, $4, $5, $6)
               ",
                &[&sender, &rec, &amount0, &amount1, &paid0, &paid1],
            )
            .await?;

        // Log a message indicating the insertion of the Flash event.
        info!("Inserting Flash event: {:?}", events);

        Ok(())
    }
}
