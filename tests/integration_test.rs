use eth_logs::{database::DB, types::pool_types::usdc_weth_pool::*};
use ethers::types::{Address, I256, U256};
use testcontainers::{clients, core::WaitFor, images::postgres::Postgres};
use tokio::test;

#[test]
async fn insert_events() {
    let docker = clients::Cli::default();

    let postgres_image = Postgres::default();

    let pg_container = docker.run(postgres_image);

    pg_container.start();

    WaitFor::seconds(60);

    // Get the PostgreSQL port
    let pg_port = pg_container.get_host_port_ipv4(5432);

    // Define the connection to the Postgress client
    let (client, connection) = tokio_postgres::Config::new()
        .user("postgres")
        .password("postgres")
        .host("localhost")
        .port(pg_port)
        .dbname("postgres")
        .connect(tokio_postgres::NoTls)
        .await
        .unwrap();

    // Spawn connection
    tokio::spawn(async move {
        if let Err(error) = connection.await {
            eprintln!("Connection error: {}", error);
        }
    });

    let db = DB { client };

    //setup database and create table
    let table = db.create_table().await;

    //test table creation is ok
    assert!(table.is_ok());

    //create events
    let s_events = SwapFilter {
        sender: Default::default(),
        recipient: Default::default(),
        amount_0: I256::zero(),
        amount_1: I256::zero(),
        sqrt_price_x96: U256::zero(),
        liquidity: Default::default(),
        tick: 0,
    };

    let m_events = MintFilter {
        sender: Default::default(),
        owner: Default::default(),
        tick_lower: 0,
        tick_upper: 0,
        amount: 0,
        amount_0: U256::MAX,
        amount_1: U256::MAX,
    };

    let b_events = BurnFilter {
        owner: Default::default(),
        tick_lower: 0,
        tick_upper: 0,
        amount: 0,
        amount_0: U256::MAX,
        amount_1: U256::MAX,
    };

    let f_events = FlashFilter {
        sender: Default::default(),
        recipient: Default::default(),
        amount_0: U256::MAX,
        amount_1: U256::MAX,
        paid_0: U256::MAX,
        paid_1: U256::MAX,
    };

    let s_result = db.insert_swap_event(s_events).await;
    let m_result = db.insert_mint_event(m_events).await;
    let b_result = db.insert_burn_event(b_events).await;
    let f_result = db.insert_flash_event(f_events).await;

    // check insertions of events
    assert!(s_result.is_ok());
    assert!(m_result.is_ok());
    assert!(b_result.is_ok());
    assert!(f_result.is_ok());

    // query the database
    let s_query = db
        .client
        .query("SELECT * FROM swap_logs", &[])
        .await
        .unwrap();

    let b_query = db
        .client
        .query("SELECT * FROM burn_logs", &[])
        .await
        .unwrap();

    let m_query = db
        .client
        .query("SELECT * FROM mint_logs", &[])
        .await
        .unwrap();

    let f_query = db
        .client
        .query("SELECT * FROM flash_logs", &[])
        .await
        .unwrap();

    //gather events into vec
    let swap_events: Vec<SwapFilter> = s_query
        .into_iter()
        .map(|row| SwapFilter::from(row))
        .collect();
    let burn_events: Vec<BurnFilter> = b_query
        .into_iter()
        .map(|row| BurnFilter::from(row))
        .collect();
    let mint_events: Vec<MintFilter> = m_query
        .into_iter()
        .map(|row| MintFilter::from(row))
        .collect();
    let flash_events: Vec<FlashFilter> = f_query
        .into_iter()
        .map(|row| FlashFilter::from(row))
        .collect();

    let se = swap_events.first().unwrap();
    let be = burn_events.first().unwrap();
    let me = mint_events.first().unwrap();
    let fe = flash_events.first().unwrap();

    let default_address: Address = "0x0000000000000000000000000000000000000000"
        .parse()
        .unwrap();

    //swap check
    assert_eq!(default_address, se.sender);
    assert_eq!(default_address, se.recipient);
    assert_eq!(I256::zero(), se.amount_0);
    assert_eq!(I256::zero(), se.amount_1);
    assert_eq!(U256::zero(), se.sqrt_price_x96);
    assert_eq!(0 as u128, se.liquidity);
    assert_eq!(0, se.tick);

    //burn check
    assert_eq!(default_address, be.owner);
    assert_eq!(0, be.tick_lower);
    assert_eq!(0, be.tick_upper);
    assert_eq!(0 as u128, be.amount);
    assert_eq!(U256::MAX, be.amount_0);
    assert_eq!(U256::MAX, be.amount_1);

    //mint check
    assert_eq!(default_address, me.sender);
    assert_eq!(default_address, me.owner);
    assert_eq!(0, me.tick_lower);
    assert_eq!(0, me.tick_upper);
    assert_eq!(0 as u128, me.amount);
    assert_eq!(U256::MAX, me.amount_0);
    assert_eq!(U256::MAX, me.amount_1);

    //flash check
    assert_eq!(default_address, fe.sender);
    assert_eq!(default_address, fe.recipient);
    assert_eq!(U256::MAX, fe.amount_0);
    assert_eq!(U256::MAX, fe.amount_1);
    assert_eq!(U256::MAX, fe.paid_0);
    assert_eq!(U256::MAX, fe.paid_1);
}
