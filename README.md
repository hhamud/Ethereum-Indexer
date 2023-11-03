# Rust Ethereum Event Indexer

This Rust project is an Ethereum event indexer that listens to events from a specific Ethereum smart contract and stores them in a PostgreSQL database.

## Prerequisites
Before running this project, ensure you have the following prerequisites installed:

- Rust (https://www.rust-lang.org/learn/get-started)
- Docker (https://docs.docker.com/get-docker/)

## Installation

1. Install project dependencies:

   ```bash
   cargo build
   ```

## Configuration

The project uses a configuration file named `config.yaml` to configure the PostgreSQL database connection. 
You can edit this as so.

```yaml
username: your_database_username
password: your_database_password
host: your_database_host
port: your_database_port
name: your_database_name
```

## Running the Indexer

To run the Ethereum event indexer, use the following commands to run docker and the application:
```bash
docker-compose up -d
```

```bash
cargo run -- run
```

This command will initialize the logger, set up a WebSocket connection to Ethereum, create tables in the PostgreSQL database, and start listening for Ethereum events. The events are decoded and stored in the database.

## Generating Event Types

You can generate event types for your specific smart contract using the following command:

```bash
cargo run -- generate
```

This command will create Rust types for your Ethereum smart contract events and save them in the `pool_types.rs` file in the `types` directory.

## Testing

To run unit tests, use the following command:

```bash
cargo test
```
