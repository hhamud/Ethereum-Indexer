use bytes::{BufMut, BytesMut};
use ethers::{
    prelude::Abigen,
    types::{H160, I256, U256},
};
use eyre::Result;
use postgres_types::{to_sql_checked, FromSql, IsNull, ToSql, Type};
use std::error::Error;
use std::ops::Deref;
use tokio_postgres::Row;

pub mod pool_types;
use crate::types::pool_types::usdc_weth_pool::*;


// use from instead of into

pub fn generate_types() -> Result<()> {
    let abi_source = "./abi/usdc_weth.abi";
    let out_file = "./src/types/pool_types.rs";

    Abigen::new("USDC_WETH_POOL", abi_source)?
        .generate()?
        .write_to_file(out_file)?;

    println!("Successfully created types file");

    Ok(())
}

#[derive(Debug)]
pub struct Address(H160);

impl Into<Address> for H160 {
    fn into(self) -> Address {
        Address(self)
    }
}

impl ToSql for Address {
    fn to_sql(
        &self,
        _ty: &Type,
        out: &mut BytesMut,
    ) -> Result<IsNull, Box<dyn Error + Sync + Send>> {
        out.put_slice(self.as_bytes());
        Ok(IsNull::No)
    }

    fn accepts(_ty: &Type) -> bool {
        true
    }

    to_sql_checked!();
}

impl<'a> FromSql<'a> for Address {
    fn from_sql(_ty: &Type, raw: &'a [u8]) -> Result<Self, Box<dyn Error + Sync + Send>> {
        Ok(Address(H160::from_slice(&raw)))
    }

    fn accepts(_ty: &Type) -> bool {
        true
    }
}

impl Deref for Address {
    type Target = H160;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

/// Wrapped type for I256
#[derive(Debug)]
pub struct WI256(I256);

impl Into<WI256> for I256 {
    fn into(self) -> WI256 {
        WI256(self)
    }
}

impl ToSql for WI256 {
    fn to_sql(
        &self,
        _ty: &Type,
        out: &mut BytesMut,
    ) -> Result<IsNull, Box<dyn Error + Sync + Send>> {
        let mut bytes = [0u8; 32];
        self.to_big_endian(&mut bytes);
        out.put_slice(&bytes);
        Ok(IsNull::No)
    }

    fn accepts(_ty: &Type) -> bool {
        true
    }

    to_sql_checked!();
}

impl<'a> FromSql<'a> for WI256 {
    fn from_sql(_ty: &Type, raw: &'a [u8]) -> Result<Self, Box<dyn Error + Sync + Send>> {
        Ok(WI256(I256::from_raw(U256::from_big_endian(raw))))
    }

    fn accepts(_ty: &Type) -> bool {
        true
    }
}

impl Deref for WI256 {
    type Target = I256;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

/// Wrapped type for U256
#[derive(Debug)]
pub struct WU256(U256);

impl Into<WU256> for U256 {
    fn into(self) -> WU256 {
        WU256(self)
    }
}

impl ToSql for WU256 {
    fn to_sql(
        &self,
        _ty: &Type,
        out: &mut BytesMut,
    ) -> Result<IsNull, Box<dyn Error + Sync + Send>> {
        let mut bytes = [0u8; 32];
        self.to_big_endian(&mut bytes);
        out.put_slice(&bytes);
        Ok(IsNull::No)
    }

    fn accepts(ty: &Type) -> bool {
        matches!(*ty, Type::BYTEA)
    }

    to_sql_checked!();
}

impl<'a> FromSql<'a> for WU256 {
    fn from_sql(_ty: &Type, raw: &'a [u8]) -> Result<Self, Box<dyn Error + Sync + Send>> {
        Ok(WU256(U256::from_big_endian(&raw)))
    }

    fn accepts(_ty: &Type) -> bool {
        true
    }
}

impl Deref for WU256 {
    type Target = U256;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

/// Wrapped type for U256
#[derive(Debug)]
pub struct Wu128(u128);

impl Into<Wu128> for u128 {
    fn into(self) -> Wu128 {
        Wu128(self)
    }
}

impl ToSql for Wu128 {
    fn to_sql(
        &self,
        _ty: &Type,
        out: &mut BytesMut,
    ) -> Result<IsNull, Box<dyn Error + Sync + Send>> {
        out.put_slice(&self.to_be_bytes());
        Ok(IsNull::No)
    }

    fn accepts(ty: &Type) -> bool {
        matches!(*ty, Type::BYTEA)
    }

    to_sql_checked!();
}

impl<'a> FromSql<'a> for Wu128 {
    fn from_sql(_ty: &Type, raw: &'a [u8]) -> Result<Self, Box<dyn Error + Sync + Send>> {
        let mut bytes: [u8; 16] = [0; 16];
        bytes.copy_from_slice(&raw[..16]);
        Ok(Wu128(u128::from_be_bytes(bytes)))
    }

    fn accepts(_ty: &Type) -> bool {
        true
    }
}

impl Deref for Wu128 {
    type Target = u128;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl From<Row> for SwapFilter {
    fn from(row: Row) -> Self {
        Self {
            sender: *row.get::<_, Address>("sender_address"),
            recipient: *row.get::<_, Address>("receiver_address"),
            amount_0: *row.get::<_, WI256>("amount0"),
            amount_1: *row.get::<_, WI256>("amount1"),
            sqrt_price_x96: *row.get::<_, WU256>("sqrt_price_x96"),
            liquidity: *row.get::<_, Wu128>("liquidity"),
            tick: row.get("tick"),
        }
    }
}

impl From<Row> for MintFilter {
    fn from(row: Row) -> Self {
        Self {
            sender: *row.get::<_, Address>("sender_address"),
            owner: *row.get::<_, Address>("owner_address"),
            tick_lower: row.get("tick_lower"),
            tick_upper: row.get("tick_upper"),
            amount: *row.get::<_, Wu128>("amount"),
            amount_0: *row.get::<_, WU256>("amount0"),
            amount_1: *row.get::<_, WU256>("amount1"),
        }
    }
}

impl From<Row> for BurnFilter {
    fn from(row: Row) -> Self {
        Self {
            owner: *row.get::<_, Address>("owner_address"),
            tick_lower: row.get("tick_lower"),
            tick_upper: row.get("tick_upper"),
            amount: *row.get::<_, Wu128>("amount"),
            amount_0: *row.get::<_, WU256>("amount0"),
            amount_1: *row.get::<_, WU256>("amount1"),
        }
    }
}

impl From<Row> for FlashFilter {
    fn from(row: Row) -> Self {
        Self {
            sender: *row.get::<_, Address>("sender_address"),
            recipient: *row.get::<_, Address>("receiver_address"),
            amount_0: *row.get::<_, WU256>("amount0"),
            amount_1: *row.get::<_, WU256>("amount1"),
            paid_0: *row.get::<_, WU256>("paid0"),
            paid_1: *row.get::<_, WU256>("paid1"),
        }
    }
}
