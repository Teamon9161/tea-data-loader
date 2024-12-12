use polars::prelude::*;

use crate::export::*;

/// Represents the spread between the best ask and best bid prices in the order book.
///
/// The spread is calculated as the difference between the lowest ask price (ASK1)
/// and the highest bid price (BID1). It is a measure of market liquidity and
/// can indicate the cost of executing a round-trip trade.
#[derive(FactorBase, FromParam, Default, Clone, Copy)]
pub struct SpreadF;

impl PlFactor for SpreadF {
    fn try_expr(&self) -> Result<Expr> {
        (ASK1_F - BID1_F).try_expr()
    }
}

#[ctor::ctor]
fn register() {
    register_pl_fac::<SpreadF>().unwrap();
}
