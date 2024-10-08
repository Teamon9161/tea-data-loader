use polars::prelude::*;

use crate::factors::export::*;

/// Represents the spread between the best ask and best bid prices in the order book.
///
/// The spread is calculated as the difference between the lowest ask price (ASK1)
/// and the highest bid price (BID1). It is a measure of market liquidity and
/// can indicate the cost of executing a round-trip trade.
#[derive(FactorBase, FromParam, Default, Clone, Copy)]
pub struct Spread;

impl PlFactor for Spread {
    fn try_expr(&self) -> Result<Expr> {
        (ASK1 - BID1).try_expr()
    }
}

/// Represents the spread between the yield to maturity (YTM) of the best bid and best ask prices in the order book.
///
/// The YTM spread is calculated as the difference between the highest bid YTM (BID1_YTM)
/// and the lowest ask YTM (ASK1_YTM). It is a measure of the yield differential between
/// the best bid and ask prices.
#[derive(FactorBase, FromParam, Default, Clone, Copy)]
pub struct YtmSpread;

impl PlFactor for YtmSpread {
    fn try_expr(&self) -> Result<Expr> {
        (BID1_YTM - ASK1_YTM).try_expr()
    }
}

#[ctor::ctor]
fn register() {
    register_pl_fac::<Spread>().unwrap();
    register_pl_fac::<YtmSpread>().unwrap();
}
