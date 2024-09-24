use polars::prelude::*;

use crate::factors::export::*;

/// Represents the spread between the best ask and best bid prices in the order book.
///
/// The spread is calculated as the difference between the lowest ask price (ASK1)
/// and the highest bid price (BID1). It is a measure of market liquidity and
/// can indicate the cost of executing a round-trip trade.
///
/// # Fields
///
/// * `Param` - A parameter type that can be used to configure the factor if needed.
#[derive(FactorBase, Default, Clone)]
pub struct Spread(pub Param);

impl PlFactor for Spread {
    fn try_expr(&self) -> Result<Expr> {
        ASK1.sub(BID1).try_expr()
    }
}

/// Represents the spread between the yield to maturity (YTM) of the best bid and best ask prices in the order book.
///
/// The YTM spread is calculated as the difference between the highest bid YTM (BID1_YTM)
/// and the lowest ask YTM (ASK1_YTM). It is a measure of the yield differential between
/// the best bid and ask prices.
///
/// # Fields
///
/// * `Param` - A parameter type that can be used to configure the factor if needed.
#[derive(FactorBase, Default, Clone)]
pub struct YtmSpread(pub Param);

impl PlFactor for YtmSpread {
    fn try_expr(&self) -> Result<Expr> {
        BID1_YTM.sub(ASK1_YTM).try_expr()
    }
}

#[ctor::ctor]
fn register() {
    register_pl_fac::<Spread>().unwrap();
    register_pl_fac::<YtmSpread>().unwrap();
}
