use polars::prelude::*;

use crate::factors::export::*;

fn get_ob_of_buy_sell() -> (impl FactorBase + PlFactor, impl FactorBase + PlFactor) {
    let of_buy = iif(BID1.gt(BID1.shift(1)), BID1_VOL, NONE);
    let of_buy = iif(BID1.eq(BID1.shift(1)), BID1_VOL - BID1_VOL.shift(1), of_buy);
    let of_buy = iif(BID1.lt(BID1.shift(1)), -BID1_VOL, of_buy);

    let of_sell = iif(ASK1.gt(ASK1.shift(1)), ASK1_VOL, NONE);
    let of_sell = iif(
        ASK1.eq(ASK1.shift(1)),
        ASK1_VOL - ASK1_VOL.shift(1),
        of_sell,
    );
    let of_sell = iif(ASK1.lt(ASK1.shift(1)), -ASK1_VOL, of_sell);
    (of_buy, of_sell)
}

/// Represents the Order Book Order Flow Imbalance (OB OFI) factor.
///
/// This factor calculates the imbalance between buy and sell order flow
/// over a specified number of periods.
///
/// The `usize` parameter determines the number of periods to consider
/// when calculating the OB OFI.
#[derive(FactorBase, FromParam, Default, Clone, Copy)]
pub struct ObOfi(pub usize);

impl PlFactor for ObOfi {
    fn try_expr(&self) -> Result<Expr> {
        let (of_buy, of_sell) = get_ob_of_buy_sell();
        let of_buy = of_buy.sum_opt(self.0, 1);
        let of_sell = of_sell.sum_opt(self.0, 1);
        of_buy.imb(of_sell).try_expr()
    }
}

#[derive(Default, FactorBase, Clone, Copy)]
pub struct AggObOfi;

impl PlAggFactor for AggObOfi {
    fn agg_expr(&self) -> Result<Expr> {
        let (of_buy, of_sell) = get_ob_of_buy_sell();
        let of_buy = of_buy.expr().sum();
        let of_sell = of_sell.expr().sum();
        Ok(of_buy.clone().protect_div(of_buy + of_sell))
    }
}

/// Represents the Cumulative Order Book Order Flow Imbalance (Cum OB OFI) factor.
///
/// This factor calculates the cumulative imbalance between buy and sell order flow
/// over a specified number of periods, and then applies a time series z-score normalization.
///
/// The `usize` parameter determines the number of periods to consider
/// when calculating the z-score of the cumulative OB OFI.
#[derive(FactorBase, FromParam, Default, Clone, Copy)]
pub struct CumObOfi(pub usize);

impl PlFactor for CumObOfi {
    fn try_expr(&self) -> Result<Expr> {
        let (of_buy, of_sell) = get_ob_of_buy_sell();
        let of_buy = of_buy.try_expr()?;
        let of_sell = of_sell.try_expr()?;
        Ok(of_buy
            .cum_sum(false)
            .forward_fill(None)
            .imbalance(of_sell.cum_sum(false).forward_fill(None))
            .ts_zscore(self.0, None)
            .over([col(&*TradingDate::fac_name())]))
    }
}

#[ctor::ctor]
fn register() {
    register_pl_fac::<ObOfi>().unwrap();
    register_pl_fac::<CumObOfi>().unwrap();
}
