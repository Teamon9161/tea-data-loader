use polars::prelude::*;

use crate::factors::export::*;

/// Volume Weighted Average Price (VWAP) factor.
///
/// This factor calculates the VWAP over a specified number of periods.
/// VWAP is computed by dividing the sum of (price * volume) by the sum of volume
/// over the given time window.
///
/// The `Param` field represents the number of periods to consider for the VWAP calculation.
#[derive(FactorBase, FromParam, Default, Clone, Copy)]
pub struct Vwap(pub usize);

impl PlFactor for Vwap {
    fn try_expr(&self) -> Result<Expr> {
        let n = self.0;
        let vwap = (ORDER_PRICE * ORDER_AMT).sum_opt(n, 1) / (ORDER_AMT.sum_opt(n, 1));
        vwap.try_expr()
    }
}

#[ctor::ctor]
fn register() {
    register_pl_fac::<Vwap>().unwrap()
}
