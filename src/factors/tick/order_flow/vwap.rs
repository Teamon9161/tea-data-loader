use polars::prelude::*;

use crate::factors::export::*;

/// Volume Weighted Average Price (VWAP) factor.
///
/// This factor calculates the VWAP over a specified number of periods.
/// VWAP is computed by dividing the sum of (price * volume) by the sum of volume
/// over the given time window.
///
/// The `Param` field represents the number of periods to consider for the VWAP calculation.
#[derive(FactorBase, Default, Clone)]
pub struct Vwap(pub Param);

impl PlFactor for Vwap {
    fn try_expr(&self) -> Result<Expr> {
        let n = self.0.as_usize();
        let vwap = (ORDER_PRICE.expr() * ORDER_AMT.expr())
            .rolling_sum(RollingOptionsFixedWindow {
                window_size: n,
                min_periods: 1,
                ..Default::default()
            })
            .protect_div(ORDER_AMT.expr().rolling_sum(RollingOptionsFixedWindow {
                window_size: n,
                min_periods: 1,
                ..Default::default()
            }));
        Ok(vwap)
    }
}

#[ctor::ctor]
fn register() {
    register_pl_fac::<Vwap>().unwrap()
}
