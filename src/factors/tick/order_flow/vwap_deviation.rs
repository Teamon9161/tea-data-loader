use polars::prelude::*;

use crate::factors::export::*;

/// Represents the deviation of the order price from the Volume Weighted Average Price (VWAP).
///
/// This factor calculates the relative difference between the current order price
/// and the VWAP, providing insight into how much the current price deviates from
/// the average trading price weighted by volume.
///
/// The deviation is calculated as (ORDER_PRICE - VWAP) / VWAP.
#[derive(FactorBase, FromParam, Default, Clone)]
pub struct VwapDeviation(pub usize);

impl PlFactor for VwapDeviation {
    fn try_expr(&self) -> Result<Expr> {
        let vwap = Vwap::fac(self.0);
        let fac = (ORDER_PRICE - vwap) / vwap * 10000;
        Ok(fac.try_expr()?)
    }
}

#[ctor::ctor]
fn register() {
    register_pl_fac::<VwapDeviation>().unwrap()
}
