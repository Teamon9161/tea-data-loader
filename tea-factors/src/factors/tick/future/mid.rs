use polars::prelude::*;

use crate::factors::export::*;

/// Represents the mid-price factor in an order book.
///
/// The mid-price is calculated as the average of the best ask and best bid prices.
/// This factor is useful for providing a central reference point for the current market price.
#[derive(FactorBase, FromParam, Default, Clone, Copy)]
pub struct MidF;

impl PlFactor for MidF {
    fn try_expr(&self) -> Result<Expr> {
        let f = (ASK1_F + BID1_F) * 0.5;
        f.try_expr()
    }
}

#[ctor::ctor]
fn register() {
    register_pl_fac::<MidF>().unwrap();
}
