use polars::prelude::*;

use crate::factors::export::*;

/// Represents the mid-price factor in an order book.
///
/// The mid-price is calculated as the average of the best ask and best bid prices.
/// This factor is useful for providing a central reference point for the current market price.
///
/// # Fields
/// * `Param` - A parameter that can be used to customize the mid-price calculation if needed.
#[derive(FactorBase, Default, Debug, Clone)]
pub struct Mid(pub Param);

impl PlFactor for Mid {
    fn try_expr(&self) -> Result<Expr> {
        let mid = (ASK1.expr() + BID1.expr()) * 0.5.lit();
        Ok(mid)
    }
}

#[ctor::ctor]
fn register() {
    register_pl_fac::<Mid>().unwrap()
}
