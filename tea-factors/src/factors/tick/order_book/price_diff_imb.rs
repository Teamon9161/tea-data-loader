use anyhow::ensure;
use polars::prelude::*;

use crate::factors::export::*;

/// Represents the Price Difference Imbalance factor.
///
/// This factor calculates the imbalance between the price differences on the ask and bid sides
/// of the order book at a specified level.
///
/// # Formula
/// The factor is calculated as:
/// ((Ask_n - Ask_1) - (Bid_n - Bid_1)) / ((Ask_n - Ask_1) + (Bid_n - Bid_1))
/// where n is the specified level.
///
/// # Parameters
/// * `usize` - The order book level to compare with the first level (must be greater than 1).
///
/// # Interpretation
/// - A positive value indicates a larger price difference on the ask side, suggesting potential upward price pressure.
/// - A negative value indicates a larger price difference on the bid side, suggesting potential downward price pressure.
/// - Values closer to zero suggest more balanced price differences between ask and bid sides.
#[derive(FactorBase, FromParam, Default, Clone, Copy)]
pub struct PriceDiffImb(pub usize);

impl PlFactor for PriceDiffImb {
    fn try_expr(&self) -> Result<Expr> {
        ensure!(self.0 > 1, "level must be greater than 1");
        let fac = (Ask::fac(self.0) - ASK1).imb(Bid::fac(1) - BID1);
        fac.try_expr()
    }
}

#[ctor::ctor]
fn register() {
    register_pl_fac::<PriceDiffImb>().unwrap();
}
