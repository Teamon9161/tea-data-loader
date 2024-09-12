use polars::prelude::*;

use crate::factors::export::*;

/// Represents the slope of the order book at level 1.
///
/// This factor calculates the slope of the order book by comparing the volume
/// and price differences between bid and ask levels, focusing on the first level.
///
/// # Interpretation
/// - A larger buy-side slope indicates lower demand elasticity, suggesting buyers are
///   less sensitive to price changes. This implies a higher expected return for the stock.
/// - A smaller sell-side slope indicates higher supply elasticity, meaning a small price
///   decrease could lead to a significant reduction in sell orders. This suggests sellers
///   are reluctant to lower prices, also implying a higher expected return for the stock.
///
/// # Parameters
/// The `Param` field determines which level of the order book to use:
/// - If `None`, it defaults to level 1.
/// - If `Some(n)`, where n is 2 to 5, it uses the nth level of the order book.
///
/// # Formula
/// The slope is calculated as: (ask_slope + bid_slope)
/// Where:
/// - ask_slope = (ASKn - ASK1) / (AskCumVol(n) - ASK1VOL)
/// - bid_slope = (BIDn - BID1) / (BidCumVol(n) - BID1VOL)
///
/// Note: The bid slope is typically negative, so adding it to the ask slope
/// effectively subtracts its absolute value.
#[derive(FactorBase, Default, Debug, Clone)]
pub struct ObSlopeL1(pub Param);

impl PlFactor for ObSlopeL1 {
    fn try_expr(&self) -> Result<Expr> {
        let level = if self.0.is_none() {
            1
        } else {
            self.0.as_usize()
        };
        let ask_slope = match level {
            2 => ASK2.sub(ASK1).expr() / AskCumVol::new(2).sub(ASK1VOL).expr(),
            3 => ASK3.sub(ASK1).expr() / AskCumVol::new(3).sub(ASK1VOL).expr(),
            4 => ASK4.sub(ASK1).expr() / AskCumVol::new(4).sub(ASK1VOL).expr(),
            5 => ASK5.sub(ASK1).expr() / AskCumVol::new(5).sub(ASK1VOL).expr(),
            _ => bail!("level must be 2,3,4,5"),
        };
        let bid_slope = match level {
            2 => BID2.sub(BID1).expr() / BidCumVol::new(2).sub(BID1VOL).expr(),
            3 => BID3.sub(BID1).expr() / BidCumVol::new(3).sub(BID1VOL).expr(),
            4 => BID4.sub(BID1).expr() / BidCumVol::new(4).sub(BID1VOL).expr(),
            5 => BID5.sub(BID1).expr() / BidCumVol::new(5).sub(BID1VOL).expr(),
            _ => bail!("level must be 2,3,4,5"),
        };
        // 因为bid slope为负值，所以直接加上bid slope即可
        let expr = ask_slope + bid_slope;
        Ok(expr)
    }
}

#[ctor::ctor]
fn register() {
    register_pl_fac::<ObSlopeL1>().unwrap()
}
