use polars::prelude::*;

use crate::factors::export::*;

/// Represents the slope of the order book.
///
/// This factor calculates the slope of the order book by comparing the volume
/// and price differences between bid and ask levels.
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
/// - If `None`, it defaults to level 5.
/// - If `Some(n)`, where n is 1 to 5, it uses the nth level of the order book.
///
/// # Formula
/// The slope is calculated as: (ask_slope + bid_slope)
/// Where:
/// - ask_slope = (ASKn - MID) / AskCumVol(n)
/// - bid_slope = (BIDn - MID) / BidCumVol(n)
///
/// Note: The bid slope is typically negative, so adding it to the ask slope
/// effectively subtracts its absolute value.
#[derive(FactorBase, FromParam, Default, Clone, Copy)]
pub struct ObSlopeF(pub Option<usize>);

impl PlFactor for ObSlopeF {
    fn try_expr(&self) -> Result<Expr> {
        let level = self.0.unwrap_or(5);
        let ask_slope = (AskF::fac(level) - MID_F) / AskCumVolF::new(level);
        let bid_slope = (BidF::fac(level) - MID_F) / BidCumVolF::new(level);
        // 因为bid slope为负值，所以直接加上bid slope即可
        let expr = ask_slope + bid_slope;
        // 避免量纲过小
        (expr * 1e9).try_expr()
    }
}

const SLOPE_FINE_PARAM: f64 = 2. / 3.;

/// Represents a refined version of the order book slope calculation.
///
/// This factor calculates a more detailed slope of the order book by considering
/// multiple levels and using a weighted approach.
///
/// # Parameters
/// The `Param` field determines the maximum level of the order book to use:
/// - If `None`, it defaults to level 5.
/// - If `Some(n)`, where n is 1 to 5, it uses levels 1 to n of the order book.
///
/// # Formula
/// The slope is calculated using a weighted sum approach across multiple levels,
/// considering both ask and bid sides. The exact formula is more complex than
/// the basic `ObSlope` and involves cumulative volumes at each level.
#[derive(FactorBase, FromParam, Default, Clone, Copy)]
pub struct ObSlopeFineF(pub Option<usize>);

impl PlFactor for ObSlopeFineF {
    fn try_expr(&self) -> Result<Expr> {
        let max_level = self.0.unwrap_or(5);
        let ask_slope = SLOPE_FINE_PARAM.lit()
            * (1..=max_level)
                .map(|level| {
                    let vi = AskCumVolF::fac(level);
                    let vi_1 = AskCumVolF::fac(level - 1);
                    ((AskF::fac(level) - MID_F) * (vi.pow(2) - vi_1.pow(2))).expr()
                })
                .reduce(|a, b| a + b)
                .unwrap()
                .protect_div(
                    (1..=max_level)
                        .map(|level| {
                            let vi = AskCumVolF::fac(level).expr();
                            let vi_1 = AskCumVolF::fac(level - 1).expr();
                            vi.clone().pow(3) - vi_1.clone().pow(3)
                        })
                        .reduce(|a, b| a + b)
                        .unwrap(),
                );
        let bid_slope = SLOPE_FINE_PARAM.lit()
            * (1..=max_level)
                .map(|level| {
                    let vi = BidCumVolF::new(level).expr();
                    let vi_1 = BidCumVolF::fac(level - 1).expr();
                    (BidF::fac(level) - MID_F).expr() * (vi.pow(2) - vi_1.pow(2))
                })
                .reduce(|a, b| a + b)
                .unwrap()
                .protect_div(
                    (1..=max_level)
                        .map(|level| {
                            let vi = BidCumVolF::new(level).expr();
                            let vi_1 = BidCumVolF::new(level - 1).expr();
                            vi.clone().pow(3) - vi_1.clone().pow(3)
                        })
                        .reduce(|a, b| a + b)
                        .unwrap(),
                );
        // 因为bid slope为负值，所以直接加上bid slope即可
        let expr = ask_slope + bid_slope;
        // 避免量纲过小
        Ok(expr * 1e9.lit())
    }
}

#[ctor::ctor]
fn register() {
    register_pl_fac::<ObSlopeF>().unwrap();
    register_pl_fac::<ObSlopeFineF>().unwrap();
}
