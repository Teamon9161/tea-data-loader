use polars::prelude::*;

use crate::factors::export::*;

/// Represents the slope of the order book.
///
/// This factor calculates the slope of the order book by comparing the volume
/// and price differences between bid and ask levels.
///
/// # Interpretation
/// - A larger buy-side slope (in absolute value) indicates lower buying pressure. This could be due to
///   buyers placing orders at lower prices or having smaller order volumes, suggesting a potential
///   downward price movement.
/// - A larger sell-side slope indicates higher selling pressure. This implies sellers are placing
///   orders at higher prices or with larger volumes, also suggesting a potential downward price movement.
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
/// Note: The bid slope is typically negative. A larger absolute value of the total slope
/// generally indicates stronger downward price pressure.
#[derive(FactorBase, FromParam, Default, Clone, Copy)]
pub struct ObSlope(pub Option<usize>);

impl PlFactor for ObSlope {
    fn try_expr(&self) -> Result<Expr> {
        let level = self.0.unwrap_or(5);
        let ask_slope = (Ask::fac(level) - MID) / AskCumVol::new(level);
        let bid_slope = (Bid::fac(level) - MID) / (BidCumVol::new(level));
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
pub struct ObSlopeFine(pub Option<usize>);

impl PlFactor for ObSlopeFine {
    fn try_expr(&self) -> Result<Expr> {
        let max_level = self.0.unwrap_or(5);
        let ask_slope = SLOPE_FINE_PARAM.lit()
            * (1..=max_level)
                .map(|level| {
                    let vi = AskCumVol::fac(level);
                    let vi_1 = AskCumVol::fac(level - 1);
                    ((Ask::fac(level) - MID) * (vi.pow(2) - vi_1.pow(2))).expr()
                })
                .reduce(|a, b| a + b)
                .unwrap()
                .protect_div(
                    (1..=max_level)
                        .map(|level| {
                            let vi = AskCumVol::new(level).expr();
                            let vi_1 = AskCumVol::new(level - 1).expr();
                            vi.clone().pow(3) - vi_1.clone().pow(3)
                        })
                        .reduce(|a, b| a + b)
                        .unwrap(),
                );
        let bid_slope = SLOPE_FINE_PARAM.lit()
            * (1..=max_level)
                .map(|level| {
                    let vi = BidCumVol::new(level).expr();
                    let vi_1 = BidCumVol::new(level - 1).expr();
                    (Bid::fac(level) - MID).expr() * (vi.pow(2) - vi_1.pow(2))
                })
                .reduce(|a, b| a + b)
                .unwrap()
                .protect_div(
                    (1..=max_level)
                        .map(|level| {
                            let vi = BidCumVol::new(level).expr();
                            let vi_1 = BidCumVol::new(level - 1).expr();
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
    register_pl_fac::<ObSlope>().unwrap();
    register_pl_fac::<ObSlopeFine>().unwrap();
}
