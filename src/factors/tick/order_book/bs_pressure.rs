use polars::prelude::*;

use crate::factors::export::*;

/// Represents the buy-sell pressure in the order book.
///
/// This factor calculates the logarithmic difference between the ask pressure and bid pressure.
/// The pressure for each side is computed using a weighted sum of volumes at different price levels,
/// where the weights are inversely proportional to the distance from the mid price.
#[derive(FactorBase, FromParam, Default, Clone, Copy)]
pub struct BsPressure;

impl PlFactor for BsPressure {
    fn try_expr(&self) -> Result<Expr> {
        let bid_weight1 = MID / (MID - BID1);
        let bid_weight2 = MID / (MID - BID2);
        let bid_weight3 = MID / (MID - BID3);
        let bid_weight4 = MID / (MID - BID4);
        let bid_weight5 = MID / (MID - BID5);
        let bid_denom = bid_weight1 + bid_weight2 + bid_weight3 + bid_weight4 + bid_weight5;
        let bid_pressure = bid_weight1 / bid_denom * BID1_VOL
            + bid_weight2 / bid_denom * BID2_VOL
            + bid_weight3 / bid_denom * BID3_VOL
            + bid_weight4 / bid_denom * BID4_VOL
            + bid_weight5 / bid_denom * BID5_VOL;

        let ask_weight1 = MID / (ASK1 - MID);
        let ask_weight2 = MID / (ASK2 - MID);
        let ask_weight3 = MID / (ASK3 - MID);
        let ask_weight4 = MID / (ASK4 - MID);
        let ask_weight5 = MID / (ASK5 - MID);
        let ask_denom = ask_weight1 + ask_weight2 + ask_weight3 + ask_weight4 + ask_weight5;
        let ask_pressure = ask_weight1 / ask_denom * ASK1_VOL
            + ask_weight2 / ask_denom * ASK2_VOL
            + ask_weight3 / ask_denom * ASK3_VOL
            + ask_weight4 / ask_denom * ASK4_VOL
            + ask_weight5 / ask_denom * ASK5_VOL;
        (ask_pressure.ln() - bid_pressure.ln()).try_expr()
    }
}

#[ctor::ctor]
fn register() {
    register_pl_fac::<BsPressure>().unwrap()
}
