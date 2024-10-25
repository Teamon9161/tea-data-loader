use polars::prelude::*;

use crate::factors::export::*;

/// Buy-Sell Intensity factor.
///
/// This factor calculates the imbalance between buy and sell intensities over a specified window.
/// Buy intensity is calculated as the ratio of buy volume to average ask volume at the best price.
/// Sell intensity is calculated as the ratio of sell volume to average bid volume at the best price.
///
/// The formula is: imbalance((buy_vol / avg_ask1_vol), (sell_vol / avg_bid1_vol))
///
/// A positive value indicates stronger buying pressure, while a negative value indicates stronger selling pressure.
#[derive(FactorBase, FromParam, Default, Clone, Copy)]
pub struct BsIntensity(pub usize);

impl PlFactor for BsIntensity {
    fn try_expr(&self) -> Result<Expr> {
        let buy_vol = (ORDER_VOL * iif(IS_BUY, 1, 0)).sum_opt(self.0, 1);
        let sell_vol = (ORDER_VOL * iif(!IS_BUY, 1, 0)).sum_opt(self.0, 1);
        let q_buy = BID1_VOL.mean_opt(self.0, 1);
        let q_sell = ASK1_VOL.mean_opt(self.0, 1);
        // let bs_intensity = buy_vol / q_sell - sell_vol / q_buy;
        let bs_intensity = (buy_vol / q_sell).imb(sell_vol / q_buy);
        bs_intensity.try_expr()
    }
}

#[derive(Default, FactorBase, Clone, Copy)]
pub struct AggBsIntensity;

impl PlAggFactor for AggBsIntensity {
    fn agg_expr(&self) -> Result<Expr> {
        let buy_vol = (ORDER_VOL * iif(IS_BUY, 1, 0)).expr().sum();
        let sell_vol = (ORDER_VOL * iif(!IS_BUY, 1, 0)).expr().sum();
        let q_buy = BID1_VOL.expr().mean();
        let q_sell = ASK1_VOL.expr().mean();
        // let bs_intensity = buy_vol.protect_div(q_sell) - sell_vol.protect_div(q_buy);
        let bs_intensity = buy_vol
            .protect_div(q_sell)
            .imbalance(sell_vol.protect_div(q_buy));
        Ok(bs_intensity)
    }
}

#[ctor::ctor]
fn register() {
    register_pl_fac::<BsIntensity>().unwrap()
}
