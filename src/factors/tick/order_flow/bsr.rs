use polars::prelude::*;

use crate::factors::export::*;

/// Buy-Sell Ratio (BSR) factor.
///
/// This factor calculates the ratio of buy orders to sell orders over a specified window.
/// It provides insight into the buying and selling pressure in the market.
///
/// The BSR is calculated as buy_count / (buy_count + sell_count).
#[derive(FactorBase, Default, Clone)]
pub struct Bsr(pub Param);

impl PlFactor for Bsr {
    fn try_expr(&self) -> Result<Expr> {
        let n = self.0.as_usize();
        let is_buy = IS_BUY.expr();
        let buy_count = when(is_buy.clone())
            .then(1.lit())
            .otherwise(0.lit())
            .rolling_sum(RollingOptionsFixedWindow {
                window_size: n,
                min_periods: 1,
                ..Default::default()
            });
        let sell_count = when(is_buy.not())
            .then(1.lit())
            .otherwise(0.lit())
            .rolling_sum(RollingOptionsFixedWindow {
                window_size: n,
                min_periods: 1,
                ..Default::default()
            });
        let bsr = buy_count.clone().protect_div(buy_count + sell_count);
        Ok(bsr)
    }
}

#[ctor::ctor]
fn register() {
    register_pl_fac::<Bsr>().unwrap()
}
