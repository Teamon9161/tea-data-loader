use polars::prelude::*;

use crate::factors::export::*;

/// Buy-Sell Ratio (BSR) factor.
///
/// This factor calculates the ratio of buy orders to sell orders over a specified window.
/// It provides insight into the buying and selling pressure in the market.
///
/// The BSR is calculated as buy_count / (buy_count + sell_count).
#[derive(FactorBase, FromParam, Default, Clone)]
pub struct Bsr(pub usize);

impl PlFactor for Bsr {
    fn try_expr(&self) -> Result<Expr> {
        let n = self.0;
        let buy_count = iif(IS_BUY, 1, 0).sum_opt(n, 1);
        let sell_count = iif(!IS_BUY, 1, 0).sum_opt(n, 1);
        let bsr = buy_count / (buy_count + sell_count);
        bsr.try_expr()
    }
}

#[ctor::ctor]
fn register() {
    register_pl_fac::<Bsr>().unwrap()
}
