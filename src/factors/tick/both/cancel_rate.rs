use polars::prelude::*;

use crate::factors::export::*;

#[derive(FactorBase, FromParam, Default, Clone, Copy)]
pub struct CancelRate(pub usize);

impl PlFactor for CancelRate {
    fn try_expr(&self) -> Result<Expr> {
        let ask_vol = AskCumVol::fac(5);
        let bid_vol = BidCumVol::fac(5);
        let ask_trade_vol = iif(IS_BUY, ORDER_VOL, 0.);
        let bid_trade_vol = iif(!IS_BUY, ORDER_VOL, 0.);
        let ask_vol_add = ask_vol.diff(1) + ask_trade_vol;
        let bid_vol_add = bid_vol.diff(1) + bid_trade_vol;
        let ask_cancel_rate = iif(ask_vol_add.lt(0), ask_vol_add.abs(), 0).sum_opt(self.0, 1)
            / ask_vol.sum_opt(self.0, 1);
        let bid_cancel_rate = iif(bid_vol_add.lt(0), bid_vol_add.abs(), 0).sum_opt(self.0, 1)
            / bid_vol.sum_opt(self.0, 1);
        let cancel_rate = ask_cancel_rate - bid_cancel_rate;
        cancel_rate.try_expr()
    }
}

#[ctor::ctor]
fn register() {
    register_pl_fac::<CancelRate>().unwrap()
}
