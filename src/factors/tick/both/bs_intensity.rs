use polars::prelude::*;

use crate::factors::export::*;

#[derive(FactorBase, FromParam, Default, Clone, Copy)]
pub struct BsIntensity(pub usize);

impl PlFactor for BsIntensity {
    fn try_expr(&self) -> Result<Expr> {
        let buy_vol = (ORDER_AMT * iif(IS_BUY, 1, 0)).sum_opt(self.0, 1);
        let sell_vol = (ORDER_AMT * iif(!IS_BUY, 1, 0)).sum_opt(self.0, 1);
        let q_buy = BID1_VOL.mean_opt(self.0, 1);
        let q_sell = ASK1_VOL.mean_opt(self.0, 1);
        let bs_intensity = buy_vol / q_buy - sell_vol / q_sell;
        bs_intensity.try_expr()
    }
}

#[ctor::ctor]
fn register() {
    register_pl_fac::<BsIntensity>().unwrap()
}
