use anyhow::ensure;
use polars::prelude::*;

use crate::factors::export::*;

fn get_ob_of_buy_sell() -> (Expr, Expr) {
    let a1 = ASK1.expr();
    let a1_shift = a1.clone().shift(1.lit());
    let b1 = BID1.expr();
    let b1_shift = b1.clone().shift(1.lit());
    let mut of_buy = when(b1.clone().gt(b1_shift.clone()))
        .then(BID1_VOL.expr())
        .otherwise(NULL.lit());
    of_buy = when(b1.clone().eq(b1_shift.clone()))
        .then(BID1_VOL.expr() - BID1_VOL.expr().shift(1.lit()))
        .otherwise(of_buy);
    of_buy = when(b1.lt(b1_shift))
        .then(-BID1_VOL.expr())
        .otherwise(of_buy);
    let mut of_sell = when(a1.clone().gt(a1_shift.clone()))
        .then(ASK1_VOL.expr())
        .otherwise(NULL.lit());
    of_sell = when(a1.clone().eq(a1_shift.clone()))
        .then(ASK1_VOL.expr() - ASK1_VOL.expr().shift(1.lit()))
        .otherwise(of_sell);
    of_sell = when(a1.lt(a1_shift))
        .then(-ASK1_VOL.expr())
        .otherwise(of_sell);
    (of_buy, of_sell)
}

#[derive(FactorBase, Default, Clone)]
pub struct ObOfi(pub Param);

impl PlFactor for ObOfi {
    fn try_expr(&self) -> Result<Expr> {
        let (of_buy, of_sell) = get_ob_of_buy_sell();
        let n = self.0.as_usize();
        let of_buy = of_buy.rolling_sum(RollingOptionsFixedWindow {
            window_size: n,
            min_periods: 1,
            ..Default::default()
        });
        let of_sell = of_sell.rolling_sum(RollingOptionsFixedWindow {
            window_size: n,
            min_periods: 1,
            ..Default::default()
        });
        Ok(of_buy.imbalance(of_sell))
    }
}

#[derive(FactorBase, Default, Clone)]
pub struct CumObOfi(pub Param);

impl PlFactor for CumObOfi {
    fn try_expr(&self) -> Result<Expr> {
        let (of_buy, of_sell) = get_ob_of_buy_sell();
        Ok(of_buy.cum_sum(false).imbalance(of_sell.cum_sum(false)))
    }
}

#[ctor::ctor]
fn register() {
    register_pl_fac::<ObOfi>().unwrap();
    register_pl_fac::<CumObOfi>().unwrap();
}
