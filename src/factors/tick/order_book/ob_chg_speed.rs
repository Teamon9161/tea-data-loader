use polars::prelude::*;

use crate::factors::export::*;

#[derive(FactorBase, Default, Clone)]
pub struct BuyObChgSpeed(pub Param);

impl PlFactor for BuyObChgSpeed {
    fn try_expr(&self) -> Result<Expr> {
        let p_diff = BID1.expr().diff(1, Default::default());
        let time_diff = crate::factors::base::TIME
            .expr()
            .diff(1, Default::default());
        Ok(p_diff.protect_div(time_diff.to_physical()))
    }
}

#[derive(FactorBase, Default, Clone)]
pub struct SellObChgSpeed(pub Param);

impl PlFactor for SellObChgSpeed {
    fn try_expr(&self) -> Result<Expr> {
        let p_diff = ASK1.expr().diff(1, Default::default());
        let time_diff = crate::factors::base::TIME
            .expr()
            .diff(1, Default::default());
        Ok(p_diff.protect_div(time_diff.to_physical()))
    }
}

#[ctor::ctor]
fn register() {
    register_pl_fac::<BuyObChgSpeed>().unwrap();
    register_pl_fac::<SellObChgSpeed>().unwrap();
}
