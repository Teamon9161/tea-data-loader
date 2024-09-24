use polars::prelude::*;

// use super::Mid;
use crate::factors::export::*;

#[derive(FactorBase, Default, Clone)]
pub struct BondFutureSpread(pub Param);

impl PlFactor for BondFutureSpread {
    fn try_expr(&self) -> Result<Expr> {
        let bond_mid = MID.try_expr()?;
        let future_mid = col("mid_f");
        Ok(future_mid - bond_mid)
    }
}

#[ctor::ctor]
fn register() {
    register_pl_fac::<BondFutureSpread>().unwrap()
}
