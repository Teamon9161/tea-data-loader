use polars::prelude::*;

use crate::factors::export::*;

#[derive(FactorBase, FromParam, Default, Clone, Copy)]
pub struct BondFutureSpread;

impl PlFactor for BondFutureSpread {
    fn try_expr(&self) -> Result<Expr> {
        (MID_F - MID).try_expr()
    }
}

#[ctor::ctor]
fn register() {
    register_pl_fac::<BondFutureSpread>().unwrap()
}
