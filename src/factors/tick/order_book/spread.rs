use polars::prelude::*;

use crate::factors::export::*;

#[derive(FactorBase, Default, Debug, Clone)]
pub struct Spread(pub Param);

impl PlFactor for Spread {
    fn try_expr(&self) -> Result<Expr> {
        ASK1.sub(BID1).try_expr()
    }
}

#[ctor::ctor]
fn register() {
    register_pl_fac::<Spread>().unwrap()
}
