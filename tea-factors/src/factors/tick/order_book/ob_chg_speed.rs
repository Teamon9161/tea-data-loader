use polars::prelude::*;

use crate::factors::base::TIME;
use crate::factors::export::*;

#[derive(FactorBase, FromParam, Default, Clone, Copy)]
pub struct BuyObChgSpeed;

const NANOS_PER_SEC: i64 = 1_000_000_000;

const SEC_PER_MIN: i64 = 60;

impl PlFactor for BuyObChgSpeed {
    fn try_expr(&self) -> Result<Expr> {
        let p_diff = BID1.diff(1);
        let time_diff = (TIME.diff(1).expr().to_physical() / NANOS_PER_SEC.lit()).fac();
        let fac = iif(
            time_diff.clone().lt_eq(30 * SEC_PER_MIN),
            p_diff / time_diff,
            NONE,
        );
        fac.try_expr()
    }
}

#[derive(FactorBase, FromParam, Default, Clone, Copy)]
pub struct SellObChgSpeed;

impl PlFactor for SellObChgSpeed {
    fn try_expr(&self) -> Result<Expr> {
        let p_diff = ASK1.diff(1);
        let time_diff = (TIME.diff(1).expr().to_physical() / NANOS_PER_SEC.lit()).fac();
        let fac = iif(
            time_diff.clone().lt_eq(30 * SEC_PER_MIN),
            p_diff / time_diff,
            NONE,
        );
        fac.try_expr()
    }
}

#[derive(FactorBase, FromParam, Default, Clone, Copy)]
pub struct ObChgSpeed;

impl PlFactor for ObChgSpeed {
    fn try_expr(&self) -> Result<Expr> {
        Ok(BuyObChgSpeed.expr() + SellObChgSpeed.expr())
    }
}

#[ctor::ctor]
fn register() {
    register_pl_fac::<BuyObChgSpeed>().unwrap();
    register_pl_fac::<SellObChgSpeed>().unwrap();
    register_pl_fac::<ObChgSpeed>().unwrap();
}
