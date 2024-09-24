use polars::prelude::*;

use crate::factors::export::*;

/// Represents the cumulative volume of ask orders up to a specified level in the order book.
///
/// This factor calculates the sum of ask volumes from the first level up to the level
/// specified by the `Param` value. For example, if `Param` is 3, it will sum the volumes
/// of the first three ask levels.
#[derive(FactorBase, Default, Clone)]
pub struct AskCumVol(pub Param);

impl PlFactor for AskCumVol {
    fn try_expr(&self) -> Result<Expr> {
        match self.0.as_u32() {
            1 => Ok(ASK1_VOL.expr()),
            2 => Ok(ASK1_VOL.expr() + ASK2_VOL.expr()),
            3 => Ok(ASK1_VOL.expr() + ASK2_VOL.expr() + ASK3_VOL.expr()),
            4 => Ok(ASK1_VOL.expr() + ASK2_VOL.expr() + ASK3_VOL.expr() + ASK4_VOL.expr()),
            5 => Ok(ASK1_VOL.expr()
                + ASK2_VOL.expr()
                + ASK3_VOL.expr()
                + ASK4_VOL.expr()
                + ASK5_VOL.expr()),
            _ => bail!("invalid param for ask_cum_vol: {}", self.0.as_u32()),
        }
    }
}

#[derive(FactorBase, Default, Clone)]
pub struct CumAskCumVol(pub Param);

impl PlFactor for CumAskCumVol {
    fn try_expr(&self) -> Result<Expr> {
        match self.0.as_u32() {
            1 => Ok(ASK1_VOL.expr().cum_sum(false)),
            2 => Ok(ASK1_VOL.expr().cum_sum(false) + ASK2_VOL.expr().cum_sum(false)),
            3 => Ok(ASK1_VOL.expr().cum_sum(false)
                + ASK2_VOL.expr().cum_sum(false)
                + ASK3_VOL.expr().cum_sum(false)),
            4 => Ok(ASK1_VOL.expr().cum_sum(false)
                + ASK2_VOL.expr().cum_sum(false)
                + ASK3_VOL.expr().cum_sum(false)
                + ASK4_VOL.expr().cum_sum(false)),
            5 => Ok(ASK1_VOL.expr().cum_sum(false)
                + ASK2_VOL.expr().cum_sum(false)
                + ASK3_VOL.expr().cum_sum(false)
                + ASK4_VOL.expr().cum_sum(false)
                + ASK5_VOL.expr().cum_sum(false)),
            _ => bail!("invalid param for ask_cum_vol: {}", self.0.as_u32()),
        }
    }
}

#[ctor::ctor]
fn register() {
    register_pl_fac::<AskCumVol>().unwrap();
    register_pl_fac::<CumAskCumVol>().unwrap();
}
