use polars::prelude::*;

use crate::factors::export::*;

/// Represents the cumulative volume of ask orders up to a specified level in the order book.
///
/// This factor calculates the sum of ask volumes from the first level up to the level
/// specified by the `Param` value. For example, if `Param` is 3, it will sum the volumes
/// of the first three ask levels.
#[derive(FactorBase, Default, Debug, Clone)]
pub struct AskCumVol(pub Param);

impl PlFactor for AskCumVol {
    fn try_expr(&self) -> Result<Expr> {
        match self.0.as_u32() {
            1 => Ok(ASK1VOL.expr()),
            2 => Ok(ASK1VOL.expr() + ASK2VOL.expr()),
            3 => Ok(ASK1VOL.expr() + ASK2VOL.expr() + ASK3VOL.expr()),
            4 => Ok(ASK1VOL.expr() + ASK2VOL.expr() + ASK3VOL.expr() + ASK4VOL.expr()),
            5 => Ok(ASK1VOL.expr()
                + ASK2VOL.expr()
                + ASK3VOL.expr()
                + ASK4VOL.expr()
                + ASK5VOL.expr()),
            _ => bail!("invalid param for ask_cum_vol: {}", self.0.as_u32()),
        }
    }
}

#[ctor::ctor]
fn register() {
    register_pl_fac::<AskCumVol>().unwrap()
}
