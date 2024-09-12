use polars::prelude::*;

use crate::factors::export::*;

/// Represents the cumulative volume of bid orders up to a specified level in the order book.
///
/// This factor calculates the sum of bid volumes from the first level up to the level
/// specified by the `Param` value. For example, if `Param` is 3, it will sum the volumes
/// of the first three bid levels.
#[derive(FactorBase, Default, Debug, Clone)]
pub struct BidCumVol(pub Param);

impl PlFactor for BidCumVol {
    fn try_expr(&self) -> Result<Expr> {
        match self.0.as_u32() {
            1 => Ok(BID1VOL.expr()),
            2 => Ok(BID1VOL.expr() + BID2VOL.expr()),
            3 => Ok(BID1VOL.expr() + BID2VOL.expr() + BID3VOL.expr()),
            4 => Ok(BID1VOL.expr() + BID2VOL.expr() + BID3VOL.expr() + BID4VOL.expr()),
            5 => Ok(BID1VOL.expr()
                + BID2VOL.expr()
                + BID3VOL.expr()
                + BID4VOL.expr()
                + BID5VOL.expr()),
            _ => bail!("invalid param for bid_cum_vol: {}", self.0.as_u32()),
        }
    }
}

#[ctor::ctor]
fn register() {
    register_pl_fac::<BidCumVol>().unwrap()
}
