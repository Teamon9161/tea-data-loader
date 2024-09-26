use polars::prelude::*;

use crate::factors::export::*;

/// Represents the cumulative volume of bid orders up to a specified level in the order book.
///
/// This factor calculates the sum of bid volumes from the first level up to the level
/// specified by the `Param` value. For example, if `Param` is 3, it will sum the volumes
/// of the first three bid levels.
#[derive(FactorBase, Default, Clone)]
pub struct BidCumVol(pub Param);

impl PlFactor for BidCumVol {
    fn try_expr(&self) -> Result<Expr> {
        match self.0.as_u32() {
            0 => Ok(0.lit()),
            1 => Ok(BID1_VOL.expr()),
            2 => Ok(BID1_VOL.expr() + BID2_VOL.expr()),
            3 => Ok(BID1_VOL.expr() + BID2_VOL.expr() + BID3_VOL.expr()),
            4 => Ok(BID1_VOL.expr() + BID2_VOL.expr() + BID3_VOL.expr() + BID4_VOL.expr()),
            5 => Ok(BID1_VOL.expr()
                + BID2_VOL.expr()
                + BID3_VOL.expr()
                + BID4_VOL.expr()
                + BID5_VOL.expr()),
            _ => bail!("invalid param for bid_cum_vol: {}", self.0.as_u32()),
        }
    }
}

#[derive(FactorBase, Default, Clone)]
/// 注意尚未over trading_date进行分组
pub struct CumBidCumVol(pub Param);

impl PlFactor for CumBidCumVol {
    fn try_expr(&self) -> Result<Expr> {
        Ok(BidCumVol::new(self.0)
            .try_expr()?
            .cum_sum(false)
            .forward_fill(None))
    }
}

#[ctor::ctor]
fn register() {
    register_pl_fac::<BidCumVol>().unwrap();
    register_pl_fac::<CumBidCumVol>().unwrap();
}
