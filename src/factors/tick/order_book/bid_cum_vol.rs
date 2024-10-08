use polars::prelude::*;

use crate::factors::export::*;

/// Represents the cumulative volume of bid orders up to a specified level in the order book.
///
/// This factor calculates the sum of bid volumes from the first level up to the level
/// specified by the wrapped `usize` value. For example, if the value is 3, it will sum the volumes
/// of the first three bid levels.
#[derive(FactorBase, FromParam, Default, Clone, Copy)]
pub struct BidCumVol(pub usize);

impl PlFactor for BidCumVol {
    fn try_expr(&self) -> Result<Expr> {
        match self.0 {
            0 => Ok(0.lit()),
            1 => BID1_VOL.try_expr(),
            2 => (BID1_VOL + BID2_VOL).try_expr(),
            3 => (BID1_VOL + BID2_VOL + BID3_VOL).try_expr(),
            4 => (BID1_VOL + BID2_VOL + BID3_VOL + BID4_VOL).try_expr(),
            5 => (BID1_VOL + BID2_VOL + BID3_VOL + BID4_VOL + BID5_VOL).try_expr(),
            p => bail!("invalid param for bid_cum_vol: {}", p),
        }
    }
}

#[derive(FactorBase, FromParam, Default, Clone, Copy)]
/// 注意尚未over trading_date进行分组
pub struct CumBidCumVol(pub usize);

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
