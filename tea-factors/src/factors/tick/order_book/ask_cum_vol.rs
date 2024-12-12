use polars::prelude::*;

use crate::factors::export::*;

/// Represents the cumulative volume of ask orders up to a specified level in the order book.
///
/// This factor calculates the sum of ask volumes from the first level up to the level
/// specified by the wrapped `usize` value.
#[derive(FactorBase, FromParam, Default, Clone, Copy)]
pub struct AskCumVol(pub usize);

impl PlFactor for AskCumVol {
    fn try_expr(&self) -> Result<Expr> {
        match self.0 {
            0 => Ok(0.lit()),
            1 => ASK1_VOL.try_expr(),
            2 => crate::hsum!(ASK1_VOL, ASK2_VOL).try_expr(),
            3 => crate::hsum!(ASK1_VOL, ASK2_VOL, ASK3_VOL).try_expr(),
            4 => crate::hsum!(ASK1_VOL, ASK2_VOL, ASK3_VOL, ASK4_VOL).try_expr(),
            5 => crate::hsum!(ASK1_VOL, ASK2_VOL, ASK3_VOL, ASK4_VOL, ASK5_VOL).try_expr(),
            6 => {
                crate::hsum!(ASK1_VOL, ASK2_VOL, ASK3_VOL, ASK4_VOL, ASK5_VOL, ASK6_VOL).try_expr()
            },
            7 => crate::hsum!(ASK1_VOL, ASK2_VOL, ASK3_VOL, ASK4_VOL, ASK5_VOL, ASK6_VOL, ASK7_VOL)
                .try_expr(),
            8 => crate::hsum!(
                ASK1_VOL, ASK2_VOL, ASK3_VOL, ASK4_VOL, ASK5_VOL, ASK6_VOL, ASK7_VOL, ASK8_VOL
            )
            .try_expr(),
            9 => crate::hsum!(
                ASK1_VOL, ASK2_VOL, ASK3_VOL, ASK4_VOL, ASK5_VOL, ASK6_VOL, ASK7_VOL, ASK8_VOL,
                ASK9_VOL
            )
            .try_expr(),
            10 => crate::hsum!(
                ASK1_VOL, ASK2_VOL, ASK3_VOL, ASK4_VOL, ASK5_VOL, ASK6_VOL, ASK7_VOL, ASK8_VOL,
                ASK9_VOL, ASK10_VOL
            )
            .try_expr(),
            p => bail!("invalid param for ask_cum_vol: {}", p),
        }
    }
}

#[derive(FactorBase, FromParam, Default, Clone, Copy)]
/// 注意尚未over trading_date进行分组
pub struct CumAskCumVol(pub usize);

impl PlFactor for CumAskCumVol {
    fn try_expr(&self) -> Result<Expr> {
        Ok(AskCumVol::new(self.0)
            .try_expr()?
            .cum_sum(false)
            .forward_fill(None))
    }
}

#[ctor::ctor]
fn register() {
    register_pl_fac::<AskCumVol>().unwrap();
    register_pl_fac::<CumAskCumVol>().unwrap();
}
