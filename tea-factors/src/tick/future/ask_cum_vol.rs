use polars::prelude::*;

use crate::export::*;

/// Represents the cumulative volume of ask orders up to a specified level in the order book.
///
/// This factor calculates the sum of ask volumes from the first level up to the level
/// specified by the wrapped `usize` value.
#[derive(FactorBase, FromParam, Default, Clone, Copy)]
pub struct AskCumVolF(pub usize);

impl PlFactor for AskCumVolF {
    fn try_expr(&self) -> Result<Expr> {
        match self.0 {
            0 => Ok(0.lit()),
            1 => ASK1_VOL_F.try_expr(),
            2 => (ASK1_VOL_F + ASK2_VOL_F).try_expr(),
            3 => (ASK1_VOL_F + ASK2_VOL_F + ASK3_VOL_F).try_expr(),
            4 => (ASK1_VOL_F + ASK2_VOL_F + ASK3_VOL_F + ASK4_VOL_F).try_expr(),
            5 => (ASK1_VOL_F + ASK2_VOL_F + ASK3_VOL_F + ASK4_VOL_F + ASK5_VOL_F).try_expr(),
            p => bail!("invalid param for ask_cum_vol: {}", p),
        }
    }
}

#[derive(FactorBase, FromParam, Default, Clone)]
/// 注意尚未over trading_date进行分组
pub struct CumAskCumVolF(pub usize);

impl PlFactor for CumAskCumVolF {
    fn try_expr(&self) -> Result<Expr> {
        Ok(AskCumVolF::new(self.0)
            .try_expr()?
            .cum_sum(false)
            .forward_fill(None))
    }
}

#[ctor::ctor]
fn register() {
    register_pl_fac::<AskCumVolF>().unwrap();
    register_pl_fac::<CumAskCumVolF>().unwrap();
}
