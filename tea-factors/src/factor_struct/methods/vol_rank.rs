use std::sync::Arc;

use polars::prelude::RollingOptionsFixedWindow;

use crate::prelude::*;

/// Represents the rolling volatility rank of a factor.
#[derive(Clone, Copy)]
pub struct FactorVolRank<F: FactorBase> {
    pub(super) fac: F,
    pub(super) param: usize,
    pub(super) min_periods: Option<usize>,
}

impl<F: FactorBase> std::fmt::Debug for FactorVolRank<F> {
    #[inline]
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}_vol_rank_{:?}", self.fac.name(), self.param)
    }
}

impl<F: FactorBase> FactorBase for FactorVolRank<F> {
    #[inline]
    fn fac_name() -> Arc<str> {
        let f = F::fac_name();
        format!("{}_vol_rank", f).into()
    }

    fn new(_param: impl Into<Param>) -> Self {
        panic!("FactorVolRank::new should not be called directly")
    }
}

impl<F> PlFactor for FactorVolRank<F>
where
    F: FactorBase + PlFactor,
{
    #[inline]
    fn try_expr(&self) -> Result<Expr> {
        let expr = self.fac.try_expr()?;
        let n = self.param;
        let vol = expr.rolling_std(RollingOptionsFixedWindow {
            window_size: n,
            min_periods: self.min_periods.unwrap_or(n / 2),
            ..Default::default()
        });
        Ok(vol.ts_rank(5 * n, self.min_periods, true, false))
    }
}
