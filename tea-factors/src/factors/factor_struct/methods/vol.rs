use std::sync::Arc;

use polars::prelude::RollingOptionsFixedWindow;

use crate::prelude::*;

/// Represents the rolling standard deviation (volatility) of a factor.
#[derive(Clone, Copy)]
pub struct FactorVol<F: FactorBase> {
    pub(super) fac: F,
    pub(super) param: usize,
    pub(super) min_periods: Option<usize>,
}

impl<F: FactorBase> std::fmt::Debug for FactorVol<F> {
    #[inline]
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}_vol_{:?}", self.fac.name(), self.param)
    }
}

impl<F: FactorBase> FactorBase for FactorVol<F> {
    #[inline]
    fn fac_name() -> Arc<str> {
        let f = F::fac_name();
        format!("{}_vol", f).into()
    }

    fn new(_param: impl Into<Param>) -> Self {
        panic!("FactorVol::new should not be called directly")
    }
}

impl<F> PlFactor for FactorVol<F>
where
    F: FactorBase + PlFactor,
{
    #[inline]
    fn try_expr(&self) -> Result<Expr> {
        let expr = self.fac.try_expr()?;
        let n = self.param;
        let min_periods = self.min_periods.unwrap_or(n / 2);
        Ok(expr.rolling_std(RollingOptionsFixedWindow {
            window_size: n,
            min_periods,
            ..Default::default()
        }))
    }
}
