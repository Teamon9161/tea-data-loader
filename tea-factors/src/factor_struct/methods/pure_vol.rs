use std::sync::Arc;

use polars::prelude::RollingOptionsFixedWindow;

use crate::prelude::*;

/// Represents the pure volatility (standard deviation divided by mean) of a factor.
#[derive(Clone, Copy)]
pub struct FactorPureVol<F: FactorBase> {
    pub(super) fac: F,
    pub(super) param: usize,
    pub(super) min_periods: Option<usize>,
}

impl<F: FactorBase> std::fmt::Debug for FactorPureVol<F> {
    #[inline]
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}_pure_vol_{:?}", self.fac.name(), self.param)
    }
}

impl<F: FactorBase> FactorBase for FactorPureVol<F> {
    #[inline]
    fn fac_name() -> Arc<str> {
        let f = F::fac_name();
        format!("{}_pure_vol", f).into()
    }

    fn new(_param: impl Into<Param>) -> Self {
        panic!("FactorPureVol::new should not be called directly")
    }
}

impl<F> PlFactor for FactorPureVol<F>
where
    F: FactorBase + PlFactor,
{
    #[inline]
    fn try_expr(&self) -> Result<Expr> {
        let expr = self.fac.try_expr()?;
        let n = self.param;
        let opt = RollingOptionsFixedWindow {
            window_size: n,
            min_periods: self.min_periods.unwrap_or(n / 2),
            ..Default::default()
        };
        Ok(expr
            .clone()
            .rolling_std(opt.clone())
            .protect_div(expr.rolling_mean(opt)))
    }
}
