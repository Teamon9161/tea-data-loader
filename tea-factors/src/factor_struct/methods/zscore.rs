use std::sync::Arc;

use polars::prelude::RollingOptionsFixedWindow;

use crate::prelude::*;

/// Represents the rolling z-score of a factor.
#[derive(Clone, Copy)]
pub struct FactorZscore<F: FactorBase> {
    pub(super) fac: F,
    pub(super) param: usize,
    pub(super) min_periods: Option<usize>,
}

impl<F: FactorBase> std::fmt::Debug for FactorZscore<F> {
    #[inline]
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}_zscore_{:?}", self.fac.name(), self.param)
    }
}

impl<F: FactorBase> FactorBase for FactorZscore<F> {
    #[inline]
    fn fac_name() -> Arc<str> {
        let f = F::fac_name();
        format!("{}_zscore", f).into()
    }

    fn new(_param: impl Into<Param>) -> Self {
        panic!("FactorZscore::new should not be called directly")
    }
}

impl<F> PlFactor for FactorZscore<F>
where
    F: FactorBase + PlFactor,
{
    #[inline]
    fn try_expr(&self) -> Result<Expr> {
        let expr = self.fac.try_expr()?;
        let n = self.param;
        let min_periods = self.min_periods.unwrap_or(n / 2);
        let opt = RollingOptionsFixedWindow {
            window_size: n,
            min_periods,
            ..Default::default()
        };
        let ma = expr.clone().rolling_mean(opt.clone());
        let vol = expr.clone().rolling_std(opt);
        Ok((expr - ma).protect_div(vol))
    }
}
