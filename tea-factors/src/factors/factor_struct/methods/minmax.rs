use std::sync::Arc;

use polars::prelude::RollingOptionsFixedWindow;

use crate::prelude::*;

/// Represents the rolling min-max normalization of a factor.
#[derive(Clone, Copy)]
pub struct FactorMinmax<F: FactorBase> {
    pub(super) fac: F,
    pub(super) param: usize,
    pub(super) min_periods: Option<usize>,
}

impl<F: FactorBase> std::fmt::Debug for FactorMinmax<F> {
    #[inline]
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}_minmax_{:?}", self.fac.name(), self.param)
    }
}

impl<F: FactorBase> FactorBase for FactorMinmax<F> {
    #[inline]
    fn fac_name() -> Arc<str> {
        let f = F::fac_name();
        format!("{}_minmax", f).into()
    }

    fn new(_param: impl Into<Param>) -> Self {
        panic!("FactorMinmax::new should not be called directly")
    }
}

impl<F> PlFactor for FactorMinmax<F>
where
    F: FactorBase + PlFactor,
{
    #[inline]
    fn try_expr(&self) -> Result<Expr> {
        let expr = self.fac.try_expr()?;
        let n = self.param;
        let rolling_opt = RollingOptionsFixedWindow {
            window_size: n,
            min_periods: self.min_periods.unwrap_or(n / 2),
            ..Default::default()
        };
        let min = expr.clone().rolling_min(rolling_opt.clone());
        let max = expr.clone().rolling_max(rolling_opt);
        let expr = (expr - min.clone()).protect_div(max - min);
        Ok(expr)
    }
}
