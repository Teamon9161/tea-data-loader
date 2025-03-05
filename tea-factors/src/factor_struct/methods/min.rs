use std::sync::Arc;

use polars::prelude::RollingOptionsFixedWindow;

use crate::prelude::*;

/// Represents the rolling minimum of a factor.
#[derive(Clone, Copy)]
pub struct FactorMin<F: FactorBase> {
    pub(super) fac: F,
    pub(super) param: usize,
    pub(super) min_periods: Option<usize>,
}

impl<F: FactorBase> std::fmt::Debug for FactorMin<F> {
    #[inline]
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}_min_{:?}", self.fac.name(), self.param)
    }
}

impl<F: FactorBase> FactorBase for FactorMin<F> {
    #[inline]
    fn fac_name() -> Arc<str> {
        let f = F::fac_name();
        format!("{}_min", f).into()
    }

    fn new(_param: impl Into<Param>) -> Self {
        panic!("FactorMin::new should not be called directly")
    }
}

impl<F> PlFactor for FactorMin<F>
where
    F: FactorBase + PlFactor,
{
    #[inline]
    fn try_expr(&self) -> Result<Expr> {
        let expr = self.fac.try_expr()?;
        let n = self.param;
        if n == 1 {
            Ok(expr)
        } else {
            let min_expr = expr.rolling_min(RollingOptionsFixedWindow {
                window_size: n,
                min_periods: self.min_periods.unwrap_or(n / 2),
                ..Default::default()
            });
            Ok(min_expr)
        }
    }
}
