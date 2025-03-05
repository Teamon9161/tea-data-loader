use std::sync::Arc;

use polars::prelude::RollingOptionsFixedWindow;

use crate::prelude::*;

/// Represents the rolling maximum of a factor.
#[derive(Clone, Copy)]
pub struct FactorMax<F: FactorBase> {
    pub(super) fac: F,
    pub(super) param: usize,
    pub(super) min_periods: Option<usize>,
}

impl<F: FactorBase> std::fmt::Debug for FactorMax<F> {
    #[inline]
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}_max_{:?}", self.fac.name(), self.param)
    }
}

impl<F: FactorBase> FactorBase for FactorMax<F> {
    #[inline]
    fn fac_name() -> Arc<str> {
        let f = F::fac_name();
        format!("{}_max", f).into()
    }

    fn new(_param: impl Into<Param>) -> Self {
        panic!("FactorMax::new should not be called directly")
    }
}

impl<F> PlFactor for FactorMax<F>
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
            let max_expr = expr.rolling_max(RollingOptionsFixedWindow {
                window_size: n,
                min_periods: self.min_periods.unwrap_or(n / 2),
                ..Default::default()
            });
            Ok(max_expr)
        }
    }
}
