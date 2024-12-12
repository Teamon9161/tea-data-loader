use std::sync::Arc;

use polars::prelude::RollingOptionsFixedWindow;

use crate::prelude::*;

/// Represents the signed efficiency ratio of a factor.
#[derive(Clone, Copy)]
pub struct FactorEfficiencySign<F: FactorBase> {
    pub(super) fac: F,
    pub(super) param: usize,
    pub(super) min_periods: Option<usize>,
}

impl<F: FactorBase> std::fmt::Debug for FactorEfficiencySign<F> {
    #[inline]
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}_efficiency_sign_{:?}", self.fac.name(), self.param)
    }
}

impl<F: FactorBase> FactorBase for FactorEfficiencySign<F> {
    #[inline]
    fn fac_name() -> Arc<str> {
        let f = F::fac_name();
        format!("{}_efficiency_sign", f).into()
    }

    fn new(_param: impl Into<Param>) -> Self {
        panic!("FactorEfficiencySign::new should not be called directly")
    }
}

impl<F> PlFactor for FactorEfficiencySign<F>
where
    F: FactorBase + PlFactor,
{
    #[inline]
    fn try_expr(&self) -> Result<Expr> {
        let expr = self.fac.try_expr()?;
        let n = self.param;
        let diff = expr.clone().diff(n as i64, Default::default());
        let route = expr
            .diff(1, Default::default())
            .abs()
            .rolling_sum(RollingOptionsFixedWindow {
                window_size: n,
                min_periods: self.min_periods.unwrap_or(n / 2),
                ..Default::default()
            });
        Ok(diff.protect_div(route))
    }
}
