use std::sync::Arc;

use polars::prelude::{Literal, RollingOptionsFixedWindow};

use crate::prelude::*;

/// Represents the bias of a factor relative to its rolling mean.
#[derive(Clone, Copy)]
pub struct FactorBias<F: FactorBase> {
    pub(super) fac: F,
    pub(super) param: usize,
    pub(super) min_periods: Option<usize>,
}

impl<F: FactorBase> std::fmt::Debug for FactorBias<F> {
    #[inline]
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}_bias_{:?}", self.fac.name(), self.param)
    }
}

impl<F: FactorBase> FactorBase for FactorBias<F> {
    #[inline]
    fn fac_name() -> Arc<str> {
        let f = F::fac_name();
        format!("{}_bias", f).into()
    }

    fn new(_param: impl Into<Param>) -> Self {
        panic!("FactorBias::new should not be called directly")
    }
}

impl<F> PlFactor for FactorBias<F>
where
    F: FactorBase + PlFactor,
{
    #[inline]
    fn try_expr(&self) -> Result<Expr> {
        let expr = self.fac.try_expr()?;
        let ma = expr.clone().rolling_mean(RollingOptionsFixedWindow {
            window_size: self.param,
            min_periods: self.min_periods.unwrap_or(self.param / 2),
            ..Default::default()
        });
        Ok(expr.protect_div(ma) - 1.lit())
    }
}
