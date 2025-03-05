use std::sync::Arc;

use crate::prelude::*;

/// Represents the exponential weighted moving average of a factor.
#[derive(Clone, Copy)]
pub struct FactorEwm<F: FactorBase> {
    pub(super) fac: F,
    pub(super) param: usize,
    pub(super) min_periods: Option<usize>,
}

impl<F: FactorBase> std::fmt::Debug for FactorEwm<F> {
    #[inline]
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}_ewm_{:?}", self.fac.name(), self.param)
    }
}

impl<F: FactorBase> FactorBase for FactorEwm<F> {
    #[inline]
    fn fac_name() -> Arc<str> {
        let f = F::fac_name();
        format!("{}_ewm", f).into()
    }

    fn new(_param: impl Into<Param>) -> Self {
        panic!("FactorEwm::new should not be called directly")
    }
}

impl<F> PlFactor for FactorEwm<F>
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
            Ok(expr.ts_ewm(n, self.min_periods))
        }
    }
}
