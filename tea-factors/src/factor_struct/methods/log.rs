use std::sync::Arc;

use crate::prelude::*;

/// Represents the logarithm of a factor.
#[derive(Clone, Copy)]
pub struct FactorLog<F: FactorBase> {
    pub(super) fac: F,
    pub(super) base: f64,
}

impl<F: FactorBase> std::fmt::Debug for FactorLog<F> {
    #[inline]
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.base != f64::EPSILON {
            write!(f, "log({}, {:?})", self.fac.name(), self.base)
        } else {
            write!(f, "ln({})", self.fac.name())
        }
    }
}

impl<F: FactorBase> FactorBase for FactorLog<F> {
    #[inline]
    fn fac_name() -> Arc<str> {
        let f = F::fac_name();
        format!("log({})", f).into()
    }

    fn new(_param: impl Into<Param>) -> Self {
        panic!("FactorLog::new should not be called directly")
    }
}

impl<F> PlFactor for FactorLog<F>
where
    F: FactorBase + PlFactor,
{
    #[inline]
    fn try_expr(&self) -> Result<Expr> {
        let expr = self.fac.try_expr()?;
        Ok(expr.log(self.base))
    }
}
