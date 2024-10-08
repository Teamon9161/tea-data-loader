use std::sync::Arc;

use polars::prelude::Literal;

use crate::prelude::*;

/// Represents the shifted version of a factor.
#[derive(Clone, Copy)]
pub struct FactorShift<F: FactorBase> {
    pub(super) fac: F,
    pub(super) param: i64,
}

impl<F: FactorBase> std::fmt::Debug for FactorShift<F> {
    #[inline]
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}_shift_{:?}", self.fac.name(), self.param)
    }
}

impl<F: FactorBase> FactorBase for FactorShift<F> {
    #[inline]
    fn fac_name() -> Arc<str> {
        let f = F::fac_name();
        format!("{}_shift", f).into()
    }

    fn new(_param: impl Into<Param>) -> Self {
        panic!("FactorShift::new should not be called directly")
    }
}

impl<F> PlFactor for FactorShift<F>
where
    F: FactorBase + PlFactor,
{
    #[inline]
    fn try_expr(&self) -> Result<Expr> {
        let expr = self.fac.try_expr()?;
        Ok(expr.shift(self.param.lit()))
    }
}
