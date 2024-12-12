use std::sync::Arc;

use crate::prelude::*;

/// Represents the difference of a factor.
#[derive(Clone, Copy)]
pub struct FactorDiff<F: FactorBase> {
    pub(super) fac: F,
    pub(super) param: i64,
}

impl<F: FactorBase> std::fmt::Debug for FactorDiff<F> {
    #[inline]
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}_diff_{:?}", self.fac.name(), self.param)
    }
}

impl<F: FactorBase> FactorBase for FactorDiff<F> {
    #[inline]
    fn fac_name() -> Arc<str> {
        let f = F::fac_name();
        format!("{}_diff", f).into()
    }

    fn new(_param: impl Into<Param>) -> Self {
        panic!("FactorDiff::new should not be called directly")
    }
}

impl<F> PlFactor for FactorDiff<F>
where
    F: FactorBase + PlFactor,
{
    #[inline]
    fn try_expr(&self) -> Result<Expr> {
        let expr = self.fac.try_expr()?;
        Ok(expr.diff(self.param, Default::default()))
    }
}
