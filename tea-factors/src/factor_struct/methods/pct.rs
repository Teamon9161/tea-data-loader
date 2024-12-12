use std::sync::Arc;

use polars::prelude::Literal;

use crate::prelude::*;

/// Represents the percentage change of a factor.
#[derive(Clone, Copy)]
pub struct FactorPct<F: FactorBase> {
    pub(super) fac: F,
    pub(super) param: i64,
}

impl<F: FactorBase> std::fmt::Debug for FactorPct<F> {
    #[inline]
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}_pct_{:?}", self.fac.name(), self.param)
    }
}

impl<F: FactorBase> FactorBase for FactorPct<F> {
    #[inline]
    fn fac_name() -> Arc<str> {
        let f = F::fac_name();
        format!("{}_pct", f).into()
    }

    fn new(_param: impl Into<Param>) -> Self {
        panic!("FactorPct::new should not be called directly")
    }
}

impl<F> PlFactor for FactorPct<F>
where
    F: FactorBase + PlFactor,
{
    #[inline]
    fn try_expr(&self) -> Result<Expr> {
        Ok(self.fac.try_expr()?.pct_change(self.param.lit()))
    }
}
