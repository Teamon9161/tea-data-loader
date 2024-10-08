use std::sync::Arc;

use crate::prelude::*;

/// Represents the rolling skewness of a factor.
#[derive(Clone, Copy)]
pub struct FactorKurt<F: FactorBase> {
    pub(super) fac: F,
    pub(super) param: usize,
    pub(super) min_periods: Option<usize>,
}

impl<F: FactorBase> std::fmt::Debug for FactorKurt<F> {
    #[inline]
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}_kurt_{:?}", self.fac.name(), self.param)
    }
}

impl<F: FactorBase> FactorBase for FactorKurt<F> {
    #[inline]
    fn fac_name() -> Arc<str> {
        let f = F::fac_name();
        format!("{}_kurt", f).into()
    }

    fn new(_param: impl Into<Param>) -> Self {
        panic!("FactorKurt::new should not be called directly")
    }
}

impl<F> PlFactor for FactorKurt<F>
where
    F: FactorBase + PlFactor,
{
    #[inline]
    fn try_expr(&self) -> Result<Expr> {
        let fac = self.fac.try_expr()?;
        let kurt = fac.ts_kurt(self.param, self.min_periods);
        Ok(kurt)
    }
}
