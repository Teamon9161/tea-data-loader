use anyhow::Result;
use polars::lazy::dsl::Expr;

use crate::prelude::{FactorBase, Param, PlFactor};

#[derive(Default, Clone)]
pub struct Factor<F: FactorBase>(pub F);

impl<F: FactorBase> std::fmt::Debug for Factor<F> {
    #[inline]
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self.0)
    }
}

impl<F: FactorBase + Copy> Copy for Factor<F> {}

impl<F: FactorBase> From<F> for Factor<F> {
    #[inline]
    fn from(fac: F) -> Self {
        Factor(fac)
    }
}

unsafe impl<F: FactorBase + Send> Send for Factor<F> {}
unsafe impl<F: FactorBase + Sync> Sync for Factor<F> {}

impl<F: FactorBase + From<Param>> From<Param> for Factor<F> {
    #[inline]
    fn from(param: Param) -> Self {
        Factor(F::from(param))
    }
}

impl<F: FactorBase> FactorBase for Factor<F> {
    #[inline]
    fn fac_name() -> std::sync::Arc<str> {
        F::fac_name()
    }
}

impl<F: FactorBase + PlFactor + Send + Sync + 'static> PlFactor for Factor<F> {
    #[inline]
    fn try_expr(&self) -> Result<Expr> {
        self.0.try_expr()
    }
}
