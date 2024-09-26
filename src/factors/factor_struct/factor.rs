use anyhow::Result;
use polars::lazy::dsl::Expr;

use crate::prelude::{FactorBase, Param, PlFactor};

#[derive(Debug, Default, Clone, Copy)]
pub struct Factor<F: FactorBase> {
    pub param: Param,
    fac: std::marker::PhantomData<F>,
}

unsafe impl<F: FactorBase + Send> Send for Factor<F> {}
unsafe impl<F: FactorBase + Sync> Sync for Factor<F> {}

impl<F: FactorBase> FactorBase for Factor<F> {
    #[inline]
    fn fac_name() -> std::sync::Arc<str> {
        F::fac_name()
    }

    #[inline]
    fn new(param: impl Into<Param>) -> Self {
        Factor {
            param: param.into(),
            fac: std::marker::PhantomData,
        }
    }
}

impl<F: FactorBase + PlFactor + Send + Sync + 'static> PlFactor for Factor<F> {
    fn try_expr(&self) -> Result<Expr> {
        let fac = F::new(self.param);
        fac.try_expr()
    }
}
