use std::sync::Arc;

use anyhow::Result;
use polars::prelude::*;

use crate::prelude::{FactorBase, GetName, Param, PlFactor};

/// Represents the absolute value of a factor.
#[derive(Clone, Copy)]
pub struct FactorCumSum<F: FactorBase>(pub F);

impl<F> std::fmt::Debug for FactorCumSum<F>
where
    F: FactorBase,
{
    #[inline]
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}_cumsum", self.0.name())
    }
}

impl<F> From<Param> for FactorCumSum<F>
where
    F: FactorBase + From<Param>,
{
    #[inline]
    fn from(param: Param) -> Self {
        FactorCumSum(F::new(param))
    }
}

impl<F> FactorBase for FactorCumSum<F>
where
    F: FactorBase,
{
    #[inline]
    fn fac_name() -> Arc<str> {
        format!("|{}|", F::fac_name()).into()
    }
}

impl<F> PlFactor for FactorCumSum<F>
where
    F: FactorBase + PlFactor,
{
    #[inline]
    fn try_expr(&self) -> Result<Expr> {
        Ok(self.0.try_expr()?.cum_sum(false))
    }
}
