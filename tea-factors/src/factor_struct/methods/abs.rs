use std::sync::Arc;

use anyhow::Result;
use polars::prelude::*;

use crate::prelude::{FactorBase, GetName, Param, PlFactor, TFactor};

/// Represents the absolute value of a factor.
#[derive(Clone, Copy)]
pub struct FactorAbs<F: FactorBase>(pub F);

impl<F> std::fmt::Debug for FactorAbs<F>
where
    F: FactorBase,
{
    #[inline]
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "|{}|", self.0.name())
    }
}

impl<F> From<Param> for FactorAbs<F>
where
    F: FactorBase + From<Param>,
{
    #[inline]
    fn from(param: Param) -> Self {
        FactorAbs(F::new(param))
    }
}

impl<F> FactorBase for FactorAbs<F>
where
    F: FactorBase,
{
    #[inline]
    fn fac_name() -> Arc<str> {
        format!("|{}|", F::fac_name()).into()
    }
}

impl<F> PlFactor for FactorAbs<F>
where
    F: FactorBase + PlFactor,
{
    #[inline]
    fn try_expr(&self) -> Result<Expr> {
        Ok(self.0.try_expr()?.abs())
    }
}

impl<F> TFactor for FactorAbs<F>
where
    F: FactorBase + TFactor,
{
    #[inline]
    fn eval(&self, df: &DataFrame) -> Result<Series> {
        Ok(abs(&self.0.eval(df)?)?)
    }
}
