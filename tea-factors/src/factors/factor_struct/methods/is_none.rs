use std::sync::Arc;

use anyhow::Result;
use polars::prelude::*;

use crate::prelude::{FactorBase, GetName, Param, PlFactor};

/// Represents the is_none of a factor.
#[derive(Clone, Copy)]
pub struct FactorIsNone<F: FactorBase>(pub F);

impl<F> std::fmt::Debug for FactorIsNone<F>
where
    F: FactorBase,
{
    #[inline]
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} = Null", self.0.name())
    }
}

impl<F> From<Param> for FactorIsNone<F>
where
    F: FactorBase + From<Param>,
{
    #[inline]
    fn from(param: Param) -> Self {
        FactorIsNone(F::new(param))
    }
}

impl<F> FactorBase for FactorIsNone<F>
where
    F: FactorBase,
{
    #[inline]
    fn fac_name() -> Arc<str> {
        format!("{} = Null", F::fac_name()).into()
    }
}

impl<F> PlFactor for FactorIsNone<F>
where
    F: FactorBase + PlFactor,
{
    #[inline]
    fn try_expr(&self) -> Result<Expr> {
        Ok(self.0.try_expr()?.is_null())
    }
}

/// Represents the is_not_none of a factor.
#[derive(Clone, Copy)]
pub struct FactorNotNone<F: FactorBase>(pub F);

impl<F> std::fmt::Debug for FactorNotNone<F>
where
    F: FactorBase,
{
    #[inline]
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} != Null", self.0.name())
    }
}

impl<F> From<Param> for FactorNotNone<F>
where
    F: FactorBase + From<Param>,
{
    #[inline]
    fn from(param: Param) -> Self {
        FactorNotNone(F::new(param))
    }
}

impl<F> FactorBase for FactorNotNone<F>
where
    F: FactorBase,
{
    #[inline]
    fn fac_name() -> Arc<str> {
        format!("{} != Null", F::fac_name()).into()
    }
}

impl<F> PlFactor for FactorNotNone<F>
where
    F: FactorBase + PlFactor,
{
    #[inline]
    fn try_expr(&self) -> Result<Expr> {
        Ok(self.0.try_expr()?.is_not_null())
    }
}
