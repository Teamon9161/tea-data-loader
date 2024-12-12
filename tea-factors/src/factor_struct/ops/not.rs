use std::ops::Not;
use std::sync::Arc;

use anyhow::Result;
use polars::prelude::*;

use crate::prelude::{Factor, FactorBase, GetName, Param, PlFactor, TFactor};

/// Represents the bitwise NOT of a factor.
#[derive(Clone, Copy)]
pub struct FactorNot<F: FactorBase>(pub F);

pub type NotFactor<F> = Factor<FactorNot<F>>;

impl<F: FactorBase> Not for Factor<F> {
    type Output = NotFactor<F>;
    #[inline]
    fn not(self) -> Self::Output {
        Factor(FactorNot(self.0))
    }
}

impl<F> std::fmt::Debug for FactorNot<F>
where
    F: FactorBase,
{
    #[inline]
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "!{}", self.0.name())
    }
}

impl<F> From<Param> for FactorNot<F>
where
    F: FactorBase + From<Param>,
{
    #[inline]
    fn from(param: Param) -> Self {
        FactorNot(F::new(param))
    }
}

impl<F> FactorBase for FactorNot<F>
where
    F: FactorBase,
{
    #[inline]
    fn fac_name() -> Arc<str> {
        format!("!{}", F::fac_name()).into()
    }

    // #[inline]
    // fn new(param: impl Into<Param>) -> Self {
    //     let fac = F::new(param);
    //     FactorNot(fac)
    // }
}

impl<F> PlFactor for FactorNot<F>
where
    F: FactorBase + PlFactor,
{
    #[inline]
    fn try_expr(&self) -> Result<Expr> {
        Ok(self.0.try_expr()?.not())
    }
}

impl<F> TFactor for FactorNot<F>
where
    F: FactorBase + TFactor,
{
    #[inline]
    fn eval(&self, df: &DataFrame) -> Result<Series> {
        Ok(polars::prelude::negate_bitwise(&self.0.eval(df)?)?)
    }
}
