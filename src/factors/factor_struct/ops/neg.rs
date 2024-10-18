use std::ops::Neg;
use std::sync::Arc;

use anyhow::Result;
use polars::prelude::*;

use crate::prelude::{Factor, FactorBase, GetName, Param, PlFactor, TFactor};

/// Represents the negative of a factor.
#[derive(Clone, Copy)]
pub struct FactorNeg<F: FactorBase>(pub F);

pub type NegFactor<F> = Factor<FactorNeg<F>>;

impl<F: FactorBase> Neg for Factor<F> {
    type Output = NegFactor<F>;
    #[inline]
    fn neg(self) -> Self::Output {
        Factor(FactorNeg(self.0))
    }
}

impl<F> std::fmt::Debug for FactorNeg<F>
where
    F: FactorBase,
{
    #[inline]
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "-{}", self.0.name())
    }
}

impl<F> From<Param> for FactorNeg<F>
where
    F: FactorBase + From<Param>,
{
    #[inline]
    fn from(param: Param) -> Self {
        FactorNeg(F::new(param))
    }
}

impl<F> FactorBase for FactorNeg<F>
where
    F: FactorBase,
{
    #[inline]
    fn fac_name() -> Arc<str> {
        format!("-{}", F::fac_name()).into()
    }

    // #[inline]
    // fn new(param: impl Into<Param>) -> Self {
    //     let fac = F::new(param);
    //     FactorNeg(fac)
    // }
}

impl<F> PlFactor for FactorNeg<F>
where
    F: FactorBase + PlFactor,
{
    #[inline]
    fn try_expr(&self) -> Result<Expr> {
        Ok(-self.0.try_expr()?)
    }
}

impl<F> TFactor for FactorNeg<F>
where
    F: FactorBase + TFactor,
{
    #[inline]
    fn eval(&self, df: &DataFrame) -> Result<Series> {
        let series = self.0.eval(df)?;
        let expr = [-col(series.name().clone())];
        let out = series.into_frame().lazy().select(expr).collect()?[0].clone();
        Ok(out)
    }
}
