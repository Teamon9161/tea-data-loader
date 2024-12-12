use std::ops::BitAnd;
use std::sync::Arc;

use anyhow::Result;
use polars::prelude::*;

use crate::prelude::{Factor, FactorBase, GetName, Param, PlFactor, TFactor};

/// Represents the bitwise AND of two factors.
#[derive(Clone, Copy)]
pub struct FactorBitAnd<F: FactorBase, G: FactorBase> {
    pub left: F,
    pub right: G,
}

pub type BitAndFactor<F, G> = Factor<FactorBitAnd<F, G>>;

impl<F: FactorBase, G: FactorBase> BitAnd<G> for Factor<F> {
    type Output = BitAndFactor<F, G>;
    #[inline]
    fn bitand(self, rhs: G) -> Self::Output {
        FactorBitAnd {
            left: self.0,
            right: rhs,
        }
        .into()
    }
}

impl<F, G> std::fmt::Debug for FactorBitAnd<F, G>
where
    F: FactorBase,
    G: FactorBase,
{
    #[inline]
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} & {}", self.left.name(), self.right.name())
    }
}

impl<F, G> FactorBase for FactorBitAnd<F, G>
where
    F: FactorBase,
    G: FactorBase,
{
    #[inline]
    fn fac_name() -> Arc<str> {
        format!("{} & {}", F::fac_name(), G::fac_name()).into()
    }

    fn new(_param: impl Into<Param>) -> Self {
        panic!("FactorBitAnd::new should not be called directly")
    }
}

impl<F, G> PlFactor for FactorBitAnd<F, G>
where
    F: FactorBase + PlFactor,
    G: FactorBase + PlFactor,
{
    #[inline]
    fn try_expr(&self) -> Result<Expr> {
        Ok(self.left.try_expr()?.and(self.right.try_expr()?))
    }
}

impl<F, G> TFactor for FactorBitAnd<F, G>
where
    F: FactorBase + TFactor,
    G: FactorBase + TFactor,
{
    #[inline]
    fn eval(&self, df: &DataFrame) -> Result<Series> {
        Ok((self.left.eval(df)?.bitand(&self.right.eval(df)?))?)
    }
}
