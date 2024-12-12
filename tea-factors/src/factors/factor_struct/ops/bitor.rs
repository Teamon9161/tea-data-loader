use std::ops::BitOr;
use std::sync::Arc;

use anyhow::Result;
use polars::prelude::*;

use crate::prelude::{Factor, FactorBase, GetName, Param, PlFactor, TFactor};

/// Represents the bitwise OR of two factors.
#[derive(Clone, Copy)]
pub struct FactorBitOr<F: FactorBase, G: FactorBase> {
    pub left: F,
    pub right: G,
}

pub type BitOrFactor<F, G> = Factor<FactorBitOr<F, G>>;

impl<F: FactorBase, G: FactorBase> BitOr<G> for Factor<F> {
    type Output = BitOrFactor<F, G>;
    #[inline]
    fn bitor(self, rhs: G) -> Self::Output {
        FactorBitOr {
            left: self.0,
            right: rhs,
        }
        .into()
    }
}

impl<F, G> std::fmt::Debug for FactorBitOr<F, G>
where
    F: FactorBase,
    G: FactorBase,
{
    #[inline]
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} | {}", self.left.name(), self.right.name())
    }
}

impl<F, G> FactorBase for FactorBitOr<F, G>
where
    F: FactorBase,
    G: FactorBase,
{
    #[inline]
    fn fac_name() -> Arc<str> {
        format!("{} | {}", F::fac_name(), G::fac_name()).into()
    }

    fn new(_param: impl Into<Param>) -> Self {
        panic!("FactorBitOr::new should not be called directly")
    }
}

impl<F, G> PlFactor for FactorBitOr<F, G>
where
    F: FactorBase + PlFactor + Send + Sync + 'static,
    G: FactorBase + PlFactor + Send + Sync + 'static,
{
    #[inline]
    fn try_expr(&self) -> Result<Expr> {
        Ok(self.left.try_expr()?.or(self.right.try_expr()?))
    }
}

impl<F, G> TFactor for FactorBitOr<F, G>
where
    F: FactorBase + TFactor + Send + Sync + 'static,
    G: FactorBase + TFactor + Send + Sync + 'static,
{
    #[inline]
    fn eval(&self, df: &DataFrame) -> Result<Series> {
        Ok((self.left.eval(df)?.bitor(&self.right.eval(df)?))?)
    }
}
