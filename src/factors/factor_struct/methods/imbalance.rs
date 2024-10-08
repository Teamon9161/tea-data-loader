use std::sync::Arc;

use anyhow::Result;
use polars::prelude::*;

use crate::prelude::*;

/// Represents the imbalance of two factors.
#[derive(Clone, Copy)]
pub struct FactorImbalance<F: FactorBase, G: FactorBase> {
    pub(super) left: F,
    pub(super) right: G,
}

impl<F, G> std::fmt::Debug for FactorImbalance<F, G>
where
    F: FactorBase,
    G: FactorBase,
{
    #[inline]
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}.imb({})", self.left.name(), self.right.name())
    }
}

impl<F, G> FactorBase for FactorImbalance<F, G>
where
    F: FactorBase,
    G: FactorBase,
{
    #[inline]
    fn fac_name() -> Arc<str> {
        format!("{}.imb({})", F::fac_name(), G::fac_name()).into()
    }

    fn new(_param: impl Into<Param>) -> Self {
        panic!("FactorAdd::new should not be called directly")
    }
}

/// polars算法实现
impl<F, G> PlFactor for FactorImbalance<F, G>
where
    F: FactorBase + PlFactor,
    G: FactorBase + PlFactor,
{
    #[inline]
    fn try_expr(&self) -> Result<Expr> {
        Ok(self.left.try_expr()?.imbalance(self.right.try_expr()?))
    }
}
