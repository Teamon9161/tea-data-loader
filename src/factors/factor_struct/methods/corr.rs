use std::sync::Arc;

use anyhow::Result;
use polars::prelude::*;

use crate::prelude::*;

/// Represents the correlation of two factors.
#[derive(Clone, Copy)]
pub struct FactorCorr<F: FactorBase, G: FactorBase> {
    pub(super) left: F,
    pub(super) right: G,
    pub(super) window: usize,
    pub(super) min_periods: Option<usize>,
}

impl<F, G> std::fmt::Debug for FactorCorr<F, G>
where
    F: FactorBase,
    G: FactorBase,
{
    #[inline]
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}.corr({})", self.left.name(), self.right.name())
    }
}

impl<F, G> FactorBase for FactorCorr<F, G>
where
    F: FactorBase,
    G: FactorBase,
{
    #[inline]
    fn fac_name() -> Arc<str> {
        format!("{}.corr({})", F::fac_name(), G::fac_name()).into()
    }

    fn new(_param: impl Into<Param>) -> Self {
        panic!("FactorCorr::new should not be called directly")
    }
}

/// polars算法实现
impl<F, G> PlFactor for FactorCorr<F, G>
where
    F: FactorBase + PlFactor,
    G: FactorBase + PlFactor,
{
    #[inline]
    fn try_expr(&self) -> Result<Expr> {
        let min_periods = self.min_periods.unwrap_or(self.window / 2);
        Ok(dsl::rolling_corr(
            self.left.try_expr()?,
            self.right.try_expr()?,
            RollingCovOptions {
                window_size: self.window as u32,
                min_periods: min_periods as u32,
                ddof: 1,
            },
        ))
    }
}
