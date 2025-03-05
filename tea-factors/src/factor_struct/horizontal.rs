use std::sync::Arc;

use anyhow::Result;
use polars::lazy::dsl::Expr;

use crate::prelude::{FactorBase, GetName, PlFactor};

#[derive(Clone, Copy)]
pub struct HSumFactor<F, const N: usize>(pub [F; N]);

impl<F: GetName, const N: usize> std::fmt::Debug for HSumFactor<F, N> {
    #[inline]
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "hsum({})",
            self.0
                .iter()
                .map(|f| f.name())
                .collect::<Vec<_>>()
                .join(", ")
        )
    }
}

impl<F: GetName + Clone, const N: usize> FactorBase for HSumFactor<F, N> {
    #[inline]
    fn fac_name() -> Arc<str> {
        "hsum".into()
    }
}

impl<F: PlFactor + Clone, const N: usize> PlFactor for HSumFactor<F, N> {
    #[inline]
    fn try_expr(&self) -> Result<Expr> {
        use polars::lazy::dsl::sum_horizontal;
        Ok(sum_horizontal(
            self.0
                .iter()
                .map(|f| f.try_expr())
                .collect::<Result<Vec<_>>>()?,
            true,
        )?)
    }
}

#[macro_export]
macro_rules! hsum {
    ($($factor:expr),+ $(,)?) => {{
        let arr = [$($factor.pl_dyn()),+];
        $crate::prelude::Factor($crate::factor_struct::HSumFactor(arr))
    }};
}
