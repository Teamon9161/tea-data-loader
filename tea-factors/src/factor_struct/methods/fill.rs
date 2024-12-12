use std::sync::Arc;

use polars::prelude::*;

use crate::prelude::*;

/// Represents a factor with null value filling strategy applied.
///
/// This struct allows for various methods of filling null values in a factor,
/// including using a specific strategy or a constant value.
///
/// # Type Parameters
///
/// * `F`: The type of the original factor, must implement `FactorBase`.
/// * `G`: The type of the fill value (if used), must implement `FactorBase`.
#[derive(Clone, Copy)]
pub struct FactorFill<F: FactorBase, G: FactorBase> {
    pub(super) fac: F,
    pub(super) strategy: Option<FillNullStrategy>,
    pub(super) value: Option<G>,
}

impl<F: FactorBase, G: FactorBase> std::fmt::Debug for FactorFill<F, G> {
    #[inline]
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.fac.name())
    }
}

impl<F: FactorBase, G: FactorBase> FactorBase for FactorFill<F, G> {
    #[inline]
    fn fac_name() -> Arc<str> {
        let f = F::fac_name();
        format!("{}", f).into()
    }

    fn new(_param: impl Into<Param>) -> Self {
        panic!("FactorFill::new should not be called directly")
    }
}

impl<F, G> PlFactor for FactorFill<F, G>
where
    F: FactorBase + PlFactor,
    G: FactorBase + PlFactor,
{
    #[inline]
    fn try_expr(&self) -> Result<Expr> {
        let expr = self.fac.try_expr()?;
        if let Some(strategy) = self.strategy {
            Ok(expr.fill_null_with_strategy(strategy))
        } else if let Some(f) = &self.value {
            Ok(expr.fill_null(f.try_expr()?))
        } else {
            bail!("No fill strategy or value provided")
        }
    }
}
