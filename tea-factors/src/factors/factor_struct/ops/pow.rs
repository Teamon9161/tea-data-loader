use std::sync::Arc;

use crate::prelude::*;

#[derive(Clone, Copy)]
/// Represents the power of two factors.
pub struct FactorPow<F: FactorBase, G: FactorBase> {
    fac: F,
    exponent: G,
}

impl<F: FactorBase, G: FactorBase> std::fmt::Debug for FactorPow<F, G> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?} ** {:?}", self.fac, self.exponent)
    }
}

impl<F: FactorBase, G: FactorBase> FactorBase for FactorPow<F, G> {
    fn fac_name() -> Arc<str> {
        format!("{:?}_pow({:?})", F::fac_name(), G::fac_name()).into()
    }

    fn new(_param: impl Into<Param>) -> Self {
        panic!("FactorPow::new should not be called directly")
    }
}

impl<F: PlFactor + FactorBase, G: PlFactor + FactorBase> PlFactor for FactorPow<F, G> {
    fn try_expr(&self) -> Result<Expr> {
        let expr = self.fac.try_expr()?.pow(self.exponent.try_expr()?);
        Ok(expr)
    }
}

pub type PowFactor<F, G> = Factor<FactorPow<F, G>>;

impl<F: FactorBase> Factor<F> {
    #[inline]
    pub fn pow<G: FactorBase>(self, exponent: G) -> PowFactor<F, G> {
        FactorPow {
            fac: self.0,
            exponent,
        }
        .into()
    }
}
