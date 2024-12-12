use std::sync::Arc;

use anyhow::Result;
use polars::prelude::*;

use crate::prelude::*;

/// Represents the when-then-else operation.
#[derive(Clone, Copy)]
pub struct FactorIIf<C: FactorBase, T: FactorBase, O: FactorBase> {
    cond: C,
    then: T,
    otherwise: O,
}

impl<C, T, O> std::fmt::Debug for FactorIIf<C, T, O>
where
    C: FactorBase,
    T: FactorBase,
    O: FactorBase,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "iif({}, {}, {})",
            self.cond.name(),
            self.then.name(),
            self.otherwise.name()
        )
    }
}

impl<C, T, O> FactorBase for FactorIIf<C, T, O>
where
    C: FactorBase,
    T: FactorBase,
    O: FactorBase,
{
    fn fac_name() -> Arc<str> {
        format!(
            "iif({}, {}, {})",
            C::fac_name(),
            T::fac_name(),
            O::fac_name()
        )
        .into()
    }

    fn new(_param: impl Into<Param>) -> Self {
        panic!("FactorIIf::new should not be called directly")
    }
}

impl<C, T, O> PlFactor for FactorIIf<C, T, O>
where
    C: FactorBase + PlFactor,
    T: FactorBase + PlFactor,
    O: FactorBase + PlFactor,
{
    #[inline]
    fn try_expr(&self) -> Result<Expr> {
        let cond = self.cond.try_expr()?;
        let then = self.then.try_expr()?;
        let otherwise = self.otherwise.try_expr()?;
        Ok(when(cond).then(then).otherwise(otherwise))
    }
}

#[inline]
pub fn iif<C: FactorBase, T: FactorBase, O: FactorBase>(
    cond: C,
    then: T,
    otherwise: O,
) -> Factor<FactorIIf<C, T, O>> {
    Factor(FactorIIf {
        cond,
        then,
        otherwise,
    })
}
