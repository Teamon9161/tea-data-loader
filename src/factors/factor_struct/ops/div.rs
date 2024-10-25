use std::ops::Div;
use std::sync::Arc;

use anyhow::Result;
use polars::prelude::*;

use crate::prelude::*;

/// Represents the division of two factors.
#[derive(Clone, Copy)]
pub struct FactorDiv<F: FactorBase, G: FactorBase> {
    pub left: F,
    pub right: G,
}

pub type DivFactor<F, G> = Factor<FactorDiv<F, G>>;

impl<F: FactorBase, G: FactorBase> Div<G> for Factor<F> {
    type Output = DivFactor<F, G>;
    #[inline]
    fn div(self, rhs: G) -> Self::Output {
        FactorDiv {
            left: self.0,
            right: rhs,
        }
        .into()
    }
}

macro_rules! impl_div_for_primitive {
    ($($t:ty),+) => {
        $(
            impl<G: FactorBase> Div<Factor<G>> for $t {
                type Output = DivFactor<$t, G>;
                #[inline]
                fn div(self, rhs: Factor<G>) -> Self::Output {
                    FactorDiv {
                        left: self,
                        right: rhs.0,
                    }
                    .into()
                }
            }
        )+
    };
}

impl_div_for_primitive!(i32, i64, usize, f32, f64);

impl<G: FactorBase> Div<Factor<G>> for &str {
    type Output = DivFactor<Arc<str>, G>;
    #[inline]
    fn div(self, rhs: Factor<G>) -> Self::Output {
        FactorDiv {
            left: Arc::from(self),
            right: rhs.0,
        }
        .into()
    }
}

impl<G: FactorBase + PlFactor> Div<Factor<G>> for Expr {
    type Output = DivFactor<ExprFactor, G>;
    #[inline]
    fn div(self, rhs: Factor<G>) -> Self::Output {
        FactorDiv {
            left: self.into(),
            right: rhs.0,
        }
        .into()
    }
}

impl<F: FactorBase + PlFactor> Div<Expr> for Factor<F> {
    type Output = DivFactor<F, ExprFactor>;
    #[inline]
    fn div(self, rhs: Expr) -> Self::Output {
        FactorDiv {
            left: self.0,
            right: rhs.into(),
        }
        .into()
    }
}

impl<F, G> std::fmt::Debug for FactorDiv<F, G>
where
    F: FactorBase,
    G: FactorBase,
{
    #[inline]
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} / {}", self.left.name(), self.right.name())
    }
}

impl<F, G> FactorBase for FactorDiv<F, G>
where
    F: FactorBase,
    G: FactorBase,
{
    #[inline]
    fn fac_name() -> Arc<str> {
        format!("{} / {}", F::fac_name(), G::fac_name()).into()
    }

    fn new(_param: impl Into<Param>) -> Self {
        panic!("FactorDiv::new should not be called directly")
    }
}

impl<F, G> PlFactor for FactorDiv<F, G>
where
    F: FactorBase + PlFactor,
    G: FactorBase + PlFactor,
{
    #[inline]
    fn try_expr(&self) -> Result<Expr> {
        Ok(self.left.try_expr()?.protect_div(self.right.try_expr()?))
    }
}

impl<F, G> TFactor for FactorDiv<F, G>
where
    F: FactorBase + TFactor,
    G: FactorBase + TFactor,
{
    #[inline]
    fn eval(&self, df: &DataFrame) -> Result<Series> {
        Ok((self.left.eval(df)?.protect_div(self.right.eval(df)?))?)
    }
}
