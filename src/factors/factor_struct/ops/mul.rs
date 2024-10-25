use std::ops::Mul;
use std::sync::Arc;

use anyhow::Result;
use polars::prelude::*;

use crate::prelude::*;

/// Represents the multiplication of two factors.
#[derive(Clone, Copy)]
pub struct FactorMul<F: FactorBase, G: FactorBase> {
    pub left: F,
    pub right: G,
}

pub type MulFactor<F, G> = Factor<FactorMul<F, G>>;

impl<F: FactorBase, G: FactorBase> Mul<G> for Factor<F> {
    type Output = MulFactor<F, G>;
    #[inline]
    fn mul(self, rhs: G) -> Self::Output {
        FactorMul {
            left: self.0,
            right: rhs,
        }
        .into()
    }
}

macro_rules! impl_mul_for_primitive {
    ($($t:ty),+) => {
        $(
            impl<G: FactorBase> Mul<Factor<G>> for $t {
                type Output = MulFactor<$t, G>;
                #[inline]
                fn mul(self, rhs: Factor<G>) -> Self::Output {
                    FactorMul {
                        left: self,
                        right: rhs.0,
                    }
                    .into()
                }
            }
        )+
    };
}

impl_mul_for_primitive!(i32, i64, usize, f32, f64);

impl<G: FactorBase> Mul<Factor<G>> for &str {
    type Output = MulFactor<Arc<str>, G>;
    #[inline]
    fn mul(self, rhs: Factor<G>) -> Self::Output {
        FactorMul {
            left: Arc::from(self),
            right: rhs.0,
        }
        .into()
    }
}

impl<G: FactorBase + PlFactor> Mul<Factor<G>> for Expr {
    type Output = MulFactor<ExprFactor, G>;
    #[inline]
    fn mul(self, rhs: Factor<G>) -> Self::Output {
        FactorMul {
            left: self.into(),
            right: rhs.0,
        }
        .into()
    }
}

impl<F: FactorBase + PlFactor> Mul<Expr> for Factor<F> {
    type Output = MulFactor<F, ExprFactor>;
    #[inline]
    fn mul(self, rhs: Expr) -> Self::Output {
        FactorMul {
            left: self.0,
            right: rhs.into(),
        }
        .into()
    }
}

impl<F, G> std::fmt::Debug for FactorMul<F, G>
where
    F: FactorBase,
    G: FactorBase,
{
    #[inline]
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} * {}", self.left.name(), self.right.name())
    }
}

impl<F, G> FactorBase for FactorMul<F, G>
where
    F: FactorBase,
    G: FactorBase,
{
    #[inline]
    fn fac_name() -> Arc<str> {
        format!("{} * {}", F::fac_name(), G::fac_name()).into()
    }

    fn new(_param: impl Into<Param>) -> Self {
        panic!("FactorMul::new should not be called directly")
    }
}

impl<F, G> PlFactor for FactorMul<F, G>
where
    F: FactorBase + PlFactor,
    G: FactorBase + PlFactor,
{
    #[inline]
    fn try_expr(&self) -> Result<Expr> {
        Ok(self.left.try_expr()? * self.right.try_expr()?)
    }
}

impl<F, G> TFactor for FactorMul<F, G>
where
    F: FactorBase + TFactor,
    G: FactorBase + TFactor,
{
    #[inline]
    fn eval(&self, df: &DataFrame) -> Result<Series> {
        Ok((self.left.eval(df)? * self.right.eval(df)?)?)
    }
}
