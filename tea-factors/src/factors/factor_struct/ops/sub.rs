use std::ops::Sub;
use std::sync::Arc;

use anyhow::Result;
use polars::prelude::*;

use crate::prelude::*;

/// Represents the subtraction of two factors.
#[derive(Clone, Copy)]
pub struct FactorSub<F: FactorBase, G: FactorBase> {
    pub left: F,
    pub right: G,
}

pub type SubFactor<F, G> = Factor<FactorSub<F, G>>;

impl<F: FactorBase, G: FactorBase> Sub<G> for Factor<F> {
    type Output = SubFactor<F, G>;
    #[inline]
    fn sub(self, rhs: G) -> Self::Output {
        let fac = FactorSub {
            left: self.0,
            right: rhs,
        };
        Factor(fac)
    }
}

macro_rules! impl_sub_for_primitive {
    ($($t:ty),+) => {
        $(
            impl<G: FactorBase> Sub<Factor<G>> for $t {
                type Output = SubFactor<$t, G>;
                #[inline]
                fn sub(self, rhs: Factor<G>) -> Self::Output {
                    FactorSub {
                        left: self,
                        right: rhs.0,
                    }
                    .into()
                }
            }
        )+
    };
}

impl_sub_for_primitive!(i32, i64, usize, f32, f64);

impl<G: FactorBase> Sub<Factor<G>> for &str {
    type Output = SubFactor<Arc<str>, G>;
    #[inline]
    fn sub(self, rhs: Factor<G>) -> Self::Output {
        FactorSub {
            left: Arc::from(self),
            right: rhs.0,
        }
        .into()
    }
}

impl<G: FactorBase + PlFactor> Sub<Factor<G>> for Expr {
    type Output = SubFactor<ExprFactor, G>;
    #[inline]
    fn sub(self, rhs: Factor<G>) -> Self::Output {
        FactorSub {
            left: self.into(),
            right: rhs.0,
        }
        .into()
    }
}

impl<F: FactorBase + PlFactor> Sub<Expr> for Factor<F> {
    type Output = SubFactor<F, ExprFactor>;
    #[inline]
    fn sub(self, rhs: Expr) -> Self::Output {
        FactorSub {
            left: self.0,
            right: rhs.into(),
        }
        .into()
    }
}

impl<F, G> std::fmt::Debug for FactorSub<F, G>
where
    F: FactorBase,
    G: FactorBase,
{
    #[inline]
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} - {}", self.left.name(), self.right.name())
    }
}

impl<F, G> FactorBase for FactorSub<F, G>
where
    F: FactorBase,
    G: FactorBase,
{
    #[inline]
    fn fac_name() -> Arc<str> {
        format!("{} - {}", F::fac_name(), G::fac_name()).into()
    }

    fn new(_param: impl Into<Param>) -> Self {
        panic!("FactorSub::new should not be called directly")
    }
}

impl<F, G> PlFactor for FactorSub<F, G>
where
    F: FactorBase + PlFactor,
    G: FactorBase + PlFactor,
{
    #[inline]
    fn try_expr(&self) -> Result<Expr> {
        Ok(self.left.try_expr()? - self.right.try_expr()?)
    }
}

impl<F, G> TFactor for FactorSub<F, G>
where
    F: FactorBase + TFactor,
    G: FactorBase + TFactor,
{
    #[inline]
    fn eval(&self, df: &DataFrame) -> Result<Series> {
        Ok((self.left.eval(df)? - self.right.eval(df)?)?)
    }
}
