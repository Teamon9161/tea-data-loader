use std::ops::Add;
use std::sync::Arc;

use anyhow::Result;
use polars::prelude::*;

use crate::prelude::*;

/// Represents the addition of two factors.
#[derive(Clone, Copy)]
pub struct FactorAdd<F: FactorBase, G: FactorBase> {
    left: F,
    right: G,
}

pub type AddFactor<F, G> = Factor<FactorAdd<F, G>>;

impl<F: FactorBase, G: FactorBase> Add<G> for Factor<F> {
    type Output = AddFactor<F, G>;
    #[inline]
    fn add(self, rhs: G) -> Self::Output {
        FactorAdd {
            left: self.0,
            right: rhs,
        }
        .into()
    }
}

macro_rules! impl_add_for_primitive {
    ($($t:ty),+) => {
        $(
            impl<G: FactorBase> Add<Factor<G>> for $t {
                type Output = AddFactor<$t, G>;
                #[inline]
                fn add(self, rhs: Factor<G>) -> Self::Output {
                    FactorAdd {
                        left: self,
                        right: rhs.0,
                    }
                    .into()
                }
            }
        )+
    };
}

impl_add_for_primitive!(i32, i64, usize, f32, f64);

impl<G: FactorBase> Add<Factor<G>> for &str {
    type Output = AddFactor<Arc<str>, G>;
    #[inline]
    fn add(self, rhs: Factor<G>) -> Self::Output {
        FactorAdd {
            left: Arc::from(self),
            right: rhs.0,
        }
        .into()
    }
}

impl<G: FactorBase + PlFactor> Add<Factor<G>> for Expr {
    type Output = AddFactor<ExprFactor, G>;
    #[inline]
    fn add(self, rhs: Factor<G>) -> Self::Output {
        FactorAdd {
            left: self.into(),
            right: rhs.0,
        }
        .into()
    }
}

impl<F: FactorBase> Add<Expr> for Factor<F> {
    type Output = AddFactor<F, ExprFactor>;
    #[inline]
    fn add(self, rhs: Expr) -> Self::Output {
        FactorAdd {
            left: self.0,
            right: rhs.into(),
        }
        .into()
    }
}

impl<F, G> std::fmt::Debug for FactorAdd<F, G>
where
    F: FactorBase,
    G: FactorBase,
{
    #[inline]
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} + {}", self.left.name(), self.right.name())
    }
}

impl<F, G> FactorBase for FactorAdd<F, G>
where
    F: FactorBase,
    G: FactorBase,
{
    #[inline]
    fn fac_name() -> Arc<str> {
        format!("{} + {}", F::fac_name(), G::fac_name()).into()
    }

    fn new(_param: impl Into<Param>) -> Self {
        panic!("FactorAdd::new should not be called directly")
    }
}

/// polars算法实现
impl<F, G> PlFactor for FactorAdd<F, G>
where
    F: FactorBase + PlFactor,
    G: FactorBase + PlFactor,
{
    #[inline]
    fn try_expr(&self) -> Result<Expr> {
        Ok(self.left.try_expr()? + self.right.try_expr()?)
    }
}

impl<F, G> TFactor for FactorAdd<F, G>
where
    F: FactorBase + TFactor,
    G: FactorBase + TFactor,
{
    #[inline]
    fn eval(&self, df: &DataFrame) -> Result<Series> {
        Ok((self.left.eval(df)? + self.right.eval(df)?)?)
    }
}
