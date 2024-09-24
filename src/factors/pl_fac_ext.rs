/// This module provides extensions for Polars-based factors.
///
/// It includes structures and traits for creating and manipulating
/// factors using Polars expressions.
use std::sync::Arc;

use anyhow::Result;
use polars::lazy::dsl::when;
use polars::prelude::*;

use super::PlFactor;
use crate::prelude::{Expr, ExprExt, Param};

/// A structure representing an extended Polars factor.
///
/// This structure wraps another `PlFactor` and applies an additional
/// transformation function to it.
pub struct PlExtFactor {
    /// The underlying factor.
    pub fac: Arc<dyn PlFactor>,
    /// A tuple containing the method of the extension function and its parameter.
    pub info: (PlExtMethod, Param),
    /// The function to apply to the factor's expression.
    pub pl_func: Arc<dyn Fn(Expr) -> Result<Expr> + Send + Sync>,
}

impl PlExtFactor {
    /// Creates a new `PlExtFactor`.
    ///
    /// # Arguments
    ///
    /// * `fac` - The underlying factor.
    /// * `name` - The name of the extension function.
    /// * `param` - The parameter for the extension function.
    /// * `pl_func` - The function to apply to the factor's expression.
    #[inline]
    pub fn new<P: PlFactor, F: Fn(Expr) -> Result<Expr> + Send + Sync + 'static>(
        fac: P,
        method: PlExtMethod,
        param: Param,
        pl_func: F,
    ) -> Self {
        Self {
            fac: Arc::new(fac),
            info: (method, param),
            pl_func: Arc::new(pl_func),
        }
    }
}

// impl GetName for PlExtFactor {
//     fn name(&self) -> String {
//         match self.info.1 {
//             Param::None => format!("{}_{}", self.fac.name(), self.info.0.name()),
//             param => format!("{}_{}_{:?}", self.fac.name(), self.info.0.name(), param),
//         }
//     }
// }

impl std::fmt::Debug for PlExtFactor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.info.1 {
            Param::None => write!(f, "{}_{}", self.fac.name(), self.info.0.name()),
            param => write!(f, "{}_{}_{:?}", self.fac.name(), self.info.0.name(), param),
        }
    }
}

impl crate::prelude::GetName for PlExtFactor {}

impl PlFactor for PlExtFactor {
    fn try_expr(&self) -> Result<Expr> {
        let expr = self.fac.try_expr()?;
        (self.pl_func)(expr)
    }
}

#[derive(Debug, Clone)]
pub enum PlExtMethod {
    Mean,
    Bias,
    Vol,
    PureVol,
    Zscore,
    Skew,
    Kurt,
    Minmax,
    VolRank,
    Pct,
    Lag,
    Efficiency,
    EfficiencySign,
    Imb(Arc<dyn PlFactor>),
    Add(Arc<dyn PlFactor>),
    Sub(Arc<dyn PlFactor>),
    Mul(Arc<dyn PlFactor>),
    Div(Arc<dyn PlFactor>),
}

impl PlExtMethod {
    pub fn name(&self) -> Arc<str> {
        match self {
            PlExtMethod::Mean => "mean".into(),
            PlExtMethod::Bias => "bias".into(),
            PlExtMethod::Vol => "vol".into(),
            PlExtMethod::PureVol => "pure_vol".into(),
            PlExtMethod::Zscore => "zscore".into(),
            PlExtMethod::Skew => "skew".into(),
            PlExtMethod::Kurt => "kurt".into(),
            PlExtMethod::Minmax => "minmax".into(),
            PlExtMethod::VolRank => "vol_rank".into(),
            PlExtMethod::Pct => "pct".into(),
            PlExtMethod::Lag => "lag".into(),
            PlExtMethod::Efficiency => "efficiency".into(),
            PlExtMethod::EfficiencySign => "efficiency_sign".into(),
            PlExtMethod::Imb(fac) => format!("imb_{}", fac.name()).into(),
            PlExtMethod::Add(fac) => format!("add_{}", fac.name()).into(),
            PlExtMethod::Sub(fac) => format!("sub_{}", fac.name()).into(),
            PlExtMethod::Mul(fac) => format!("mul_{}", fac.name()).into(),
            PlExtMethod::Div(fac) => format!("div_{}", fac.name()).into(),
        }
    }
}
/// A trait providing extension methods for Polars factors.
pub trait PlFactorExt: PlFactor + Sized {
    /// Calculates the rolling mean of the factor.
    fn mean(self, p: impl Into<Param>) -> impl PlFactor {
        let param: Param = p.into();
        let func = move |expr: Expr| {
            if param.as_i32() == 1 {
                Ok(expr)
            } else {
                Ok(expr.rolling_mean(param.into()))
            }
        };
        PlExtFactor::new(self, PlExtMethod::Mean, param, func)
    }

    /// Calculates the bias of the factor relative to its rolling mean.
    fn bias(self, p: impl Into<Param>) -> impl PlFactor {
        let param: Param = p.into();
        let func = move |expr: Expr| {
            let ma = expr.clone().rolling_mean(param.into());
            Ok(expr / ma - lit(1.))
        };
        PlExtFactor::new(self, PlExtMethod::Bias, param, func)
    }

    /// Calculates the rolling standard deviation (volatility) of the factor.
    fn vol(self, p: impl Into<Param>) -> impl PlFactor {
        let param: Param = p.into();
        let func = move |expr: Expr| Ok(expr.rolling_std(param.into()));

        PlExtFactor::new(self, PlExtMethod::Vol, param, func)
    }

    /// Calculates the pure volatility (standard deviation divided by mean) of the factor.
    fn pure_vol(self, p: impl Into<Param>) -> impl PlFactor {
        let param: Param = p.into();
        let func = move |expr: Expr| {
            let vol = expr.clone().rolling_std(param.into());

            let ma = expr.rolling_mean(param.into());
            Ok(vol / ma)
        };
        PlExtFactor::new(self, PlExtMethod::PureVol, param, func)
    }

    fn zscore(self, p: impl Into<Param>) -> impl PlFactor {
        let param: Param = p.into();
        let func = move |expr: Expr| {
            let ma = expr.clone().rolling_mean(param.into());
            let vol = expr.clone().rolling_std(param.into());
            Ok((expr - ma).protect_div(vol))
        };

        PlExtFactor::new(self, PlExtMethod::Zscore, param, func)
    }

    /// Calculates the skewness of the factor.
    fn skew(self, p: impl Into<Param>) -> impl PlFactor {
        let param: Param = p.into();
        let func = move |expr: Expr| {
            let skew = expr.ts_skew(param.as_usize(), None);
            Ok(skew)
        };
        PlExtFactor::new(self, PlExtMethod::Skew, param, func)
    }

    /// Calculates the kurtosis of the factor.
    fn kurt(self, p: impl Into<Param>) -> impl PlFactor {
        let param: Param = p.into();
        let func = move |expr: Expr| {
            let kurt = expr.ts_kurt(param.as_usize(), None);
            Ok(kurt)
        };
        PlExtFactor::new(self, PlExtMethod::Kurt, param, func)
    }

    /// Normalizes the factor to a 0-1 range based on its rolling min and max.
    fn minmax(self, p: impl Into<Param>) -> impl PlFactor {
        let param: Param = p.into();
        let func = move |expr: Expr| {
            let min = expr.clone().rolling_min(param.into());
            let max = expr.clone().rolling_max(param.into());
            let expr = when(max.clone().gt(min.clone()))
                .then((expr - min.clone()) / (max - min))
                .otherwise(lit(NULL));
            Ok(expr)
        };
        PlExtFactor::new(self, PlExtMethod::Minmax, param, func)
    }

    /// Calculates the rank of the factor's volatility.
    fn vol_rank(self, p: impl Into<Param>) -> impl PlFactor {
        let param: Param = p.into();
        let func = move |expr: Expr| {
            Ok(expr
                .rolling_std(param.into())
                .ts_rank(5 * param.as_usize(), None, true, false))
        };
        PlExtFactor::new(self, PlExtMethod::VolRank, param, func)
    }

    /// Calculates the percentage change of the factor.
    fn pct(self, p: impl Into<Param>) -> impl PlFactor {
        let param: Param = p.into();
        let func = move |expr: Expr| Ok(expr.pct_change(lit(param.as_i32())));

        PlExtFactor::new(self, PlExtMethod::Pct, param, func)
    }

    /// Shifts the factor by a given number of periods.
    fn lag(self, p: impl Into<Param>) -> impl PlFactor {
        let param: Param = p.into();
        let func = move |expr: Expr| Ok(expr.shift(lit(param.as_i32())));

        PlExtFactor::new(self, PlExtMethod::Lag, param, func)
    }

    /// Calculates the efficiency ratio of the factor.
    fn efficiency(self, p: impl Into<Param>) -> impl PlFactor {
        let param: Param = p.into();
        let func = move |expr: Expr| {
            let diff_abs = expr.clone().diff(param.into(), Default::default()).abs();
            Ok(diff_abs
                / expr
                    .diff(1, Default::default())
                    .abs()
                    .rolling_sum(param.into()))
        };
        PlExtFactor::new(self, PlExtMethod::Efficiency, param, func)
    }

    /// Calculates the signed efficiency ratio of the factor.
    fn efficiency_sign(self, p: impl Into<Param>) -> impl PlFactor {
        let param: Param = p.into();
        let func = move |expr: Expr| {
            let diff = expr.clone().diff(param.into(), Default::default());

            Ok(diff
                / expr
                    .diff(1, Default::default())
                    .abs()
                    .rolling_sum(param.into()))
        };
        PlExtFactor::new(self, PlExtMethod::EfficiencySign, param, func)
    }

    /// Calculates the imbalance between two factors.
    ///
    /// The imbalance is defined as (self - other) / (self + other) when (self + other) > 0,
    /// and NULL otherwise.
    fn imb(self, other: impl PlFactor) -> impl PlFactor {
        let other = Arc::new(other);
        let other_expr = other.try_expr().unwrap();
        let func = move |expr: Expr| {
            let imb_expr =
                (expr.clone() - other_expr.clone()) / (expr.clone() + other_expr.clone());
            let expr = when((expr + other_expr.clone()).gt(0.lit()))
                .then(imb_expr)
                .otherwise(NULL.lit());
            Ok(expr)
        };
        PlExtFactor::new(self, PlExtMethod::Imb(other), Param::None, func)
    }

    /// Adds two factors together.
    fn add(self, other: impl PlFactor) -> impl PlFactor {
        let other = Arc::new(other);
        let other_expr = other.expr();

        let func = move |expr: Expr| Ok(expr.clone() + other_expr.clone());
        PlExtFactor::new(self, PlExtMethod::Add(other), Param::None, func)
    }

    /// Subtracts one factor from another.
    fn sub(self, other: impl PlFactor) -> impl PlFactor {
        let other = Arc::new(other);
        let other_expr = other.expr();

        let func = move |expr: Expr| Ok(expr.clone() - other_expr.clone());
        PlExtFactor::new(self, PlExtMethod::Sub(other), Param::None, func)
    }

    /// Multiplies two factors together.
    fn mul(self, other: impl PlFactor) -> impl PlFactor {
        let other = Arc::new(other);
        let other_expr = other.expr();

        let func = move |expr: Expr| Ok(expr.clone() * other_expr.clone());
        PlExtFactor::new(self, PlExtMethod::Mul(other), Param::None, func)
    }

    /// Divides one factor by another, using protected division.
    fn div(self, other: impl PlFactor) -> impl PlFactor {
        let other = Arc::new(other);
        let other_expr = other.expr();

        let func = move |expr: Expr| Ok(expr.clone().protect_div(other_expr.clone()));

        PlExtFactor::new(self, PlExtMethod::Div(other), Param::None, func)
    }
}

impl<F: PlFactor + Sized> PlFactorExt for F {}
