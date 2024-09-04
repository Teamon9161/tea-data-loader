/// This module provides extensions for Polars-based factors.
///
/// It includes structures and traits for creating and manipulating
/// factors using Polars expressions.
use std::sync::Arc;

use anyhow::Result;
use polars::lazy::dsl::when;
use polars::prelude::*;

use super::{GetName, PlFactor};
use crate::prelude::{Expr, ExprExt, Param};

/// A structure representing an extended Polars factor.
///
/// This structure wraps another `PlFactor` and applies an additional
/// transformation function to it.
pub struct PlExtFactor {
    /// The underlying factor.
    pub fac: Arc<dyn PlFactor>,
    /// A tuple containing the name of the extension function and its parameter.
    pub info: (Arc<str>, Param),
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
        name: &str,
        param: Param,
        pl_func: F,
    ) -> Self {
        Self {
            fac: Arc::new(fac),
            info: (name.into(), param),
            pl_func: Arc::new(pl_func),
        }
    }
}

impl GetName for PlExtFactor {
    fn name(&self) -> String {
        match self.info.1 {
            Param::None => format!("{}_{}", self.fac.name(), &self.info.0),
            param => format!("{}_{}_{:?}", self.fac.name(), &self.info.0, param),
        }
    }
}

impl PlFactor for PlExtFactor {
    fn try_expr(&self) -> Result<Expr> {
        let expr = self.fac.try_expr()?;
        (self.pl_func)(expr)
    }
}

/// A trait providing extension methods for Polars factors.
pub trait PlFactorExt: PlFactor + Sized {
    /// Calculates the rolling mean of the factor.
    fn mean<P: Into<Param>>(self, p: P) -> impl PlFactor {
        let param: Param = p.into();
        let func = move |expr: Expr| Ok(expr.rolling_mean(param.into()));
        PlExtFactor::new(self, "mean", param, func)
    }

    /// Calculates the bias of the factor relative to its rolling mean.
    fn bias<P: Into<Param>>(self, p: P) -> impl PlFactor {
        let param: Param = p.into();
        let func = move |expr: Expr| {
            let ma = expr.clone().rolling_mean(param.into());
            Ok(expr / ma - lit(1.))
        };
        PlExtFactor::new(self, "bias", param, func)
    }

    /// Calculates the rolling standard deviation (volatility) of the factor.
    fn vol(self, p: Param) -> impl PlFactor {
        let func = move |expr: Expr| Ok(expr.rolling_std(p.into()));
        PlExtFactor::new(self, "vol", p, func)
    }

    /// Calculates the pure volatility (standard deviation divided by mean) of the factor.
    fn pure_vol(self, p: Param) -> impl PlFactor {
        let func = move |expr: Expr| {
            let vol = expr.clone().rolling_std(p.into());
            let ma = expr.rolling_mean(p.into());
            Ok(vol / ma)
        };
        PlExtFactor::new(self, "pure_vol", p, func)
    }

    /// Calculates the skewness of the factor.
    fn skew(self, p: Param) -> impl PlFactor {
        let func = move |expr: Expr| {
            let skew = expr.ts_skew(p.as_usize(), None);
            Ok(skew)
        };
        PlExtFactor::new(self, "skew", p, func)
    }

    /// Calculates the kurtosis of the factor.
    fn kurt(self, p: Param) -> impl PlFactor {
        let func = move |expr: Expr| {
            let kurt = expr.ts_kurt(p.as_usize(), None);
            Ok(kurt)
        };
        PlExtFactor::new(self, "kurt", p, func)
    }

    /// Normalizes the factor to a 0-1 range based on its rolling min and max.
    fn minmax(self, p: Param) -> impl PlFactor {
        let func = move |expr: Expr| {
            let min = expr.clone().rolling_min(p.into());
            let max = expr.clone().rolling_max(p.into());
            let expr = when(max.clone().gt(min.clone()))
                .then((expr - min.clone()) / (max - min))
                .otherwise(lit(NULL));
            Ok(expr)
        };
        PlExtFactor::new(self, "minmax", p, func)
    }

    /// Calculates the rank of the factor's volatility.
    fn vol_rank(self, p: Param) -> impl PlFactor {
        let func = move |expr: Expr| {
            Ok(expr
                .rolling_std(p.into())
                .ts_rank(5 * p.as_usize(), None, true, false))
        };
        PlExtFactor::new(self, "vol_rank", p, func)
    }

    /// Calculates the percentage change of the factor.
    fn pct(self, p: Param) -> impl PlFactor {
        use polars::lazy::dsl::Expr;
        let func = move |expr: Expr| Ok(expr.pct_change(lit(p.as_i32())));
        PlExtFactor::new(self, "pct", p, func)
    }

    /// Shifts the factor by a given number of periods.
    fn lag(self, p: Param) -> impl PlFactor {
        use polars::lazy::dsl::Expr;
        let func = move |expr: Expr| Ok(expr.shift(lit(p.as_i32())));
        PlExtFactor::new(self, "pct", p, func)
    }

    /// Calculates the efficiency ratio of the factor.
    fn efficiency(self, p: Param) -> impl PlFactor {
        let func = move |expr: Expr| {
            let diff_abs = expr.clone().diff(p.into(), Default::default()).abs();
            Ok(diff_abs / expr.diff(1, Default::default()).abs().rolling_sum(p.into()))
        };
        PlExtFactor::new(self, "efficiency", p, func)
    }

    /// Calculates the signed efficiency ratio of the factor.
    fn efficiency_sign(self, p: Param) -> impl PlFactor {
        let func = move |expr: Expr| {
            let diff = expr.clone().diff(p.into(), Default::default());
            Ok(diff / expr.diff(1, Default::default()).abs().rolling_sum(p.into()))
        };
        PlExtFactor::new(self, "efficiency_sign", p, func)
    }
}

impl<F: PlFactor + Sized> PlFactorExt for F {}
