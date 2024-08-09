use std::sync::Arc;

use anyhow::Result;
use polars::lazy::dsl::when;
use polars::prelude::*;

use super::{GetName, PlFactor};
use crate::prelude::{Expr, ExprExt, Param};

pub struct PlExtFactor {
    pub fac: Arc<dyn PlFactor>,
    pub info: (Arc<str>, Param), // ext function name & param
    pub pl_func: Arc<dyn Fn(Expr) -> Result<Expr> + Send + Sync>,
}

impl PlExtFactor {
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

pub trait PlFactorExt: PlFactor + Sized {
    fn mean<P: Into<Param>>(self, p: P) -> impl PlFactor {
        let param: Param = p.into();
        let func = move |expr: Expr| Ok(expr.rolling_mean(param.into()));
        PlExtFactor::new(self, "mean", param, func)
    }

    fn bias<P: Into<Param>>(self, p: P) -> impl PlFactor {
        let param: Param = p.into();
        let func = move |expr: Expr| {
            let ma = expr.clone().rolling_mean(param.into());
            Ok(expr / ma - lit(1.))
        };
        PlExtFactor::new(self, "bias", param, func)
    }

    fn vol(self, p: Param) -> impl PlFactor {
        let func = move |expr: Expr| Ok(expr.rolling_std(p.into()));
        PlExtFactor::new(self, "vol", p, func)
    }

    fn pure_vol(self, p: Param) -> impl PlFactor {
        let func = move |expr: Expr| {
            let vol = expr.clone().rolling_std(p.into());
            let ma = expr.rolling_mean(p.into());
            Ok(vol / ma)
        };
        PlExtFactor::new(self, "pure_vol", p, func)
    }

    fn skew(self, p: Param) -> impl PlFactor {
        let func = move |expr: Expr| {
            let skew = expr.ts_skew(p.as_usize(), None);
            Ok(skew)
        };
        PlExtFactor::new(self, "skew", p, func)
    }

    fn kurt(self, p: Param) -> impl PlFactor {
        let func = move |expr: Expr| {
            let kurt = expr.ts_kurt(p.as_usize(), None);
            Ok(kurt)
        };
        PlExtFactor::new(self, "kurt", p, func)
    }

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

    fn vol_rank(self, p: Param) -> impl PlFactor {
        let func = move |expr: Expr| {
            Ok(expr
                .rolling_std(p.into())
                .ts_rank(5 * p.as_usize(), None, true, false))
        };
        PlExtFactor::new(self, "vol_rank", p, func)
    }

    fn pct(self, p: Param) -> impl PlFactor {
        use polars::lazy::dsl::Expr;
        let func = move |expr: Expr| Ok(expr.pct_change(lit(p.as_i32())));
        PlExtFactor::new(self, "pct", p, func)
    }

    fn lag(self, p: Param) -> impl PlFactor {
        use polars::lazy::dsl::Expr;
        let func = move |expr: Expr| Ok(expr.shift(lit(p.as_i32())));
        PlExtFactor::new(self, "pct", p, func)
    }

    fn efficiency(self, p: Param) -> impl PlFactor {
        let func = move |expr: Expr| {
            let diff_abs = expr.clone().diff(p.into(), Default::default()).abs();
            Ok(diff_abs / expr.diff(1, Default::default()).abs().rolling_sum(p.into()))
        };
        PlExtFactor::new(self, "efficiency", p, func)
    }

    fn efficiency_sign(self, p: Param) -> impl PlFactor {
        let func = move |expr: Expr| {
            let diff = expr.clone().diff(p.into(), Default::default());
            Ok(diff / expr.diff(1, Default::default()).abs().rolling_sum(p.into()))
        };
        PlExtFactor::new(self, "efficiency_sign", p, func)
    }
}

impl<F: PlFactor + Sized> PlFactorExt for F {}
