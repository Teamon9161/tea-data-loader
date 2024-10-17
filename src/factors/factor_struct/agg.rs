use polars::prelude::*;

use super::super::export::*;
use crate::factors::GetName;

#[derive(Clone, Copy)]
pub enum FactorAggMethod {
    Mean,
    Sum,
    Min,
    Max,
    Median,
    Std,
    Var,
    Skew,
    Kurt,
    Quantile(f64),
    First,
    Last,
    Nth(usize),
    Count,
}

impl std::fmt::Debug for FactorAggMethod {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FactorAggMethod::Mean => write!(f, "mean"),
            FactorAggMethod::Sum => write!(f, "sum"),
            FactorAggMethod::Min => write!(f, "min"),
            FactorAggMethod::Max => write!(f, "max"),
            FactorAggMethod::Median => write!(f, "median"),
            FactorAggMethod::Std => write!(f, "std"),
            FactorAggMethod::Var => write!(f, "var"),
            FactorAggMethod::Skew => write!(f, "skew"),
            FactorAggMethod::Kurt => write!(f, "kurt"),
            FactorAggMethod::Quantile(q) => write!(f, "quantile({})", q),
            FactorAggMethod::First => write!(f, "first"),
            FactorAggMethod::Last => write!(f, "last"),
            FactorAggMethod::Nth(n) => write!(f, "nth({})", n),
            FactorAggMethod::Count => write!(f, "count"),
        }
    }
}

#[derive(Clone, Debug, Copy)]
pub struct FactorAgg<F: FactorBase> {
    pub fac: F,
    pub method: FactorAggMethod,
}

impl<F: FactorBase> FactorAgg<F> {
    #[inline]
    pub fn fac_name(&self) -> String {
        self.fac.name()
    }
}

pub trait PlAggFactor: std::fmt::Debug + GetName + 'static {
    fn fac_expr(&self) -> Result<Option<Expr>>;

    fn agg_expr(&self) -> Result<Expr>;

    fn fac_name(&self) -> String;

    #[inline]
    fn pl_dyn(self) -> Arc<dyn PlAggFactor>
    where
        Self: Sized,
    {
        Arc::new(self)
    }
}

impl<F: FactorBase + PlFactor> PlAggFactor for FactorAgg<F> {
    #[inline]
    fn fac_expr(&self) -> Result<Option<Expr>> {
        self.fac.try_expr().map(Some)
    }

    #[inline]
    fn fac_name(&self) -> String {
        self.fac.name()
    }

    fn agg_expr(&self) -> Result<Expr> {
        let name = self.fac.name();
        let expr = col(&name);
        let expr = match self.method {
            FactorAggMethod::Mean => expr.mean(),
            FactorAggMethod::Sum => expr.sum(),
            FactorAggMethod::Min => expr.min(),
            FactorAggMethod::Max => expr.max(),
            FactorAggMethod::Median => expr.median(),
            FactorAggMethod::Std => expr.std(1),
            FactorAggMethod::Var => expr.var(1),
            FactorAggMethod::Skew => expr.skew(false).fill_nan(NONE),
            FactorAggMethod::Kurt => expr.kurtosis(true, false).fill_nan(NONE),
            FactorAggMethod::Quantile(q) => expr.quantile(q.lit(), QuantileInterpolOptions::Linear),
            FactorAggMethod::First => expr.first(),
            FactorAggMethod::Last => expr.last(),
            FactorAggMethod::Nth(n) => expr.get(n as i32),
            FactorAggMethod::Count => expr.count(),
        };
        Ok(expr)
    }
}

impl<F: FactorBase> GetName for FactorAgg<F> {
    #[inline]
    fn name(&self) -> String {
        format!("{}_agg({:?})", self.fac.name(), self.method)
    }
}
