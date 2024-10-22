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

/// Represents an aggregation operation on a factor.
///
/// This struct combines a factor with an aggregation method, allowing for
/// various statistical operations to be performed on the factor.
///
/// # Type Parameters
///
/// * `F`: The type of the factor, which must implement the `FactorBase` trait.
///
/// # Fields
///
/// * `fac`: The factor to be aggregated.
/// * `method`: The method of aggregation to be applied to the factor.
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

/// Trait for aggregation factors in Polars expressions.
///
/// This trait defines the interface for factors that can be used in aggregation operations.
pub trait PlAggFactor: std::fmt::Debug + GetName + 'static {
    /// Returns the factor expression.
    ///
    /// # Returns
    /// A `Result` containing an `Option<Expr>` representing the factor expression.
    fn fac_expr(&self) -> Result<Option<Expr>>;

    /// Returns the aggregation expression.
    ///
    /// # Returns
    /// A `Result` containing an `Expr` representing the aggregation expression.
    fn agg_expr(&self) -> Result<Expr>;

    /// Returns the name of the factor.
    ///
    /// # Returns
    /// A `String` containing the name of the factor.
    fn fac_name(&self) -> Option<String>;

    /// Converts the factor into a dynamically dispatched trait object.
    ///
    /// # Returns
    /// An `Arc<dyn PlAggFactor>` representing the factor as a trait object.
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
    fn fac_name(&self) -> Option<String> {
        Some(self.fac.name())
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
