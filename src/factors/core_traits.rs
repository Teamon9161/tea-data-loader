use anyhow::Result;
use polars::lazy::dsl::Expr;
use polars::prelude::*;

use super::param::Param;
use crate::factors::factor_struct::{FactorAgg, FactorAggMethod};
use crate::factors::Factor;

/// Trait defining the base functionality for factors.
///
/// This trait provides two essential methods for factors:
/// - `fac_name`: for obtaining the factor's name
/// - `new`: for creating a new instance of the factor
pub trait FactorBase: std::fmt::Debug + Clone + Sized {
    /// Returns the name of the factor as an `Arc<str>`.
    fn fac_name() -> Arc<str>;

    /// Creates a new instance of the factor with the given parameter.
    ///
    /// # Arguments
    ///
    /// * `param` - A value that can be converted into a `Param`.
    // fn new<P: Into<Param>>(param: P) -> Self;
    #[inline]
    fn new(param: impl Into<Param>) -> Self
    where
        Self: From<Param>,
    {
        let param = param.into();
        param.into()
    }

    /// Creates a new `Factor` instance with the given parameter.
    ///
    /// This is a convenience method that wraps the factor in a `Factor` struct.
    ///
    /// # Arguments
    ///
    /// * `param` - A value that can be converted into a `Param`.
    ///
    /// # Returns
    ///
    /// A `Factor<Self>` instance containing the new factor.
    #[inline]
    fn fac(param: impl Into<Param>) -> Factor<Self>
    where
        Self: From<Param>,
    {
        Factor(Self::new(param))
    }

    /// Aggregates the factor using the specified method.
    ///
    /// This method creates a `FactorAgg` instance, which represents an aggregated version of the factor.
    ///
    /// # Arguments
    ///
    /// * `method` - The aggregation method to be applied, specified as a `FactorAggMethod`.
    ///
    /// # Returns
    ///
    /// A `FactorAgg<Self>` instance containing the factor and the specified aggregation method.
    #[inline]
    fn agg(self, method: FactorAggMethod) -> FactorAgg<Self>
    where
        Self: Sized,
    {
        FactorAgg { fac: self, method }
    }
}

/// Trait for retrieving the name of a factor.
///
/// This trait provides a method to get the name of a factor as a String.
/// It is primarily used to obtain a unique identifier for each factor.
pub trait GetName: std::fmt::Debug {
    /// Returns the name of the factor.
    ///
    /// # Returns
    ///
    /// A `String` representing the name of the factor.
    #[inline]
    fn name(&self) -> String {
        format!("{:?}", self)
    }
}

impl<F: std::fmt::Debug + FactorBase> GetName for F {}

/// Trait for factors that can be computed using Polars expressions.

///
/// This trait is implemented by factors that can be expressed and calculated
/// using Polars' lazy expressions. It provides methods to convert the factor
/// into a Polars expression for efficient computation.
pub trait PlFactor: GetName + Send + Sync + 'static {
    /// Attempts to convert the factor into a Polars expression.
    ///
    /// # Returns
    ///
    /// A `Result` containing the Polars `Expr` if successful, or an error if the
    /// conversion fails.
    fn try_expr(&self) -> Result<Expr>;

    /// Converts the factor into a Polars expression.
    ///
    /// This method is a convenience wrapper around `try_expr` that panics on error.
    ///
    /// # Returns
    ///
    /// The Polars `Expr` representing the factor.
    ///
    /// # Panics
    ///
    /// Panics if `try_expr` returns an error.
    #[inline]
    fn expr(&self) -> Expr {
        self.try_expr().unwrap()
    }

    #[inline]
    fn pl_dyn(self) -> Arc<dyn PlFactor>
    where
        Self: Sized,
    {
        Arc::new(self)
    }
}

impl GetName for Arc<dyn PlFactor> {
    #[inline]
    fn name(&self) -> String {
        self.as_ref().name()
    }
}

impl PlFactor for Arc<dyn PlFactor> {
    #[inline]
    fn try_expr(&self) -> Result<Expr> {
        self.as_ref().try_expr()
    }

    #[inline]
    fn pl_dyn(self) -> Arc<dyn PlFactor> {
        self
    }
}

#[derive(Clone)]
pub struct ExprFactor(pub Expr);

impl From<Param> for ExprFactor {
    #[inline]
    fn from(_param: Param) -> Self {
        panic!("ExprFactor::from should not be called directly")
    }
}

impl FactorBase for ExprFactor {
    #[inline]
    fn fac_name() -> Arc<str> {
        "expr".into()
    }

    // #[inline]
    // fn new(_param: impl Into<Param>) -> Self {
    //     panic!("ExprFactor::new should not be called directly")
    // }
}

impl std::fmt::Debug for ExprFactor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "PlExprFactor({})",
            self.0.clone().meta().output_name().unwrap().to_string()
        )
    }
}

impl PlFactor for ExprFactor {
    #[inline]
    fn try_expr(&self) -> Result<Expr> {
        Ok(self.0.clone())
    }
}

/// Trait for factors that can be computed directly from a DataFrame.
///
/// This trait is implemented by factors that can be calculated using the data
/// in a DataFrame. It provides a method to evaluate the factor and return a Series.
pub trait TFactor: GetName + Send + Sync + 'static {
    /// Evaluates the factor using the provided DataFrame.
    ///
    /// # Arguments
    ///
    /// * `df` - A reference to the DataFrame containing the necessary data.
    ///
    /// # Returns
    ///
    /// A `Result` containing the computed `Series` if successful, or an error if the
    /// evaluation fails.
    fn eval(&self, df: &DataFrame) -> Result<Series>;

    #[inline]
    fn t_dyn(self) -> Arc<dyn TFactor>
    where
        Self: Sized,
    {
        Arc::new(self)
    }
}

impl GetName for Arc<dyn TFactor> {
    #[inline]
    fn name(&self) -> String {
        self.as_ref().name()
    }
}

impl TFactor for Arc<dyn TFactor> {
    #[inline]
    fn eval(&self, df: &DataFrame) -> Result<Series> {
        self.as_ref().eval(df)
    }

    #[inline]
    fn t_dyn(self) -> Arc<dyn TFactor> {
        self
    }
}

impl From<Expr> for ExprFactor {
    #[inline]
    fn from(expr: Expr) -> Self {
        ExprFactor(expr)
    }
}

pub trait IntoFactor<F: FactorBase> {
    fn fac(self) -> F
    where
        Self: Sized;
}

impl IntoFactor<Factor<ExprFactor>> for Expr {
    #[inline]
    fn fac(self) -> Factor<ExprFactor> {
        Factor(ExprFactor(self))
    }
}
