use anyhow::Result;
use polars::lazy::dsl::Expr;
use polars::prelude::*;

use super::param::Param;

/// Trait defining the base functionality for factors.
///
/// This trait provides two essential methods for factors:
/// - `fac_name`: for obtaining the factor's name
/// - `new`: for creating a new instance of the factor
pub trait FactorBase: Sized {
    /// Returns the name of the factor as an `Arc<str>`.
    fn fac_name() -> Arc<str>;

    /// Creates a new instance of the factor with the given parameter.
    ///
    /// # Arguments
    ///
    /// * `param` - A value that can be converted into a `Param`.
    fn new<P: Into<Param>>(param: P) -> Self;
}

/// Trait for retrieving the name of a factor.
///
/// This trait provides a method to get the name of a factor as a String.
/// It is primarily used to obtain a unique identifier for each factor.
pub trait GetName {
    /// Returns the name of the factor.
    ///
    /// # Returns
    ///
    /// A `String` representing the name of the factor.
    fn name(&self) -> String;
}

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
