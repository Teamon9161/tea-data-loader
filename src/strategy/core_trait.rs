use anyhow::{bail, Result};
use polars::prelude::*;

use crate::prelude::{GetName, Params};
/// Defines the base structure for a strategy.
///
/// This trait is essential for all strategies, providing methods for naming and creation.
pub trait StrategyBase: Sized {
    /// Returns the name of the strategy.
    ///
    /// This method should return a unique identifier for the strategy.
    fn strategy_name() -> Arc<str>;

    /// Creates a new instance of the strategy with the given parameters.
    ///
    /// # Arguments
    ///
    /// * `params` - The parameters for the strategy, which can be converted into `Params`.
    ///
    /// # Returns
    ///
    /// Returns a new instance of the strategy initialized with the provided parameters.
    fn new<P: Into<Params>>(params: P) -> Self;
}

/// Defines the core functionality for a strategy.
pub trait Strategy: GetName + Send + Sync + 'static {
    /// Evaluates the strategy on a given factor series, applying filters.
    ///
    /// # Arguments
    ///
    /// * `fac` - The factor series to evaluate.
    /// * `filters` - DataFrame containing filter conditions with four columns in the following order:
    ///   1. long_open_cond (whether long positions can be opened)
    ///   2. long_close_cond (whether long positions should be closed)
    ///   3. short_open_cond (whether short positions can be opened)
    ///   4. short_close_cond (whether short positions should be closed)
    ///
    /// # Returns
    ///
    /// A `Result` containing the evaluated Series or an error.
    fn eval_to_fac(&self, _fac: &Series, _filters: Option<DataFrame>) -> Result<Series> {
        bail!("eval_to_fac is not implemented for {}", self.name())
    }

    /// Evaluates the strategy on a given factor within a DataFrame, optionally applying filters.
    ///
    /// # Arguments
    ///
    /// * `fac` - The name of the factor column in the DataFrame.
    /// * `df` - The DataFrame containing the factor and other relevant data.
    /// * `filters` - Optional array of four filter expressions. The order of the expressions should be:
    ///   1. long_open_cond (whether long positions can be opened)
    ///   2. long_close_cond (whether long positions should be closed)
    ///   3. short_open_cond (whether short positions can be opened)
    ///   4. short_close_cond (whether short positions should be closed)
    ///     
    /// Unlike [`eval_to_fac`](Strategy::eval_to_fac), these are Polars `Expr` objects instead of DataFrame columns.
    ///     
    /// However, the order of the filters must be consistent with `eval_to_fac`.
    ///
    /// # Returns
    ///
    /// A `Result` containing the evaluated Series or an error.
    ///
    /// This method converts the `Expr` filters to a DataFrame and then calls [`eval_to_fac`](Strategy::eval_to_fac).
    fn eval(&self, fac: &str, df: &DataFrame, filters: Option<[Expr; 4]>) -> Result<Series> {
        let fac = df.column(fac)?.clone();
        if let Some(filters) = filters {
            let filters = [
                filters[0].clone().alias("__long_open"),
                filters[1].clone().alias("__long_close"),
                filters[2].clone().alias("__short_open"),
                filters[3].clone().alias("__short_close"),
            ];
            let filters = df.clone().lazy().select(filters).collect()?;
            self.eval_to_fac(&fac, Some(filters))
        } else {
            self.eval_to_fac(&fac, None)
        }
    }
}
