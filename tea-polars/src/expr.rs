use polars::lazy::dsl::{Expr, GetOutput};
use polars::prelude::{DataType, *};
use tea_strategy::tevec::prelude::*;

use super::series::SeriesExt;

/// Extension trait for Polars expressions providing time series operations.
pub trait ExprExt {
    /// Performs addition between two expressions, ignoring null values.
    ///
    /// This function adds the current expression with another expression,
    /// skipping null values and only adding non-null values together.
    ///
    /// # Arguments
    /// * `other` - The expression to add with.
    ///
    /// # Returns
    /// An expression representing the sum of non-null values.
    fn vadd(self, other: Expr) -> Self;
    /// Calculates the imbalance between two expressions.
    ///
    /// The imbalance is calculated using the formula: (a - b) / (a + b)
    /// where 'a' is the current expression and 'b' is the other expression.
    ///
    /// # Arguments
    /// * `other` - The other expression to compare with.
    ///
    /// # Returns
    /// An expression representing the imbalance between `self` and `other`.
    fn imbalance(self, other: Expr) -> Self;

    /// Performs a protected division operation.
    ///
    /// This function divides the current expression by another expression,
    /// with protection against division by zero.
    ///
    /// # Arguments
    /// * `other` - The expression to divide by.
    ///
    /// # Returns
    /// An expression representing the result of the protected division.
    fn protect_div(self, other: Expr) -> Self;

    /// Winsorizes  using the specified method.
    ///
    /// # Arguments
    ///
    /// * `method` - The winsorization method to use (Quantile, Median, or Sigma).
    /// * `method_params` - Optional parameter specific to the chosen method:
    ///   - For Quantile: The quantile value (default: 0.01).
    ///   - For Median: The number of MADs to use for clipping (default: 3).
    ///   - For Sigma: The number of standard deviations to use for clipping (default: 3).
    fn winsorize(self, method: WinsorizeMethod, method_params: Option<f64>) -> Self;

    /// Calculates the exponentially weighted moving average.
    ///
    /// # Arguments
    /// * `window` - The size of the moving window.
    /// * `min_periods` - The minimum number of observations in window required to have a value.
    fn ts_ewm(self, window: usize, min_periods: Option<usize>) -> Self;

    /// Calculates the rolling skewness.
    ///
    /// # Arguments
    /// * `window` - The size of the moving window.
    /// * `min_periods` - The minimum number of observations in window required to have a value.
    fn ts_skew(self, window: usize, min_periods: Option<usize>) -> Self;

    /// Calculates the rolling kurtosis.
    ///
    /// # Arguments
    /// * `window` - The size of the moving window.
    /// * `min_periods` - The minimum number of observations in window required to have a value.
    fn ts_kurt(self, window: usize, min_periods: Option<usize>) -> Self;

    /// Calculates the rolling rank.
    ///
    /// # Arguments
    /// * `window` - The size of the moving window.
    /// * `min_periods` - The minimum number of observations in window required to have a value.
    /// * `pct` - If true, compute percentage rank.
    /// * `rev` - If true, compute reverse rank.
    fn ts_rank(self, window: usize, min_periods: Option<usize>, pct: bool, rev: bool) -> Self;

    /// Calculates the rolling z-score.
    ///
    /// # Arguments
    /// * `window` - The size of the moving window.
    /// * `min_periods` - The minimum number of observations in window required to have a value.
    fn ts_zscore(self, window: usize, min_periods: Option<usize>) -> Self;

    /// Calculates the rolling regression beta coefficient.
    ///
    /// # Arguments
    /// * `x` - The independent variable expression.
    /// * `window` - The size of the moving window.
    /// * `min_periods` - The minimum number of observations in window required to have a value.
    fn ts_regx_beta(self, x: Expr, window: usize, min_periods: Option<usize>) -> Self;

    /// Cuts the data into bins and labels them.
    ///
    /// # Arguments
    /// * `bin` - An expression defining the bin edges.
    /// * `labels` - An expression defining the labels for each bin.
    /// * `right` - Whether the intervals should be closed on the right (and open on the left) or vice versa. Default is true.
    /// * `add_bounds` - Whether to add the minimum and maximum of the data as explicit bin edges. Default is false.
    ///
    /// # Returns
    /// An expression representing the binned and labeled data.
    fn tcut(self, bin: Expr, labels: Expr, right: Option<bool>, add_bounds: Option<bool>) -> Expr;

    /// Returns the first non-null value in a vector.
    ///
    /// This function is useful for obtaining the first valid observation in a series,
    /// ignoring any null values at the beginning.
    fn vfirst(self) -> Self;

    /// Returns the last non-null value in a vector.
    ///
    /// This function is useful for obtaining the last valid observation in a series,
    /// ignoring any null values at the end.
    fn vlast(self) -> Self;

    /// Calculates the half-life of a factor series using autocorrelation.
    ///
    /// The half-life is defined as the lag at which the autocorrelation drops to 0.5.
    ///
    /// # Arguments
    ///
    /// * `min_periods` - The minimum number of observations required to calculate the half-life.
    ///   If None, defaults to half the length of the series.
    fn half_life(self, min_periods: Option<usize>) -> Self;
}

impl ExprExt for Expr {
    #[inline]
    fn vadd(self, other: Expr) -> Self {
        use polars::lazy::dsl::sum_horizontal;
        let res = when(self.clone().is_not_null().and(other.clone().is_not_null()))
            .then(self.clone() + other.clone())
            .otherwise(NULL.lit());
        when(res.clone().is_null())
            .then(sum_horizontal(&[self, other], true).unwrap())
            .otherwise(res)
    }

    #[inline]
    fn imbalance(self, other: Expr) -> Self {
        (self.clone() - other.clone()).protect_div(self + other)
    }

    #[inline]
    fn protect_div(self, other: Expr) -> Self {
        when(other.clone().neq(0.lit()))
            .then(self.cast(DataType::Float64) / other)
            .otherwise(NULL.lit())
    }

    #[inline]
    fn winsorize(self, method: WinsorizeMethod, method_params: Option<f64>) -> Self {
        self.apply(
            move |s| {
                s.as_materialized_series()
                    .winsorize(method, method_params)
                    .map(|s| Some(s.into_column()))
                    .map_err(|e| PolarsError::ComputeError(e.to_string().into()))
            },
            GetOutput::float_type(),
        )
    }

    #[inline]
    fn ts_ewm(self, window: usize, min_periods: Option<usize>) -> Self {
        self.apply(
            move |s| {
                Ok(Some(
                    s.as_materialized_series()
                        .ts_ewm(window, min_periods)
                        .into_column(),
                ))
            },
            GetOutput::float_type(),
        )
    }

    #[inline]
    fn ts_skew(self, window: usize, min_periods: Option<usize>) -> Self {
        self.apply(
            move |s| {
                Ok(Some(
                    s.as_materialized_series()
                        .ts_skew(window, min_periods)
                        .into_column(),
                ))
            },
            GetOutput::float_type(),
        )
    }

    #[inline]
    fn ts_kurt(self, window: usize, min_periods: Option<usize>) -> Self {
        self.apply(
            move |s| {
                Ok(Some(
                    s.as_materialized_series()
                        .ts_kurt(window, min_periods)
                        .into_column(),
                ))
            },
            GetOutput::float_type(),
        )
    }

    #[inline]
    fn ts_rank(self, window: usize, min_periods: Option<usize>, pct: bool, rev: bool) -> Self {
        self.apply(
            move |s| {
                Ok(Some(
                    s.as_materialized_series()
                        .ts_rank(window, min_periods, pct, rev)
                        .into_column(),
                ))
            },
            GetOutput::float_type(),
        )
    }

    #[inline]
    fn ts_zscore(self, window: usize, min_periods: Option<usize>) -> Self {
        self.apply(
            move |s| {
                Ok(Some(
                    s.as_materialized_series()
                        .ts_zscore(window, min_periods)
                        .into_column(),
                ))
            },
            GetOutput::float_type(),
        )
    }

    fn ts_regx_beta(self, x: Expr, window: usize, min_periods: Option<usize>) -> Self {
        self.apply_many(
            move |series_slice| {
                let y = series_slice[0].as_materialized_series();
                let x = series_slice[1].as_materialized_series();
                Ok(Some(y.ts_regx_beta(x, window, min_periods).into_column()))
            },
            &[x],
            GetOutput::map_dtypes(|dtypes| {
                Ok(match dtypes[0] {
                    DataType::Float32 => DataType::Float32,
                    _ => DataType::Float64,
                })
            }),
        )
    }

    fn tcut(self, bin: Expr, labels: Expr, right: Option<bool>, add_bounds: Option<bool>) -> Expr {
        self.apply_many(
            move |series_slice| {
                let s = series_slice[0].as_materialized_series();
                let bin = series_slice[1].as_materialized_series();
                let labels = series_slice[2].as_materialized_series();
                s.tcut(bin, labels, right, add_bounds)
                    .map(|s| Some(s.into_column()))
                    .map_err(|e| PolarsError::ComputeError(e.to_string().into()))
            },
            &[bin, labels],
            GetOutput::from_type(DataType::Float64),
        )
    }

    fn vfirst(self) -> Self {
        self.apply(
            |s| {
                Series::from_any_values_and_dtype(
                    s.name().clone(),
                    &[s.as_materialized_series().vfirst()],
                    s.dtype(),
                    false,
                )
                .map(|s| Some(s.into_column()))
            },
            GetOutput::same_type(),
        )
        .get(0)
    }

    fn vlast(self) -> Self {
        self.apply(
            |s| {
                Series::from_any_values_and_dtype(
                    s.name().clone(),
                    &[s.as_materialized_series().vlast()],
                    s.dtype(),
                    false,
                )
                .map(|s| Some(s.into_column()))
            },
            GetOutput::same_type(),
        )
        .get(0)
    }

    fn half_life(self, min_periods: Option<usize>) -> Self {
        self.apply(
            move |s| {
                Ok(Some(
                    std::iter::once(Some(
                        s.as_materialized_series().half_life(min_periods) as i32
                    ))
                    .collect::<Series>()
                    .into_column(),
                ))
            },
            GetOutput::from_type(DataType::Int32),
        )
    }
}

pub fn where_(cond: impl Into<Expr>, then: impl Into<Expr>, otherwise: impl Into<Expr>) -> Expr {
    when(cond).then(then).otherwise(otherwise)
}
