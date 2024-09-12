use polars::lazy::dsl::{Expr, GetOutput};
use polars::prelude::{DataType, *};

use crate::export::tevec::prelude::*;
/// Extension trait for Series providing additional functionality.
pub trait SeriesExt {
    /// Casts the Series to Float64 type.
    ///
    /// # Returns
    /// A Result containing the casted Series or an error.
    fn cast_f64(&self) -> Result<Series>;

    /// Casts the Series to Boolean type.
    ///
    /// # Returns
    /// A Result containing the casted Series or an error.
    fn cast_bool(&self) -> Result<Series>;

    /// Casts the Series to Float32 type.
    ///
    /// # Returns
    /// A Result containing the casted Series or an error.
    fn cast_f32(&self) -> Result<Series>;

    /// Winsorizes the series using the specified method.
    ///
    /// # Arguments
    ///
    /// * `method` - The winsorization method to use (Quantile, Median, or Sigma).
    /// * `method_params` - Optional parameter specific to the chosen method:
    ///   - For Quantile: The quantile value (default: 0.01).
    ///   - For Median: The number of MADs to use for clipping (default: 3).
    ///   - For Sigma: The number of standard deviations to use for clipping (default: 3).
    fn winsorize(&self, method: WinsorizeMethod, method_params: Option<f64>) -> Result<Series>;

    /// Calculates the exponentially weighted moving average.
    ///
    /// # Arguments
    /// * `window` - The size of the moving window.
    /// * `min_periods` - The minimum number of observations in window required to have a value.
    ///
    /// # Returns
    /// A new Series with the calculated values.
    fn ts_ewm(&self, window: usize, min_periods: Option<usize>) -> Self;

    /// Calculates the rolling skewness.
    ///
    /// # Arguments
    /// * `window` - The size of the moving window.
    /// * `min_periods` - The minimum number of observations in window required to have a value.
    ///
    /// # Returns
    /// A new Series with the calculated values.
    fn ts_skew(&self, window: usize, min_periods: Option<usize>) -> Self;

    /// Calculates the rolling kurtosis.
    ///
    /// # Arguments
    /// * `window` - The size of the moving window.
    /// * `min_periods` - The minimum number of observations in window required to have a value.
    ///
    /// # Returns
    /// A new Series with the calculated values.
    fn ts_kurt(&self, window: usize, min_periods: Option<usize>) -> Self;

    /// Calculates the rolling rank.
    ///
    /// # Arguments
    /// * `window` - The size of the moving window.
    /// * `min_periods` - The minimum number of observations in window required to have a value.
    /// * `pct` - If true, compute percentage rank.
    /// * `rev` - If true, compute reverse rank.
    ///
    /// # Returns
    /// A new Series with the calculated values.
    fn ts_rank(&self, window: usize, min_periods: Option<usize>, pct: bool, rev: bool) -> Self;

    /// Calculates the rolling z-score.
    ///
    /// # Arguments
    /// * `window` - The size of the moving window.
    /// * `min_periods` - The minimum number of observations in window required to have a value.
    ///
    /// # Returns
    /// A new Series with the calculated values.
    fn ts_zscore(&self, window: usize, min_periods: Option<usize>) -> Self;

    /// Calculates the rolling regression beta coefficient.
    ///
    /// # Arguments
    /// * `x` - The independent variable Series.
    /// * `window` - The size of the moving window.
    /// * `min_periods` - The minimum number of observations in window required to have a value.
    ///
    /// # Returns
    /// A new Series with the calculated beta coefficients.
    fn ts_regx_beta(&self, x: &Series, window: usize, min_periods: Option<usize>) -> Self;
}

impl SeriesExt for Series {
    #[inline]
    fn cast_f64(&self) -> Result<Series> {
        if let DataType::Float64 = self.dtype() {
            Ok(self.clone())
        } else {
            Ok(Series::cast(self, &DataType::Float64)?)
        }
    }

    #[inline]
    fn cast_bool(&self) -> Result<Series> {
        if let DataType::Boolean = self.dtype() {
            Ok(self.clone())
        } else {
            Ok(Series::cast(self, &DataType::Boolean)?)
        }
    }

    #[inline]
    fn cast_f32(&self) -> Result<Series> {
        if let DataType::Float32 = self.dtype() {
            Ok(self.clone())
        } else {
            Ok(Series::cast(self, &DataType::Float32)?)
        }
    }

    fn winsorize(&self, method: WinsorizeMethod, method_params: Option<f64>) -> Result<Series> {
        let res: Series = match self.dtype() {
            DataType::Float64 => {
                let ca: Float64Chunked = self
                    .f64()
                    .unwrap()
                    .winsorize(method, method_params)?
                    .map(IsNone::to_opt)
                    .collect_trusted_vec1();
                ca.into_series()
            },
            DataType::Float32 => {
                let ca: Float32Chunked = self
                    .f32()
                    .unwrap()
                    .winsorize(method, method_params)?
                    .map(|v| v.as_opt().map(|v| *v as f32))
                    .collect_trusted_vec1();
                ca.into_series()
            },
            DataType::Int64 => {
                let ca: Float64Chunked = self
                    .i64()
                    .unwrap()
                    .winsorize(method, method_params)?
                    .map(IsNone::to_opt)
                    .collect_trusted_vec1();
                ca.into_series()
            },
            DataType::Int32 => {
                let ca: Float64Chunked = self
                    .i32()
                    .unwrap()
                    .winsorize(method, method_params)?
                    .map(IsNone::to_opt)
                    .collect_trusted_vec1();
                ca.into_series()
            },
            _ => bail!("unsupported data type in winsorize"),
        };
        Ok(res)
    }

    fn ts_ewm(&self, window: usize, min_periods: Option<usize>) -> Self {
        let res: Series = match self.dtype() {
            DataType::Float64 => {
                let ca: Float64Chunked = self.f64().unwrap().ts_vewm(window, min_periods);
                ca.into_series()
            },
            DataType::Float32 => {
                let ca: Float32Chunked = self.f32().unwrap().ts_vewm(window, min_periods);
                ca.into_series()
            },
            DataType::Int64 => {
                let ca: Float64Chunked = self.i64().unwrap().ts_vewm(window, min_periods);
                ca.into_series()
            },
            DataType::Int32 => {
                let ca: Float64Chunked = self.i32().unwrap().ts_vewm(window, min_periods);
                ca.into_series()
            },
            _ => panic!("unsupported data type"),
        };
        res
    }

    fn ts_skew(&self, window: usize, min_periods: Option<usize>) -> Self {
        let res: Series = match self.dtype() {
            DataType::Float64 => {
                let ca: Float64Chunked = self.f64().unwrap().ts_vskew(window, min_periods);
                ca.into_series()
            },
            DataType::Float32 => {
                let ca: Float32Chunked = self.f32().unwrap().ts_vskew(window, min_periods);
                ca.into_series()
            },
            DataType::Int64 => {
                let ca: Float64Chunked = self.i64().unwrap().ts_vskew(window, min_periods);
                ca.into_series()
            },
            DataType::Int32 => {
                let ca: Float64Chunked = self.i32().unwrap().ts_vskew(window, min_periods);
                ca.into_series()
            },
            _ => panic!("unsupported data type"),
        };
        res
    }

    fn ts_kurt(&self, window: usize, min_periods: Option<usize>) -> Self {
        let res: Series = match self.dtype() {
            DataType::Float64 => {
                let ca: Float64Chunked = self.f64().unwrap().ts_vkurt(window, min_periods);
                ca.into_series()
            },
            DataType::Float32 => {
                let ca: Float32Chunked = self.f32().unwrap().ts_vkurt(window, min_periods);
                ca.into_series()
            },
            DataType::Int64 => {
                let ca: Float64Chunked = self.i64().unwrap().ts_vkurt(window, min_periods);
                ca.into_series()
            },
            DataType::Int32 => {
                let ca: Float64Chunked = self.i32().unwrap().ts_vkurt(window, min_periods);
                ca.into_series()
            },
            _ => panic!("unsupported data type"),
        };
        res
    }

    fn ts_rank(&self, window: usize, min_periods: Option<usize>, pct: bool, rev: bool) -> Self {
        let res: Series = match self.dtype() {
            DataType::Float64 => {
                let ca: Float64Chunked =
                    self.f64().unwrap().ts_vrank(window, min_periods, pct, rev);
                ca.into_series()
            },
            DataType::Float32 => {
                let ca: Float32Chunked =
                    self.f32().unwrap().ts_vrank(window, min_periods, pct, rev);
                ca.into_series()
            },
            DataType::Int64 => {
                let ca: Float64Chunked =
                    self.i64().unwrap().ts_vrank(window, min_periods, pct, rev);
                ca.into_series()
            },
            DataType::Int32 => {
                let ca: Float64Chunked =
                    self.i32().unwrap().ts_vrank(window, min_periods, pct, rev);
                ca.into_series()
            },
            _ => panic!("unsupported data type"),
        };
        res
    }

    fn ts_zscore(&self, window: usize, min_periods: Option<usize>) -> Self {
        let res: Series = match self.dtype() {
            DataType::Float64 => {
                let ca: Float64Chunked = self.f64().unwrap().ts_vzscore(window, min_periods);
                ca.into_series()
            },
            DataType::Float32 => {
                let ca: Float32Chunked = self.f32().unwrap().ts_vzscore(window, min_periods);
                ca.into_series()
            },
            DataType::Int64 => {
                let ca: Float64Chunked = self.i64().unwrap().ts_vzscore(window, min_periods);
                ca.into_series()
            },
            DataType::Int32 => {
                let ca: Float64Chunked = self.i32().unwrap().ts_vzscore(window, min_periods);
                ca.into_series()
            },
            _ => panic!("unsupported data type"),
        };
        res
    }

    fn ts_regx_beta(&self, x: &Series, window: usize, min_periods: Option<usize>) -> Self {
        let res: Series = match self.dtype() {
            DataType::Float64 => {
                let ca: Float64Chunked = self.f64().unwrap().ts_vregx_beta(
                    x.cast_f64().unwrap().f64().unwrap(),
                    window,
                    min_periods,
                );
                ca.into_series()
            },
            DataType::Float32 => {
                let ca: Float32Chunked = self.f32().unwrap().ts_vregx_beta(
                    x.cast_f32().unwrap().f32().unwrap(),
                    window,
                    min_periods,
                );
                ca.into_series()
            },
            DataType::Int64 => {
                let ca: Float64Chunked = self.i64().unwrap().ts_vregx_beta(
                    x.cast_f64().unwrap().f64().unwrap(),
                    window,
                    min_periods,
                );
                ca.into_series()
            },
            DataType::Int32 => {
                let ca: Float64Chunked = self.i32().unwrap().ts_vregx_beta(
                    x.cast_f64().unwrap().f64().unwrap(),
                    window,
                    min_periods,
                );
                ca.into_series()
            },
            _ => panic!("unsupported data type"),
        };
        res
    }
}

/// Extension trait for Polars expressions providing time series operations.
pub trait ExprExt {
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
}

impl ExprExt for Expr {
    #[inline]
    fn imbalance(self, other: Expr) -> Self {
        when((self.clone() + other.clone()).gt(0.lit()))
            .then((self.clone() - other.clone()) / (self + other))
            .otherwise(NULL.lit())
    }

    #[inline]
    fn protect_div(self, other: Expr) -> Self {
        when(other.clone().lt(0.lit()))
            .then(self / other)
            .otherwise(NULL.lit())
    }

    #[inline]
    fn winsorize(self, method: WinsorizeMethod, method_params: Option<f64>) -> Self {
        self.apply(
            move |s| {
                s.winsorize(method, method_params)
                    .map(Some)
                    .map_err(|e| PolarsError::ComputeError(e.to_string().into()))
            },
            GetOutput::float_type(),
        )
    }

    #[inline]
    fn ts_ewm(self, window: usize, min_periods: Option<usize>) -> Self {
        self.apply(
            move |s| Ok(Some(s.ts_ewm(window, min_periods))),
            GetOutput::float_type(),
        )
    }

    #[inline]
    fn ts_skew(self, window: usize, min_periods: Option<usize>) -> Self {
        self.apply(
            move |s| Ok(Some(s.ts_skew(window, min_periods))),
            GetOutput::float_type(),
        )
    }

    #[inline]
    fn ts_kurt(self, window: usize, min_periods: Option<usize>) -> Self {
        self.apply(
            move |s| Ok(Some(s.ts_kurt(window, min_periods))),
            GetOutput::float_type(),
        )
    }

    #[inline]
    fn ts_rank(self, window: usize, min_periods: Option<usize>, pct: bool, rev: bool) -> Self {
        self.apply(
            move |s| Ok(Some(s.ts_rank(window, min_periods, pct, rev))),
            GetOutput::float_type(),
        )
    }

    #[inline]
    fn ts_zscore(self, window: usize, min_periods: Option<usize>) -> Self {
        self.apply(
            move |s| Ok(Some(s.ts_zscore(window, min_periods))),
            GetOutput::float_type(),
        )
    }

    fn ts_regx_beta(self, x: Expr, window: usize, min_periods: Option<usize>) -> Self {
        self.apply_many(
            move |series_slice| {
                let y = &series_slice[0];
                let x = &series_slice[1];
                Ok(Some(y.ts_regx_beta(x, window, min_periods)))
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
}
