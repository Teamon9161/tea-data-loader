use anyhow::{bail, Result};
use polars::lazy::dsl::{Expr, GetOutput};
use polars::prelude::{DataType, *};

use tea_strategy::tevec::prelude::*;

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

    fn protect_div(&self, other: Series) -> Result<Series>;

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

    /// Categorize values into bins.
    ///
    /// This function categorizes the values in the Series into bins defined by the `bin` parameter.
    /// It assigns labels to each bin as specified by the `labels` parameter.
    ///
    /// # Arguments
    ///
    /// * `bin` - A Series of bin edges.
    /// * `labels` - A Series of labels for each bin.
    /// * `right` - If true, intervals are closed on the right. If false, intervals are closed on the left.
    /// * `add_bounds` - If true, adds -∞ and +∞ as the first and last bin edges respectively.
    ///
    /// # Returns
    ///
    /// Returns a `Result<Series>` containing the categorized values.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The number of labels doesn't match the number of bins (accounting for `add_bounds`).
    /// - A value falls outside the bin ranges.
    /// - The input Series has an unsupported data type.
    fn tcut(
        &self,
        bin: &Series,
        labels: &Series,
        right: Option<bool>,
        add_bounds: Option<bool>,
    ) -> Result<Series>;

    /// Calculates the valid first non-null value.
    ///
    /// This function calculates the first non-null value in the Series.
    /// If the Series is empty or all values are null, it returns null.
    ///
    /// # Returns
    ///
    /// A new Series with the valid first non-null value.
    fn vfirst(&self) -> AnyValue<'_>;

    /// Calculates the valid last non-null value.
    ///
    /// This function calculates the last non-null value in the Series.
    /// If the Series is empty or all values are null, it returns null.
    ///
    /// # Returns
    ///
    /// A new Series with the valid last non-null value.
    fn vlast(&self) -> AnyValue<'_>;

    /// Calculates the half-life of a factor series using autocorrelation.
    ///
    /// The half-life is defined as the lag at which the autocorrelation drops to 0.5.
    ///
    /// # Arguments
    ///
    /// * `min_periods` - The minimum number of observations required to calculate the half-life.
    ///                   If None, defaults to half the length of the series.
    fn half_life(&self, min_periods: Option<usize>) -> usize;
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

    #[inline]
    fn protect_div(&self, other: Series) -> Result<Series> {
        Ok(LazyFrame::default()
            .select([self.clone().lit().protect_div(other.lit())])
            .collect()?[0]
            .as_materialized_series()
            .clone())
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

    fn tcut(
        &self,
        bin: &Series,
        labels: &Series,
        right: Option<bool>,
        add_bounds: Option<bool>,
    ) -> Result<Series> {
        use DataType::*;
        let name = self.name();
        let right = right.unwrap_or(true);
        let add_bounds = add_bounds.unwrap_or(true);
        let labels_f64 = labels.cast(&Float64)?;
        let labels = labels_f64.f64()?;
        let res: Float64Chunked = match self.dtype() {
            Int32 => self
                .i32()?
                .titer()
                .vcut(bin.cast(&Int32)?.i32()?, labels, right, add_bounds)?
                .try_collect_trusted_vec1()?,
            Int64 => self
                .i64()?
                .titer()
                .vcut(bin.cast(&Int64)?.i64()?, labels, right, add_bounds)?
                .try_collect_trusted_vec1()?,
            Float32 => self
                .f32()?
                .titer()
                .vcut(bin.cast(&Float32)?.f32()?, labels, right, add_bounds)?
                .try_collect_trusted_vec1()?,
            Float64 => self
                .f64()?
                .titer()
                .vcut(bin.cast(&Float64)?.f64()?, labels, right, add_bounds)?
                .try_collect_trusted_vec1()?,
            dtype => bail!(
                "dtype {} not supported for cut, expected Int32, Int64, Float32, Float64.",
                dtype
            ),
        };
        Ok(res.with_name(name.clone()).into_series())
    }

    fn vfirst(&self) -> AnyValue<'_> {
        match self.dtype() {
            DataType::Float64 => self.f64().unwrap().vfirst().into(),
            DataType::Float32 => self.f32().unwrap().vfirst().into(),
            DataType::Int64 => self.i64().unwrap().vfirst().into(),
            DataType::Int32 => self.i32().unwrap().vfirst().into(),
            DataType::Boolean => self.bool().unwrap().vfirst().into(),
            DataType::String => self.str().unwrap().vfirst().into(),
            DataType::Date => self.date().unwrap().vfirst().into(),
            DataType::Datetime(_, _) => self.datetime().unwrap().vfirst().into(),
            dtype => panic!("dtype {} not supported for vfirst", dtype),
        }
    }

    fn vlast(&self) -> AnyValue<'_> {
        match self.dtype() {
            DataType::Float64 => self.f64().unwrap().vlast().into(),
            DataType::Float32 => self.f32().unwrap().vlast().into(),
            DataType::Int64 => self.i64().unwrap().vlast().into(),
            DataType::Int32 => self.i32().unwrap().vlast().into(),
            DataType::Boolean => self.bool().unwrap().vlast().into(),
            DataType::String => self.str().unwrap().vlast().into(),
            DataType::Date => self.date().unwrap().vlast().into(),
            DataType::Datetime(_, _) => self.datetime().unwrap().vlast().into(),
            dtype => panic!("dtype {} not supported for vlast", dtype),
        }
    }

    fn half_life(&self, min_periods: Option<usize>) -> usize {
        match self.dtype() {
            DataType::Float64 => self.f64().unwrap().half_life(min_periods),
            DataType::Float32 => self.f32().unwrap().half_life(min_periods),
            DataType::Int64 => self.i64().unwrap().half_life(min_periods),
            DataType::Int32 => self.i32().unwrap().half_life(min_periods),
            dtype => panic!("dtype {} not supported for half_life", dtype),
        }
    }
}

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
    ///                   If None, defaults to half the length of the series.
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
            .then(sum_horizontal(&[self, other]).unwrap())
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
                Ok(s.tcut(bin, labels, right, add_bounds)
                    .map(|s| Some(s.into_column()))
                    .map_err(|e| PolarsError::ComputeError(e.to_string().into()))?)
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
                    &s.dtype(),
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
                    &s.dtype(),
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
