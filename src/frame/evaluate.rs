use std::path::Path;

use anyhow::Result;
use polars::lazy::dsl::{ExprEvalExtension, cols};
use polars::prelude::*;

use crate::prelude::*;

/// Options for evaluating strategies.
#[derive(Debug, Clone)]
pub struct EvaluateOpt<'a> {
    /// The name of the time column.
    pub time: &'a str,
    /// The frequency of the data (e.g., "1d" for daily).
    pub freq: &'a str,
    /// The risk-free rate used in calculations.
    pub rf: f64,
    /// Whether to sort the results.
    pub sort: bool,
    /// Whether to save the results.
    pub save: bool,
    /// The path to save the results, if saving.
    pub save_name: Option<&'a Path>,
    #[cfg(feature = "plot")]
    /// Whether to plot the results.
    pub plot: bool,
    #[cfg(feature = "plot")]
    /// Options for plotting.
    pub plot_opt: PlotOpt<'a>,
}

impl Default for EvaluateOpt<'_> {
    #[inline]
    fn default() -> Self {
        Self {
            time: "time",
            freq: "1d",
            rf: 0.0,
            sort: true,
            save: true,
            save_name: None,
            #[cfg(feature = "plot")]
            plot: false,
            #[cfg(feature = "plot")]
            plot_opt: PlotOpt {
                x: "time",
                ..Default::default()
            },
        }
    }
}

/// Retrieves the strategy column names from a given schema.
///
/// This function determines which columns in the schema represent strategies to be evaluated.
/// It either uses the provided column names or, if none are provided, selects all columns
/// except for the time column.
///
/// # Arguments
///
/// * `schema` - The schema of the data frame.
/// * `time` - The name of the time column.
/// * `eval_cols` - Optional slice of column names to evaluate.
///
/// # Returns
///
/// A vector of `Arc<str>` containing the names of the strategy columns.
fn get_strategy_columns<S: AsRef<str>>(
    schema: &Schema,
    time: &str,
    eval_cols: Option<&[S]>,
) -> Vec<PlSmallStr> {
    eval_cols
        .map(|cols| cols.iter().map(|s| s.as_ref().into()).collect())
        .unwrap_or_else(|| {
            schema
                .iter_names()
                .filter_map(|name| {
                    if name != time {
                        Some(name.as_str().into())
                    } else {
                        None
                    }
                })
                .collect::<Vec<_>>()
        })
}

impl Frame {
    /// Evaluates strategies based on return rates.
    ///
    /// This function calculates various performance metrics for strategies using their return rates.
    ///
    /// # Arguments
    ///
    /// * `eval_cols` - Optional slice of column names representing strategies to evaluate.
    ///   If None, all columns except the time column will be evaluated.
    /// * `opt` - Evaluation options including time column, frequency, risk-free rate, and output preferences.
    ///
    /// # Returns
    ///
    /// A `Result` containing the evaluated `Frame` with performance metrics for each strategy.
    ///
    /// # Performance Metrics
    ///
    /// - Annual Return
    /// - Annual Standard Deviation
    /// - Sharpe Ratio
    /// - Win Rate
    /// - Maximum Drawdown
    /// - Maximum Drawdown Start Time
    /// - Maximum Drawdown End Time
    ///
    /// # Note
    ///
    /// This function assumes that the input data represents return rates of strategies.
    /// For equity-based evaluation, use the `equity_evaluate` function instead.
    ///
    /// # See also
    ///
    /// [`equity_evaluate`](Self::equity_evaluate)
    ///
    /// [`profit_evaluate`](Self::profit_evaluate)
    pub fn ret_evaluate<S: AsRef<str>>(
        mut self,
        eval_cols: Option<&[S]>,
        opt: EvaluateOpt,
    ) -> Result<Self> {
        use crate::utils::column_to_expr;
        let strategies = get_strategy_columns(&self.schema().unwrap(), opt.time, eval_cols);
        let ret_df = self.with_column(cols(strategies.clone()).fill_nan(lit(NULL)))?;
        let equity_curves: Vec<String> = strategies
            .iter()
            .map(|s| format!("{}{}", s, "_equity_curve"))
            .collect();
        #[cfg(feature = "plot")]
        if opt.plot {
            let plot_opt = if opt.plot_opt.x != opt.time {
                opt.plot_opt.with_x(opt.time)
            } else {
                opt.plot_opt
            };
            ret_df
                .clone()
                .with_column(cols(strategies.clone()).fill_null(lit(0.)).cum_sum(false))?
                .collect()?
                .into_frame()
                .plot(&strategies.clone(), &plot_opt)?;
        }
        // calculate equity curve
        let ret_df = ret_df
            .with_column(
                (cols(strategies.clone()) + lit(1.))
                    .cum_prod(false)
                    .name()
                    .suffix("_equity_curve"),
            )?
            .collect()?;
        let freq = Duration::parse(opt.freq);
        assert_eq!(freq.months(), 0, "freq should not be month");
        assert_eq!(freq.weeks(), 0, "freq should not be week");
        let n = Duration::parse("252d").duration_ms() as f64 / freq.duration_ms() as f64;
        let mut result = df!(
            "策略" => strategies.iter().map(|s| s.as_str()).collect::<Vec<_>>(),
            "年化收益率" => strategies.iter().map(|s| ret_df[s.as_ref()].as_materialized_series().mean().map(|v| v * n)).collect::<Float64Chunked>(),
            "年化标准差" => strategies.iter().map(|s| ret_df[s.as_ref()].as_materialized_series().std(1).map(|v| v * (n.sqrt()))).collect::<Float64Chunked>(),
        )?;
        result.with_column(
            ((&result["年化收益率"] - opt.rf)
                .as_materialized_series()
                .protect_div(result["年化标准差"].as_materialized_series().clone()))?
            .with_name("夏普比率".into()),
        )?;
        let drawdown_expr = cols(&equity_curves)
            / cols(&equity_curves).cumulative_eval(col("").max(), 1, false)
            - lit(1.);
        // let drawdown_expr = cols(&equity_curves).cumulative_eval(col("").max(), 1, false) - lit(1.);
        let drawdown_end_date_idx = drawdown_expr.clone().arg_min();
        let drawdown_start_date_idx = cols(&equity_curves)
            .slice(0, drawdown_end_date_idx.clone() + lit(1))
            .arg_max();
        let drawdown_end_date_idx_df = ret_df
            .clone()
            .lazy()
            .select([drawdown_end_date_idx])
            .collect()?
            .transpose(None, None)?;
        let drawdown_start_date_idx_df = ret_df
            .clone()
            .lazy()
            .select([drawdown_start_date_idx])
            .collect()?
            .transpose(None, None)?;
        let res_expand = df!(
            "胜率" => strategies.iter().map(|s| -> Result<Option<f64>> {
                let series = &ret_df[s.as_ref()].as_materialized_series();
                Ok(Some(series.gt_eq(0.)?.sum().map(|v| v as f64).unwrap_or(0.) / (series.len() - series.null_count()) as f64))
            }).collect::<Result<Float64Chunked>>()?,
            "最大回撤" => ret_df
                .clone()
                .lazy()
                .select([drawdown_expr.abs().max()])
                .collect()?
                .transpose(None, None)?[0].as_materialized_series(),
            "最大回撤开始时间" => ret_df.clone().lazy().select(drawdown_start_date_idx_df.get_columns().iter().map(|s| col(opt.time).gather(column_to_expr(s)).alias(s.name().clone())).collect::<Vec<_>>()).collect()?[0].as_materialized_series(),
            "最大回撤结束时间" => ret_df.clone().lazy().select(drawdown_end_date_idx_df.get_columns().iter().map(|s| col(opt.time).gather(column_to_expr(s)).alias(s.name().clone())).collect::<Vec<_>>()).collect()?[0].as_materialized_series(),
        )?;
        result.hstack_mut(res_expand.get_columns())?;
        if opt.sort {
            result.sort_in_place(
                ["夏普比率"],
                SortMultipleOptions::new().with_order_descending(true),
            )?;
        }
        if opt.save {
            let save_path = opt
                .save_name
                .unwrap_or_else(|| Path::new("equity_curve.csv"));
            CsvWriter::new(std::fs::File::create(save_path)?).finish(&mut result)?;
        }
        Ok(result.into())
    }

    /// Evaluates equity-based strategies.
    ///
    /// # Arguments
    ///
    /// * `eval_cols` - Optional slice of column names to evaluate.
    /// * `opt` - Evaluation options.
    ///
    /// # Returns
    ///
    /// A `Result` containing the evaluated `Frame` with performance metrics for each strategy.
    ///
    /// # Performance Metrics
    ///
    /// - Annual Return
    /// - Annual Standard Deviation
    /// - Sharpe Ratio
    /// - Win Rate
    /// - Maximum Drawdown
    /// - Maximum Drawdown Start Time
    /// - Maximum Drawdown End Time
    ///
    /// # See also
    ///
    /// [`ret_evaluate`](Self::ret_evaluate)
    ///
    /// [`profit_evaluate`](Self::profit_evaluate)
    pub fn equity_evaluate<S: AsRef<str>>(
        mut self,
        eval_cols: Option<&[S]>,
        opt: EvaluateOpt,
    ) -> Result<Self> {
        let strategies = get_strategy_columns(&self.schema().unwrap(), opt.time, eval_cols);
        let df = self.with_column(cols(strategies.clone()).pct_change(lit(1)))?;
        df.ret_evaluate(Some(&strategies), opt)
    }

    /// Evaluates profit-based strategies.
    ///
    /// # Arguments
    ///
    /// * `eval_cols` - Optional slice of column names to evaluate.
    /// * `init_cash` - Initial cash amount for each strategy.
    /// * `opt` - Evaluation options.
    ///
    /// # Returns
    ///
    /// A `Result` containing the evaluated `Frame` with performance metrics for each strategy.
    ///
    /// # Performance Metrics
    ///
    /// This method calculates the same performance metrics as `ret_evaluate`:
    /// - Annual Return
    /// - Annual Standard Deviation
    /// - Sharpe Ratio
    /// - Win Rate
    /// - Maximum Drawdown
    /// - Maximum Drawdown Start Time
    /// - Maximum Drawdown End Time
    ///
    /// # See also
    ///
    /// [`ret_evaluate`](Self::ret_evaluate)
    ///
    /// [`equity_evaluate`](Self::equity_evaluate)
    pub fn profit_evaluate<S: AsRef<str>>(
        mut self,
        eval_cols: Option<&[S]>,
        init_cash: f64,
        opt: EvaluateOpt,
    ) -> Result<Self> {
        let strategies = get_strategy_columns(&self.schema().unwrap(), opt.time, eval_cols);
        let df = self.with_column(
            (cols(strategies.clone()).cum_sum(false) + init_cash.lit()).pct_change(1.lit()),
        )?;
        df.ret_evaluate(Some(&strategies), opt)
    }
}
