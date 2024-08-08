use anyhow::Result;
use polars::lazy::dsl::{cols, ExprEvalExtension};
use polars::prelude::*;

use crate::prelude::*;

#[derive(Debug, Clone)]
pub struct EvaluateOpt<'a> {
    pub time: &'a str,
    pub freq: &'a str,
    pub rf: f64,
    pub plot: bool,
    pub plot_opt: PlotOpt<'a>,
}

impl Default for EvaluateOpt<'_> {
    #[inline]
    fn default() -> Self {
        Self {
            time: "time",
            freq: "1d",
            rf: 0.0,
            plot: false,
            plot_opt: PlotOpt {
                x: "time",
                ..Default::default()
            },
        }
    }
}

impl Frame {
    pub fn ret_evaluate<S: AsRef<str>>(
        mut self,
        eval_cols: Option<&[S]>,
        opt: EvaluateOpt,
    ) -> Result<Self> {
        let strategies: Vec<Arc<str>> = eval_cols
            .map(|cols| cols.iter().map(|s| s.as_ref().into()).collect())
            .unwrap_or_else(|| {
                self.schema()
                    .unwrap()
                    .iter_names()
                    .filter_map(|name| {
                        if name != opt.time {
                            Some(name.as_str().into())
                        } else {
                            None
                        }
                    })
                    .collect::<Vec<_>>()
            });
        let ret_df = self.with_column(cols(&strategies).fill_nan(lit(NULL)))?;
        let equity_curves: Vec<String> = strategies
            .iter()
            .map(|s| format!("{}{}", s, "_equity_curve"))
            .collect();
        if opt.plot {
            ret_df
                .clone()
                .with_column(cols(&strategies).fill_null(lit(0.)).cum_sum(false))?
                .collect()?
                .into_frame()
                .plot(&strategies, &opt.plot_opt)?;
        }
        // calculate equity curve
        let ret_df = ret_df
            .with_column(
                (cols(&strategies) + lit(1.))
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
            "策略" => strategies.iter().map(|s| s.as_ref()).collect::<Vec<_>>(),
            "年化收益率" => strategies.iter().map(|s| ret_df[s.as_ref()].mean().map(|v| v * n)).collect::<Float64Chunked>(),
            "年化标准差" => strategies.iter().map(|s| ret_df[s.as_ref()].std(1).map(|v| v * (n.sqrt()))).collect::<Float64Chunked>(),
        )?;
        result.with_column(
            ((&result["年化收益率"] - opt.rf) / result["年化标准差"].clone())?
                .with_name("夏普比率"),
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
                let series = &ret_df[s.as_ref()];
                Ok(Some(series.gt_eq(0.)?.sum().map(|v| v as f64).unwrap_or(0.) / (series.len() - series.null_count()) as f64))
            }).try_collect::<Float64Chunked>()?,
            "最大回撤" => &ret_df
                .clone()
                .lazy()
                .select([drawdown_expr.abs().max()])
                .collect()?
                .transpose(None, None)?[0],
            "最大回撤开始时间" => &ret_df.clone().lazy().select(drawdown_start_date_idx_df.get_columns().iter().map(|s| col(opt.time).gather(lit(s.clone())).alias(s.name())).collect::<Vec<_>>()).collect()?[0],
            "最大回撤结束时间" => &ret_df.clone().lazy().select(drawdown_end_date_idx_df.get_columns().iter().map(|s| col(opt.time).gather(lit(s.clone())).alias(s.name())).collect::<Vec<_>>()).collect()?[0],
        )?;
        result.hstack_mut(res_expand.get_columns())?;
        Ok(result.into())
    }

    pub fn equity_evaluate<S: AsRef<str>>(
        mut self,
        eval_cols: Option<&[S]>,
        opt: EvaluateOpt,
    ) -> Result<Self> {
        let strategies: Vec<Arc<str>> = eval_cols
            .map(|cols| cols.iter().map(|s| s.as_ref().into()).collect())
            .unwrap_or_else(|| {
                self.schema()
                    .unwrap()
                    .iter_names()
                    .filter_map(|name| {
                        if name != opt.time {
                            Some(name.as_str().into())
                        } else {
                            None
                        }
                    })
                    .collect::<Vec<_>>()
            });
        let df =
            self.with_column(cols(&strategies) / cols(&strategies).shift(lit(1.)) - lit(1.))?;
        df.ret_evaluate(Some(&strategies), opt)
    }
}