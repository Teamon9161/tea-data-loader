use std::iter::once;

use anyhow::ensure;
use polars::prelude::*;
use rayon::prelude::*;
// use smartstring::alias::String;
use tea_strategy::tevec::prelude::*;

use super::summary::Summary;
use super::utils::{get_ts_group, infer_label_periods, stable_corr};
use crate::prelude::*;
use crate::POOL;

#[derive(Clone)]
pub struct FacAnalysis {
    pub dl: DataLoader,
    facs: Vec<String>,
    labels: Vec<String>,
    label_periods: Vec<usize>,
    pub summary: Summary,
}

impl DataLoader {
    pub fn fac_analyse(
        self,
        facs: &[impl AsRef<str>],
        labels: &[impl AsRef<str>],
        // label_periods: &[usize],
        drop_peak: bool,
    ) -> Result<FacAnalysis> {
        let facs = facs.iter().map(|s| s.as_ref().into()).collect();
        let labels: Vec<String> = labels.iter().map(|s| s.as_ref().into()).collect();
        // let label_periods = label_periods.to_vec();
        let label_periods = infer_label_periods(&labels);
        ensure!(
            label_periods.len() == labels.len(),
            "label_periods and labels must have the same length"
        );
        FacAnalysis::new(self, facs, labels, label_periods, drop_peak)
    }
}

impl FacAnalysis {
    pub fn new(
        dl: DataLoader,
        facs: Vec<String>,
        labels: Vec<String>,
        label_periods: Vec<usize>,
        drop_peak: bool,
    ) -> Result<Self> {
        let dl = if drop_peak {
            // 去除极端值
            dl.with_column(
                cols(&facs)
                    .fill_nan(NULL.lit())
                    .winsorize(WinsorizeMethod::Quantile, Some(0.01)),
            )?
        } else {
            dl
        };
        let summary = Summary::new(facs.clone(), labels.clone());
        Ok(Self {
            dl,
            facs,
            labels,
            label_periods,
            summary,
        })
    }

    pub fn with_ic_overall(mut self, method: CorrMethod) -> Result<Self> {
        let ic_vec: Vec<_> = POOL
            .install(|| {
                self.facs.par_iter().map(|fac| {
                    self.dl
                        .clone()
                        .select(
                            self.labels
                                .iter()
                                .map(|label| stable_corr(col(fac), col(label), method).alias(label))
                                .chain(once(dsl::len().alias("count")))
                                .collect::<Vec<_>>(),
                        )
                        .unwrap()
                        .collect(true)
                        .unwrap()
                })
            })
            .collect();
        self.summary = self
            .summary
            .with_symbol_ic(
                ic_vec
                    .iter()
                    .map(|ic| ic.clone().drop(["count"]))
                    .collect::<Result<Vec<_>>>()?,
            )
            .with_ic_overall(
                ic_vec
                    .into_iter()
                    .map(|ic| {
                        ic.dfs.horizontal_agg(
                            &self.labels,
                            // vec![AggMethod::Mean; self.labels.len()],
                            vec![AggMethod::WeightMean("count".into()); self.labels.len()],
                        )
                    })
                    .collect::<Result<Vec<_>>>()?,
            );
        Ok(self)
    }

    pub fn with_ts_ic(mut self, rule: &str, method: CorrMethod) -> Result<Self> {
        // 每一个factor是一个loader，loader里面是不同symbol的ic
        let daily_col = self.dl.daily_col();
        let symbol_ts_ic = POOL.install(|| {
            self.facs.par_iter().map(|fac| {
                self.dl
                    .clone()
                    .group_by_time(
                        rule,
                        GroupByTimeOpt {
                            time: daily_col,
                            ..Default::default()
                        },
                    )?
                    .agg([stable_corr(cols(&self.labels), col(fac), method)])
                    .collect(true)?
                    .align([col(daily_col)], None)?
                    .collect(true)
            })
        });
        // .collect::<Result<Vec<_>>>()?;
        let ts_ic = symbol_ts_ic
            .map(|dl| {
                dl?.dfs.horizontal_agg(
                    once(daily_col).chain(self.labels.iter().map(|s| s.as_ref())),
                    once(AggMethod::First).chain(vec![AggMethod::Mean; self.labels.len()]),
                )
            })
            .collect::<Result<Vec<_>>>()?;
        self.summary = self.summary.with_ts_ic(ts_ic);
        Ok(self)
    }

    pub fn with_ts_group_ret(mut self, group: usize) -> Result<Self> {
        let daily_col = self.dl.daily_col();
        // 日频的平均分组下期收益
        // 尚未在品种间进行平均
        let symbol_ts_group_rets = POOL
            .install(|| {
                self.facs.par_iter().map(|fac| {
                    let group_expr = get_ts_group(col(fac), group).alias("group");
                    self.dl
                        .clone()
                        // 按照日频聚合分组收益
                        .group_by([col(daily_col), group_expr])
                        .agg(
                            self.label_periods
                                .iter()
                                .zip(&self.labels)
                                .map(|(n, label)| (col(label) / (*n as f64).lit()).sum())
                                .collect::<Vec<_>>(),
                        )
                        .filter(col("group").is_not_null())?
                        .sort(["group", daily_col], SortMultipleOptions::default())?
                        .collect(true)?
                        .align([col("group"), col(daily_col)], None)?
                        .collect(true)
                })
            })
            .collect::<Result<Vec<_>>>()?;
        let ts_group_rets = symbol_ts_group_rets
            .iter()
            .map(|tgr| {
                use AggMethod::*;
                tgr.dfs.clone().horizontal_agg(
                    ["group", daily_col]
                        .into_iter()
                        .chain(self.labels.iter().map(|s| s.as_ref())),
                    [First, First]
                        .into_iter()
                        .chain(vec![Mean; self.labels.len()]),
                )
            })
            .collect::<Result<Vec<_>>>()?;
        self.summary = self
            .summary
            .with_symbol_ts_group_rets(symbol_ts_group_rets)
            .with_ts_group_rets(ts_group_rets);
        Ok(self)
    }

    pub fn with_group_ret(mut self, rule: Option<&str>, group: usize) -> Result<Self> {
        let daily_col = self.dl.daily_col();
        if let Some(rule) = rule {
            // 根据某种时间规则聚合后分组
            let symbol_group_rets = POOL
                .install(|| {
                    self.facs.par_iter().map(|fac| {
                        let group_expr = get_ts_group(col(fac), group).alias("group");
                        self.dl
                            .clone()
                            .with_column(group_expr)?
                            .sort(["group", daily_col], Default::default())?
                            .group_by_time(
                                rule,
                                GroupByTimeOpt {
                                    time: daily_col,
                                    group_by: Some(&[col("group")]),
                                    ..Default::default()
                                },
                            )?
                            .agg(
                                [
                                    col(fac).min().alias("min"),
                                    col(fac).max().alias("max"),
                                    col(fac).count().alias("count"),
                                ]
                                .into_iter()
                                .chain(self.labels.iter().map(|n| col(n).mean()))
                                .collect::<Vec<_>>(),
                            )
                            .filter(col("group").is_not_null())?
                            .collect(true)?
                            .align([col("group"), col(daily_col)], None)?
                            .collect(true)
                    })
                })
                .collect::<Result<Vec<_>>>()?;
            let group_rets = symbol_group_rets
                .iter()
                .map(|tgr| {
                    use AggMethod::*;
                    tgr.clone()
                        .group_by_stable(["group"])
                        .agg([col("*").exclude([daily_col]).mean()])
                        .dfs
                        .horizontal_agg(
                            once("group").chain(self.labels.iter().map(|s| s.as_ref())),
                            once(First).chain(vec![Mean; self.labels.len()]),
                        )
                })
                .collect::<Result<Vec<_>>>()?;
            self.summary = self
                .summary
                .with_symbol_group_rets(symbol_group_rets)
                .with_group_rets(group_rets);
        } else {
            // 使用全历史数据直接分组
            let symbol_group_rets = POOL
                .install(|| {
                    self.facs.par_iter().map(|fac| {
                        let group_expr = get_ts_group(col(fac), group).alias("group");
                        self.dl
                            .clone()
                            .group_by([group_expr])
                            .agg(
                                [
                                    col(fac).min().alias("min"),
                                    col(fac).max().alias("max"),
                                    col(fac).count().alias("count"),
                                ]
                                .into_iter()
                                .chain(self.labels.iter().map(|n| col(n).mean()))
                                .collect::<Vec<_>>(),
                            )
                            .filter(col("group").is_not_null())?
                            .sort(["group"], Default::default())?
                            .collect(true)?
                            .align([col("group")], None)?
                            .collect(true)
                    })
                })
                .collect::<Result<Vec<_>>>()?;
            let group_rets = symbol_group_rets
                .iter()
                .map(|tgr| {
                    use AggMethod::*;
                    tgr.dfs.clone().horizontal_agg(
                        once("group").chain(self.labels.iter().map(|s| s.as_ref())),
                        once(First).chain(vec![WeightMean("count".into()); self.labels.len()]),
                    )
                })
                .collect::<Result<Vec<_>>>()?;
            self.summary = self
                .summary
                .with_symbol_group_rets(symbol_group_rets)
                .with_group_rets(group_rets);
        };
        Ok(self)
    }

    pub fn with_half_life(mut self) -> Result<Self> {
        let symbol_half_life = self
            .dl
            .clone()
            .select([cols(&self.facs).half_life(None)])?
            .collect(true)?;
        let half_life = symbol_half_life
            .dfs
            .horizontal_agg(&self.facs, vec![AggMethod::Mean; self.facs.len()])?;
        self.summary = self.summary.with_half_life(half_life);
        Ok(self)
    }
}
