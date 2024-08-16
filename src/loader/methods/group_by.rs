use polars::prelude::*;
use tea_strategy::tevec::prelude::CollectTrustedToVec;

use crate::prelude::*;

const DAILY_COL: &str = "trading_date";

pub struct DataLoaderGroupBy {
    pub dl: DataLoader,
    pub lgbs: Vec<LazyGroupBy>,
    pub last_time: Option<Arc<str>>,
    pub time: Option<Arc<str>>,
}

pub struct GroupByTimeOpt<'a> {
    pub last_time: Option<&'a str>,
    pub time: &'a str,
    pub group_by: Option<&'a [Expr]>,
    pub daily_col: &'a str,
    pub maintain_order: bool,
    pub label: Label,
}

impl Default for GroupByTimeOpt<'_> {
    fn default() -> Self {
        Self {
            last_time: None,
            time: "time",
            group_by: None,
            daily_col: DAILY_COL,
            maintain_order: true,
            label: Label::Left,
        }
    }
}

impl DataLoader {
    #[inline]
    /// group by dynamic frequency
    /// rule: rule to group by, 'daily' or any rule support by polars
    /// last_time: if not None, the value should be the name of time col,
    ///     the last time of each group will be added to the result as a column
    /// time: time column to group by, only work if rule is not daily
    /// group_by: also group by other columns, note that the data should be sorted
    /// kwargs: other arguments for group_by
    pub fn group_by_time(self, rule: &str, opt: GroupByTimeOpt) -> Result<DataLoaderGroupBy> {
        let source = CONFIG.path_finder.type_source[self.typ.as_ref()]
            .as_str()
            .unwrap();
        let closed_window = match source {
            "rq" => ClosedWindow::Right,
            "coin" => ClosedWindow::Left,
            _ => {
                eprintln!(
                    "unsupported source in group_by_time: {}, use Left Closed by default",
                    source
                );
                ClosedWindow::Left
            },
        };
        match rule {
            "daily" => {
                let lgbs = if !opt.maintain_order {
                    self.dfs
                        .iter()
                        .map(|df| df.clone().lazy().group_by([col(opt.daily_col)]))
                        .collect_trusted_to_vec()
                } else {
                    self.dfs
                        .iter()
                        .map(|df| df.clone().lazy().group_by_stable([col(opt.daily_col)]))
                        .collect_trusted_to_vec()
                };
                Ok(DataLoaderGroupBy {
                    dl: self,
                    lgbs,
                    last_time: opt.last_time.map(Into::into),
                    time: Some(opt.daily_col.into()),
                })
            },
            _ => self.group_by_dynamic(
                col(opt.time),
                [],
                DynamicGroupOptions {
                    every: Duration::parse(rule),
                    period: Duration::parse(rule),
                    offset: Duration::parse("0ns"),
                    label: opt.label,
                    closed_window,
                    ..Default::default()
                },
            ),
        }
    }

    #[inline]
    pub fn group_by_stable<E: AsRef<[IE]>, IE: Into<Expr> + Clone>(
        self,
        by: E,
    ) -> DataLoaderGroupBy {
        let by = by.as_ref();
        let lgbs = self
            .dfs
            .iter()
            .map(|df| df.clone().lazy().group_by_stable(by))
            .collect_trusted_to_vec();
        DataLoaderGroupBy {
            dl: self,
            lgbs,
            last_time: None,
            time: None,
        }
    }

    #[inline]
    pub fn group_by<E: AsRef<[IE]>, IE: Into<Expr> + Clone>(self, by: E) -> DataLoaderGroupBy {
        let by = by.as_ref();
        let lgbs = self
            .dfs
            .iter()
            .map(|df| df.clone().lazy().group_by(by))
            .collect_trusted_to_vec();
        DataLoaderGroupBy {
            dl: self,
            lgbs,
            last_time: None,
            time: None,
        }
    }

    #[inline]
    pub fn group_by_dynamic<E: AsRef<[Expr]>>(
        self,
        index_column: Expr,
        group_by: E,
        options: DynamicGroupOptions,
    ) -> Result<DataLoaderGroupBy> {
        let group_by = group_by.as_ref();
        // let time_col = index_column.name();
        let lgbs = self
            .dfs
            .iter()
            .map(|df| {
                df.clone()
                    .lazy()
                    .group_by_dynamic(index_column.clone(), group_by, options.clone())
            })
            .collect_trusted_to_vec();
        let time_col = index_column.meta().output_name()?;
        Ok(DataLoaderGroupBy {
            dl: self,
            lgbs,
            last_time: None,
            time: Some(time_col),
        })
    }
}

impl DataLoaderGroupBy {
    pub fn agg<E: AsRef<[Expr]>>(self, aggs: E) -> DataLoader {
        let aggs = aggs.as_ref();
        let time_col = self.time.as_deref().unwrap();
        let dfs = if let Some(last_time) = &self.last_time {
            if last_time.as_ref() != time_col {
                let aggs: Vec<_> = aggs
                    .iter()
                    .cloned()
                    .chain(std::iter::once(col(last_time).last()))
                    .collect();
                self.lgbs
                    .into_iter()
                    .map(|lgb| lgb.agg(&aggs))
                    .collect_trusted_to_vec()
            } else {
                let aggs: Vec<_> = aggs
                    .iter()
                    .cloned()
                    .chain(std::iter::once(
                        col(last_time).last().name().suffix("_last"),
                    ))
                    .collect();
                self.lgbs
                    .into_iter()
                    .map(|lgb| {
                        lgb.agg(&aggs)
                            .drop([time_col])
                            .rename([last_time.to_string() + "_last"], [last_time])
                    })
                    .collect_trusted_to_vec()
            }
        } else {
            self.lgbs
                .into_iter()
                .map(|lgb| lgb.agg(aggs))
                .collect_trusted_to_vec()
        };
        self.dl.with_dfs(dfs)
    }
}
