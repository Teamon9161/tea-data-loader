use polars::prelude::*;
use tea_strategy::tevec::prelude::CollectTrustedToVec;

use crate::prelude::*;

const DAILY_COL: &str = "trading_date";

/// Represents a DataLoader with grouped data
pub struct DataLoaderGroupBy {
    /// The original DataLoader
    pub dl: DataLoader,
    /// A vector of LazyGroupBy operations
    pub lgbs: Vec<LazyGroupBy>,
    /// Optional last time column name
    pub last_time: Option<Arc<str>>,
    /// Optional time column name
    pub time: Option<PlSmallStr>,
}

/// Options for grouping data by time
pub struct GroupByTimeOpt<'a> {
    /// Optional last time column name
    pub last_time: Option<&'a str>,
    /// Time column name to group by
    pub time: &'a str,
    /// Optional additional columns to group by
    pub group_by: Option<&'a [Expr]>,
    /// Column name for daily grouping
    pub daily_col: &'a str,
    /// Whether to maintain the original order
    pub maintain_order: bool,
    /// Label position for the time window
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
    /// Groups data by dynamic frequency.
    ///
    /// # Arguments
    ///
    /// * `rule` - The grouping rule. Can be 'daily' or any rule supported by Polars.
    /// * `opt` - A `GroupByTimeOpt` struct containing grouping options.
    ///
    /// # Returns
    ///
    /// A `Result` containing a `DataLoaderGroupBy` if successful, or an error otherwise.
    ///
    /// # Details
    ///
    /// This method groups the data based on the specified rule and options:
    ///
    /// - If `rule` is "daily", it groups by the daily column specified in `opt.daily_col`.
    /// - For other rules, it uses Polars' dynamic grouping functionality.
    ///
    /// The method determines the appropriate closed window based on the data source:
    /// - For "rq" source, it uses `ClosedWindow::Right`.
    /// - For "coin" source, it uses `ClosedWindow::Left`.
    /// - For other sources, it defaults to `ClosedWindow::Left` and prints a warning.
    ///
    /// If `opt.maintain_order` is true, it uses stable grouping to maintain the original order.
    #[inline]
    pub fn group_by_time(self, rule: &str, opt: GroupByTimeOpt) -> Result<DataLoaderGroupBy> {
        let source = CONFIG.path_finder.type_source[self.typ.as_ref()]
            .as_str()
            .unwrap();
        let closed_window = match source {
            "rq" => ClosedWindow::Right,
            "coin" => ClosedWindow::Left,
            "ddb-xbond" => ClosedWindow::Left,
            "ddb-future" => ClosedWindow::Left,
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
                ensure!(
                    opt.group_by.is_none(),
                    "Also group_by on specified columns is not implemented yet"
                );
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
            _ => {
                if let Some(last_time) = opt.last_time {
                    self.group_by_dynamic_with_last_time(
                        col(opt.time),
                        opt.group_by.unwrap_or_default(),
                        last_time,
                        DynamicGroupOptions {
                            every: Duration::parse(rule),
                            period: Duration::parse(rule),
                            offset: Duration::parse("0ns"),
                            label: opt.label,
                            closed_window,
                            ..Default::default()
                        },
                    )
                } else {
                    self.group_by_dynamic(
                        col(opt.time),
                        opt.group_by.unwrap_or_default(),
                        DynamicGroupOptions {
                            every: Duration::parse(rule),
                            period: Duration::parse(rule),
                            offset: Duration::parse("0ns"),
                            label: opt.label,
                            closed_window,
                            ..Default::default()
                        },
                    )
                }
            },
        }
    }

    /// Groups the data by the specified columns, maintaining the original order of the groups.
    ///
    /// This method performs a stable grouping operation, which means that the order of the groups
    /// in the result will match the order of their first occurrences in the original data.
    ///
    /// # Arguments
    ///
    /// * `by` - An expression or a list of expressions to group by.
    ///
    /// # Returns
    ///
    /// A `DataLoaderGroupBy` instance representing the grouped data.
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

    /// Groups the data by the specified columns.
    ///
    /// This method performs a standard grouping operation. The order of the groups in the result
    /// is not guaranteed to match the order of their occurrences in the original data.
    ///
    /// # Arguments
    ///
    /// * `by` - An expression or a list of expressions to group by.
    ///
    /// # Returns
    ///
    /// A `DataLoaderGroupBy` instance representing the grouped data.
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

    /// Groups the data dynamically based on a time index and additional grouping expressions.
    ///
    /// This method allows for more complex grouping operations, particularly useful for time-based
    /// grouping with various options.
    ///
    /// # Arguments
    ///
    /// * `index_column` - The expression representing the time index column.
    /// * `group_by` - Additional expressions to group by alongside the time index.
    /// * `options` - Dynamic grouping options to configure the grouping behavior.
    ///
    /// # Returns
    ///
    /// A `Result` containing a `DataLoaderGroupBy` instance if successful, or an error if the
    /// operation fails.
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

    #[inline]
    pub fn group_by_dynamic_with_last_time<E: AsRef<[Expr]>>(
        self,
        index_column: Expr,
        group_by: E,
        last_time: &str,
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
            last_time: Some(last_time.into()),
            time: Some(time_col),
        })
    }
}

impl DataLoaderGroupBy {
    /// Applies aggregation functions to the grouped data.
    ///
    /// This method performs aggregation on the grouped data using the provided aggregation expressions.
    /// It handles different scenarios based on the presence of a last time column and its relation to the time column.
    ///
    /// # Arguments
    ///
    /// * `aggs` - A collection of aggregation expressions to apply to the grouped data.
    ///
    /// # Returns
    ///
    /// A `DataLoader` instance containing the aggregated data.
    ///
    /// # Behavior
    ///
    /// - If a last time column is present and different from the time column, it adds a last() aggregation for the last time column.
    /// - If the last time column is the same as the time column, it renames the aggregated last time column and drops the original time column.
    /// - If no last time column is present, it simply applies the provided aggregations.
    pub fn agg<E: AsRef<[Expr]>>(self, aggs: E) -> DataLoader {
        let aggs = aggs.as_ref();
        let dfs = if let Some(last_time) = &self.last_time {
            let time_col = self.time.as_deref().unwrap();
            if last_time.as_ref() != time_col {
                let aggs: Vec<_> = aggs
                    .iter()
                    .cloned()
                    .chain(std::iter::once(col(&**last_time).last()))
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
                        col(&**last_time).last().name().suffix("_last"),
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
