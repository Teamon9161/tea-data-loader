use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3_polars::PyExpr;
use tea_data_loader::export::polars::prelude::*;
use tea_data_loader::prelude::*;

use super::pyloader::PyLoader;
use crate::utils::Wrap;

#[pyclass(name = "DataLoaderGroupBy")]
pub struct PyDataLoaderGroupBy(pub DataLoaderGroupBy);

#[pymethods]
impl PyDataLoaderGroupBy {
    /// Gets the underlying DataLoader
    #[getter]
    fn get_dl(&self) -> PyLoader {
        self.0.dl.clone().into()
    }

    /// Sets the underlying DataLoader
    #[setter]
    fn set_dl(&mut self, dl: PyLoader) {
        self.0.dl = dl.0;
    }

    /// Gets the last time column name if present
    #[getter]
    fn get_last_time(&self) -> Option<String> {
        self.0.last_time.as_ref().map(|v| v.to_string())
    }

    /// Sets the last time column name
    #[setter]
    fn set_last_time(&mut self, last_time: Option<&str>) {
        self.0.last_time = last_time.map(Into::into);
    }

    /// Gets the time column name if present
    #[getter]
    fn get_time(&self) -> Option<&str> {
        self.0.time.as_ref().map(|s| s.as_str())
    }

    /// Sets the time column name
    #[setter]
    fn set_time(&mut self, time: Option<&str>) {
        self.0.time = time.map(Into::into);
    }

    /// Applies aggregation functions to the grouped data.
    ///
    /// This method performs aggregation on the grouped data using the provided aggregation expressions.
    /// It handles different scenarios based on the presence of a last time column and its relation to the time column.
    ///
    /// # Arguments
    ///
    /// * `aggs` - A list of aggregation expressions to apply to the grouped data.
    ///
    /// # Returns
    ///
    /// A `PyLoader` instance containing the aggregated data.
    ///
    /// # Behavior
    ///
    /// - If a last time column is present and different from the time column, it adds a last() aggregation for the last time column.
    /// - If the last time column is the same as the time column, it renames the aggregated last time column and drops the original time column.
    /// - If no last time column is present, it simply applies the provided aggregations.
    fn agg(&self, aggs: Vec<PyExpr>) -> PyResult<PyLoader> {
        Ok(self
            .0
            .clone()
            .agg(aggs.into_iter().map(|e| e.0).collect::<Vec<_>>())
            .into())
    }
}

#[pymethods]
impl PyLoader {
    #[pyo3(signature = (rule, last_time=None, time="time", group_by=None, daily_col="trading_date", maintain_order=true, label=Wrap(Label::Left)))]
    /// Groups data by dynamic frequency.
    ///
    /// # Arguments
    ///
    /// * `rule` - The grouping rule. Can be 'daily' or any rule supported by Polars.
    /// * `last_time` - Optional time column name to call last method on
    /// * `time` - Time column name to group by
    /// * `group_by` - Optional additional columns to group by
    /// * `daily_col` - Column name for daily grouping, defaults to "trading_date"
    /// * `maintain_order` - Whether to maintain original order within groups
    /// * `label` - Which edge of the window to use for labels (left, right or datapoint)
    ///
    /// # Returns
    ///
    /// A `PyDataLoaderGroupBy` containing the grouped data.
    ///
    /// # Details
    ///
    /// This method groups the data based on the specified rule and options:
    ///
    /// - If `rule` is "daily", it groups by the daily column specified in `daily_col`.
    /// - For other rules, it uses Polars' dynamic grouping functionality.
    ///
    /// The grouping maintains the original order within groups if `maintain_order` is true.
    fn group_by_time(
        &self,
        rule: &str,
        last_time: Option<&str>,
        time: &str,
        group_by: Option<Vec<PyExpr>>,
        daily_col: &str,
        maintain_order: bool,
        label: Wrap<Label>,
    ) -> PyResult<PyDataLoaderGroupBy> {
        let group_by = group_by.map(|v| v.into_iter().map(|e| e.0).collect::<Vec<_>>());
        Ok(PyDataLoaderGroupBy(
            self.0
                .clone()
                .group_by_time(
                    rule,
                    GroupByTimeOpt {
                        last_time,
                        time,
                        group_by: group_by.as_ref().map(|v| v.as_slice()),
                        daily_col,
                        maintain_order,
                        label: label.0,
                    },
                )?
                .into(),
        ))
    }

    #[pyo3(signature = (by, maintain_order=true))]
    /// Groups the data by the specified columns.
    ///
    /// This method performs a standard grouping operation. The order of the groups in the result
    /// is determined by the `maintain_order` parameter.
    ///
    /// # Arguments
    ///
    /// * `by` - An expression or a list of expressions to group by.
    /// * `maintain_order` - Whether to maintain the original order within groups (default: true).
    ///
    /// # Returns
    ///
    /// A `PyDataLoaderGroupBy` instance representing the grouped data.
    fn group_by(&self, by: Vec<PyExpr>, maintain_order: bool) -> PyResult<PyDataLoaderGroupBy> {
        if maintain_order {
            Ok(PyDataLoaderGroupBy(self.0.clone().group_by_stable(
                by.into_iter().map(|e| e.0).collect::<Vec<_>>(),
            )))
        } else {
            Ok(PyDataLoaderGroupBy(
                self.0
                    .clone()
                    .group_by(by.into_iter().map(|e| e.0).collect::<Vec<_>>()),
            ))
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
    /// * `every` - The length of the window as a duration string (e.g. "1d", "2h", "30m").
    /// * `period` - The period to advance the window as a duration string. Defaults to `every` if not specified.
    /// * `offset` - The offset for the window boundaries as a duration string. Defaults to 0.
    /// * `group_by` - Additional expressions to group by alongside the time index.
    /// * `label` - Which edge of the window to use for labels (left, right or datapoint). Defaults to left.
    /// * `include_boundaries` - Whether to include the window boundaries in the output. Defaults to false.
    /// * `closed_window` - How the window boundaries should be handled (left, right, both or none). Defaults to left.
    /// * `start_by` - Strategy for determining the start of the first window. Defaults to window bound.
    /// * `last_time` - Optional last time to consider for grouping.
    ///
    /// # Returns
    ///
    /// A `PyDataLoaderGroupBy` instance representing the dynamically grouped data.
    #[pyo3(signature = (index_column, every, period=None, offset=None, group_by=None, label=Wrap(Label::Left), include_boundaries=false, closed_window=Wrap(ClosedWindow::Left), start_by=Wrap(StartBy::WindowBound), last_time=None))]
    fn group_by_dynamic(
        &self,
        index_column: PyExpr,
        every: &str,
        period: Option<&str>,
        offset: Option<&str>,
        group_by: Option<Vec<PyExpr>>,
        label: Wrap<Label>,
        include_boundaries: bool,
        closed_window: Wrap<ClosedWindow>,
        start_by: Wrap<StartBy>,
        last_time: Option<&str>,
    ) -> PyResult<PyDataLoaderGroupBy> {
        let group_by = group_by
            .map(|v| v.into_iter().map(|e| e.0).collect::<Vec<_>>())
            .unwrap_or_default();
        let every = Duration::try_parse(every).map_err(|e| PyValueError::new_err(e.to_string()))?;
        let period = if let Some(period) = period {
            Duration::try_parse(period).map_err(|e| PyValueError::new_err(e.to_string()))?
        } else {
            every.clone()
        };
        let offset = if let Some(offset) = offset {
            Duration::try_parse(offset).map_err(|e| PyValueError::new_err(e.to_string()))?
        } else {
            Duration::try_parse("0ns").unwrap()
        };
        if let Some(last_time) = last_time {
            Ok(PyDataLoaderGroupBy(
                self.0.clone().group_by_dynamic_with_last_time(
                    index_column.0,
                    group_by,
                    last_time,
                    DynamicGroupOptions {
                        every,
                        period,
                        offset,
                        label: label.0,
                        include_boundaries,
                        closed_window: closed_window.0,
                        start_by: start_by.0,
                        ..Default::default()
                    },
                )?,
            ))
        } else {
            Ok(PyDataLoaderGroupBy(self.0.clone().group_by_dynamic(
                index_column.0,
                group_by,
                DynamicGroupOptions {
                    every,
                    period,
                    offset,
                    label: label.0,
                    include_boundaries,
                    closed_window: closed_window.0,
                    start_by: start_by.0,
                    ..Default::default()
                },
            )?))
        }
    }
}
