use pyo3::prelude::*;
use pyo3::types::PyList;
use pyo3_polars::PyExpr;
use tea_data_loader::export::polars::prelude::Label;
use tea_data_loader::prelude::*;

use super::pyfactors::PyAggFactor;
use crate::pyloader::PyLoader;
use crate::utils::Wrap;

#[pymethods]
impl PyLoader {
    /// Adds factors to the DataLoader using the specified backend.
    ///
    /// This method processes a list of factor names, parses them according to the chosen backend,
    /// and adds the resulting factors to each DataFrame in the DataLoader.
    ///
    /// # Arguments
    ///
    /// * `facs` - A slice of factor names to be added.
    /// * `backend` - The backend to use for factor calculation (Polars or Tevec), defaults to Polars.
    ///
    /// # Returns
    ///
    /// A `Result` containing the modified `DataLoader` with new factors added, or an error.
    #[pyo3(signature = (facs, backend=Wrap(Backend::Polars)))]
    fn with_facs(&self, facs: Vec<String>, backend: Wrap<Backend>) -> Result<Self> {
        Ok(PyLoader(self.0.clone().with_facs(&facs, backend.0)?))
    }

    #[pyo3(signature = (rule, facs, agg_exprs, last_time=None, time="time", group_by=None, daily_col="trading_date", maintain_order=true, label=Wrap(Label::Left)))]
    fn with_agg_facs(
        &self,
        rule: &str,
        facs: &Bound<'_, PyList>,
        agg_exprs: Vec<PyExpr>,
        last_time: Option<&str>,
        time: &str,
        group_by: Option<Vec<PyExpr>>,
        daily_col: &str,
        maintain_order: bool,
        label: Wrap<Label>,
    ) -> PyResult<Self> {
        let facs: PyResult<Vec<_>> = facs
            .iter()
            .map(|f| f.extract::<PyAggFactor>().map(|f| f.0))
            .collect();
        let agg_exprs = agg_exprs.into_iter().map(|e| e.0).collect::<Vec<_>>();
        let group_by = group_by.map(|v| v.into_iter().map(|e| e.0).collect::<Vec<_>>());
        Ok(PyLoader(self.0.clone().with_pl_agg_facs(
            rule,
            &facs?,
            agg_exprs,
            GroupByTimeOpt {
                last_time,
                time,
                group_by: group_by.as_ref().map(|v| v.as_slice()),
                daily_col,
                maintain_order,
                label: label.0,
            },
        )?))
    }
}
