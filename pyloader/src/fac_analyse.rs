use pyo3::prelude::*;
use pyo3_polars::{PyDataFrame, PySeries};
use tea_data_loader::export::tevec::agg::CorrMethod;
use tea_data_loader::fac_analyse::{FacAnalysis, FacSummary, Summary, SummaryReport};
use tea_data_loader::prelude::*;

use super::pyloader::PyLoader;
use crate::utils::Wrap;

#[pyclass(name = "FacAnalysis")]
pub struct PyFacAnalysis(FacAnalysis);

#[pyclass(name = "Summary")]
pub struct PySummary(Summary);

#[pyclass(name = "FacSummary")]
pub struct PyFacSummary(FacSummary);

#[pyclass(name = "SummaryReport")]
pub struct PySummaryReport(SummaryReport);

#[pymethods]
impl PyLoader {
    fn fac_analyse(
        &self,
        facs: Vec<String>,
        labels: Vec<String>,
        drop_peak: bool,
    ) -> Result<PyFacAnalysis> {
        Ok(PyFacAnalysis(
            self.0.clone().fac_analyse(&facs, &labels, drop_peak)?,
        ))
    }
}

#[pymethods]
impl PyFacAnalysis {
    #[getter]
    fn summary(&self) -> PySummary {
        PySummary(self.0.summary.clone())
    }

    #[pyo3(signature = (method=Wrap(CorrMethod::Pearson)))]
    fn with_ic_overall(&self, method: Wrap<CorrMethod>) -> Result<Self> {
        Ok(PyFacAnalysis(self.0.clone().with_ic_overall(method.0)?))
    }

    #[pyo3(signature = (rule, method=Wrap(CorrMethod::Pearson)))]
    fn with_ts_ic(&self, rule: &str, method: Wrap<CorrMethod>) -> Result<Self> {
        Ok(PyFacAnalysis(self.0.clone().with_ts_ic(rule, method.0)?))
    }

    #[pyo3(signature = (group=10))]
    fn with_ts_group_ret(&self, group: usize) -> Result<Self> {
        Ok(PyFacAnalysis(self.0.clone().with_ts_group_ret(group)?))
    }

    #[pyo3(signature = (rule=None, group=10))]
    fn with_group_ret(&self, rule: Option<&str>, group: usize) -> Result<Self> {
        Ok(PyFacAnalysis(self.0.clone().with_group_ret(rule, group)?))
    }

    fn with_half_life(&self) -> Result<Self> {
        Ok(PyFacAnalysis(self.0.clone().with_half_life()?))
    }
}

#[pymethods]
impl PySummary {
    /// Get the list of factor names
    #[getter]
    fn facs(&self) -> Vec<String> {
        self.0.facs.clone()
    }

    /// Get the list of label names
    #[getter]
    fn labels(&self) -> Vec<String> {
        self.0.labels.clone()
    }

    /// Get the symbol-level IC for each factor.
    /// Each element is a DataLoader containing IC values for different symbols for one factor.
    #[getter]
    fn symbol_ic(&self) -> Vec<PyLoader> {
        self.0
            .symbol_ic
            .iter()
            .map(|s| PyLoader(s.clone()))
            .collect()
    }

    /// Get the overall IC for each factor
    #[getter]
    fn ic_overall(&self) -> Vec<PyDataFrame> {
        self.0
            .ic_overall
            .iter()
            .map(|s| PyDataFrame(s.clone()))
            .collect()
    }

    /// Get the time-series IC for each factor.
    /// Each DataFrame is a factor's time-series IC, with columns representing IC for different labels.
    #[getter]
    fn ts_ic(&self) -> Vec<PyDataFrame> {
        self.0
            .ts_ic
            .iter()
            .map(|s| PyDataFrame(s.clone()))
            .collect()
    }

    /// Get the symbol-level time-series group returns for each factor
    #[getter]
    fn symbol_ts_group_rets(&self) -> Vec<PyLoader> {
        self.0
            .symbol_ts_group_rets
            .iter()
            .map(|s| PyLoader(s.clone()))
            .collect()
    }

    /// Get the time-series group returns for each factor.
    /// Returns group returns calculated over time periods, averaged to generate group performance curves.
    #[getter]
    fn ts_group_rets(&self) -> Vec<PyDataFrame> {
        self.0
            .ts_group_rets
            .iter()
            .map(|s| PyDataFrame(s.clone()))
            .collect()
    }

    /// Get the symbol-level group returns for each factor.
    /// Returns average returns for each group per factor, before averaging across symbols.
    #[getter]
    fn symbol_group_rets(&self) -> Vec<PyLoader> {
        self.0
            .symbol_group_rets
            .iter()
            .map(|s| PyLoader(s.clone()))
            .collect()
    }

    /// Get the group returns for each factor.
    /// Returns average returns for each group.
    #[getter]
    fn group_rets(&self) -> Vec<PyDataFrame> {
        self.0
            .group_rets
            .iter()
            .map(|s| PyDataFrame(s.clone()))
            .collect()
    }

    /// Get the half-life values for each factor.
    /// Returns half-life calculations for factors if available.
    #[getter]
    fn half_life(&self) -> Option<PyDataFrame> {
        self.0.half_life.as_ref().map(|s| PyDataFrame(s.clone()))
    }

    /// Finalize the summary and create a SummaryReport
    fn finish(&self) -> PySummaryReport {
        PySummaryReport(self.0.clone().finish())
    }
}

#[pymethods]
impl PySummaryReport {
    #[getter]
    fn labels(&self) -> Vec<String> {
        self.0.labels().to_vec()
    }

    #[getter]
    fn fac_series(&self) -> PySeries {
        PySeries(self.0.fac_series())
    }

    #[getter]
    fn ts_ic(&self) -> Vec<PyDataFrame> {
        self.0.ts_ic().into_iter().map(PyDataFrame).collect()
    }

    #[getter]
    fn ic(&self) -> Result<PyDataFrame> {
        Ok(PyDataFrame(self.0.ic()?))
    }

    #[getter]
    fn ir(&self) -> Result<PyDataFrame> {
        Ok(PyDataFrame(self.0.ir()?))
    }

    #[getter]
    fn ic_std(&self) -> Result<PyDataFrame> {
        Ok(PyDataFrame(self.0.ic_std()?))
    }

    #[getter]
    fn ic_skew(&self) -> Result<PyDataFrame> {
        Ok(PyDataFrame(self.0.ic_skew()?))
    }

    #[getter]
    fn ic_kurt(&self) -> Result<PyDataFrame> {
        Ok(PyDataFrame(self.0.ic_kurt()?))
    }

    #[getter]
    fn ic_overall(&self) -> Result<PyDataFrame> {
        Ok(PyDataFrame(self.0.ic_overall()?))
    }

    #[getter]
    fn group_rets(&self) -> Vec<PyDataFrame> {
        self.0.group_rets().into_iter().map(PyDataFrame).collect()
    }

    #[getter]
    fn half_life(&self) -> PyDataFrame {
        PyDataFrame(self.0.half_life())
    }
}
