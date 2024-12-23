use pyo3::prelude::*;
use pyo3::pybacked::PyBackedStr;
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
    /// Perform factor analysis on the data
    ///
    /// # Arguments
    /// * `facs` - A vector of factor names to analyze
    /// * `labels` - A vector of label names to use in the analysis
    /// * `drop_peak` - Whether to drop the peak values in the analysis
    ///
    /// # Returns
    /// A `PyFacAnalysis` object containing the results of the factor analysis
    #[pyo3(signature = (facs, labels, drop_peak=true))]
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
    /// Get a `Summary` object that contains overall results of the analysis
    ///
    /// Returns a `Summary` object containing results of the analysis, including
    /// overall IC, group returns, and half-life for each factor.
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
            .map(|s: &tea_data_loader::export::polars::prelude::DataFrame| PyDataFrame(s.clone()))
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
    /// Get a PyFacSummary by index or factor name
    fn __getitem__(&self, index: &Bound<'_, PyAny>) -> Result<PyFacSummary> {
        if let Ok(index) = index.extract::<usize>() {
            return Ok(PyFacSummary(self.0[index].clone()));
        } else if let Ok(index) = index.extract::<PyBackedStr>() {
            return Ok(PyFacSummary(self.0[&*index].clone()));
        } else {
            bail!("Index must be int or str");
        }
    }

    /// Get the list of label names
    #[getter]
    fn labels(&self) -> Vec<String> {
        self.0.labels().to_vec()
    }

    /// Get the factor series
    #[getter]
    fn fac_series(&self) -> PySeries {
        PySeries(self.0.fac_series())
    }

    /// Get the time-series IC for each factor
    #[getter]
    fn ts_ic(&self) -> Vec<PyDataFrame> {
        self.0.ts_ic().into_iter().map(PyDataFrame).collect()
    }

    /// Get the IC (Information Coefficient) for each factor
    #[getter]
    fn ic(&self) -> Result<PyDataFrame> {
        Ok(PyDataFrame(self.0.ic()?))
    }

    /// Get the IR (Information Ratio) for each factor
    #[getter]
    fn ir(&self) -> Result<PyDataFrame> {
        Ok(PyDataFrame(self.0.ir()?))
    }

    /// Get the standard deviation of IC for each factor
    #[getter]
    fn ic_std(&self) -> Result<PyDataFrame> {
        Ok(PyDataFrame(self.0.ic_std()?))
    }

    /// Get the skewness of IC for each factor
    #[getter]
    fn ic_skew(&self) -> Result<PyDataFrame> {
        Ok(PyDataFrame(self.0.ic_skew()?))
    }

    /// Get the kurtosis of IC for each factor
    #[getter]
    fn ic_kurt(&self) -> Result<PyDataFrame> {
        Ok(PyDataFrame(self.0.ic_kurt()?))
    }

    /// Get the overall IC for each factor
    #[getter]
    fn ic_overall(&self) -> Result<PyDataFrame> {
        Ok(PyDataFrame(self.0.ic_overall()?))
    }

    /// Get the group returns for each factor
    #[getter]
    fn group_rets(&self) -> Vec<PyDataFrame> {
        self.0.group_rets().into_iter().map(PyDataFrame).collect()
    }

    /// Get the half-life for each factor
    #[getter]
    fn half_life(&self) -> PyDataFrame {
        PyDataFrame(self.0.half_life())
    }
}

#[pymethods]
impl PyFacSummary {
    /// Get the factor name
    #[getter]
    fn fac(&self) -> String {
        self.0.fac.clone()
    }

    /// Get the list of label names
    #[getter]
    fn labels(&self) -> Vec<String> {
        self.0.labels.iter().cloned().collect()
    }   

    /// Get the symbol-level IC for the factor
    #[getter]
    fn symbol_ic(&self) -> Option<PyLoader> {
        self.0.symbol_ic.clone().map(PyLoader)
    }

    /// Get the overall IC for the factor
    #[getter]
    fn ic_overall(&self) -> Option<PyDataFrame> {
        self.0.ic_overall.clone().map(PyDataFrame)
    }

    /// Get the time-series IC for the factor
    #[getter]
    fn ts_ic(&self) -> Option<PyDataFrame> {
        self.0.ts_ic.clone().map(PyDataFrame)
    }

    /// Get the symbol-level time-series group returns for the factor
    #[getter]
    fn symbol_ts_group_rets(&self) -> Option<PyLoader> {
        self.0.symbol_ts_group_rets.clone().map(PyLoader)
    }

    /// Get the time-series group returns for the factor
    #[getter]
    fn ts_group_rets(&self) -> Option<PyDataFrame> {
        self.0.ts_group_rets.clone().map(PyDataFrame)
    }

    /// Get the symbol-level group returns for the factor
    #[getter]
    fn symbol_group_rets(&self) -> Option<PyLoader> {
        self.0.symbol_group_rets.clone().map(PyLoader)
    }

    /// Get the group returns for the factor
    #[getter]
    fn group_rets(&self) -> Option<PyDataFrame> {
        self.0.group_rets.clone().map(PyDataFrame)
    }

    /// Get the half-life for the factor
    #[getter]
    fn half_life(&self) -> Option<f64> {
        self.0.half_life
    }
}