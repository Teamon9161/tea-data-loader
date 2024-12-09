mod agg;
mod map;

use std::sync::Arc;

pub use agg::register_agg_facs;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::pybacked::PyBackedStr;
use pyo3_polars::PyExpr;
use tea_data_loader::factors::{Param, PlAggFactor};

use crate::utils::Wrap;

// #[pyclass(name = "Factor")]
// pub struct PyFactor {}

#[pyclass(name = "AggFactor", subclass)]
#[derive(Clone)]
pub struct PyAggFactor(pub Arc<dyn PlAggFactor>);

#[pymethods]
impl PyAggFactor {
    /// Returns the string representation of the factor.
    fn __repr__(&self) -> String {
        self.0.name()
    }

    /// Returns the name of the factor.
    ///
    /// # Returns
    /// A string containing the factor name.
    fn name(&self) -> String {
        self.0.name()
    }

    /// Returns the factor expression used in aggregation.
    ///
    /// # Returns
    /// An optional Polars expression representing the factor to be aggregated.
    /// Returns None if the factor does not have an underlying expression.
    ///
    /// # Errors
    /// Returns a PyResult error if expression creation fails.
    fn agg_fac_expr(&self) -> PyResult<Option<PyExpr>> {
        Ok(self.0.agg_fac_expr()?.map(PyExpr))
    }

    /// Returns the aggregation expression for this factor.
    ///
    /// # Returns
    /// A Polars expression representing the aggregation operation.
    ///
    /// # Errors
    /// Returns a PyResult error if expression creation fails.
    fn agg_expr(&self) -> PyResult<PyExpr> {
        Ok(PyExpr(self.0.agg_expr()?))
    }

    /// Returns the name of the underlying factor used in aggregation.
    ///
    /// # Returns
    /// An optional string containing the name of the factor being aggregated.
    /// Returns None if there is no underlying factor name.
    fn agg_fac_name(&self) -> Option<String> {
        self.0.agg_fac_name()
    }
}

impl FromPyObject<'_> for Wrap<Param> {
    fn extract_bound(ob: &Bound<'_, PyAny>) -> PyResult<Self> {
        if ob.is_none() {
            return Ok(Wrap(Param::None));
        }
        if let Ok(v) = ob.extract::<bool>() {
            Ok(Wrap(Param::Bool(v)))
        } else if let Ok(v) = ob.extract::<i32>() {
            Ok(Wrap(Param::I32(v)))
        } else if let Ok(v) = ob.extract::<f64>() {
            Ok(Wrap(Param::F64(v)))
        } else if let Ok(v) = ob.extract::<PyBackedStr>() {
            Ok(Wrap(Param::Str((&*v).into())))
        } else {
            Err(PyValueError::new_err("Invalid parameter type"))
        }
    }
}
