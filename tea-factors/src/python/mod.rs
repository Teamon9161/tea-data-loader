mod agg;
mod map;

use std::sync::Arc;

pub use agg::register_agg_facs;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::pybacked::PyBackedStr;
use pyo3_polars::PyExpr;
use crate::{Param, PlAggFactor, PlFactor};
use crate::prelude::Result;
// use crate::utils::Wrap;

#[pyclass(name="Factor", subclass)]
pub struct PyFactor(pub Arc<dyn PlFactor>);

#[pymethods]
impl PyFactor {
    fn expr(&self) -> Result<PyExpr> {
        Ok(PyExpr(self.0.try_expr()?))
    }

    fn __repr__(&self) -> String {
        self.0.name()
    }

    fn name(&self) -> String {
        self.0.name()
    }
}

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

impl FromPyObject<'_> for Param {
    fn extract_bound(ob: &Bound<'_, PyAny>) -> PyResult<Self> {
        if ob.is_none() {
            return Ok(Param::None);
        }
        if let Ok(v) = ob.extract::<bool>() {
            Ok(Param::Bool(v))
        } else if let Ok(v) = ob.extract::<i32>() {
            Ok(Param::I32(v))
        } else if let Ok(v) = ob.extract::<f64>() {
            Ok(Param::F64(v))
        } else if let Ok(v) = ob.extract::<PyBackedStr>() {
            Ok(Param::Str((&*v).into()))
        } else {
            Err(PyValueError::new_err("Invalid parameter type"))
        }
    }
}

pub fn register(m: &Bound<'_, PyModule>) -> PyResult<()> {
    register_agg_facs(m)?;
    m.add_class::<PyFactor>()?;
    m.add_class::<PyAggFactor>()?;
    Ok(())
}
