mod map;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::pybacked::PyBackedStr;
use tea_data_loader::factors::Param;

use crate::utils::Wrap;

#[pyclass(name = "Factor", subclass)]
pub struct PyFactor {}

// #[pymethods]
// impl PyFactor {
//     fn __repr__(&self) -> String {
//         "Factor".to_string()
//     }
// }

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
