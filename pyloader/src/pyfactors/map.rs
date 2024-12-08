use pyo3::prelude::*;
use tea_data_loader::factors::map::*;
use tea_data_loader::factors::*;

use crate::utils::Wrap;

#[pyclass(name = "Typ")]
pub struct PyTyp(Factor<Typ>);

#[pymethods]
impl PyTyp {
    #[new]
    fn new(param: Wrap<Param>) -> Self {
        Self(param.0.into())
    }

    fn __repr__(&self) -> String {
        format!("{:?}", self.0)
    }

    fn fac_name(&self) -> String {
        Factor::<Typ>::fac_name().to_string()
    }

    fn name(&self) -> String {
        self.0.name()
    }
}
