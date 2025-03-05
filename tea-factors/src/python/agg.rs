use std::sync::Arc;

use pyo3::prelude::*;
use crate::tick::both::*;
use crate::tick::order_book::*;
use crate::tick::order_flow::*;

use super::PyAggFactor;

macro_rules! define_py_agg_class {
    ($name:ident, $inner:ident, $str_inner:expr) => {
        #[pyclass(name=$str_inner, extends=PyAggFactor)]
        pub struct $name;

        #[pymethods]
        impl $name {
            #[new]
            fn new() -> (Self, PyAggFactor) {
                ($name, PyAggFactor(Arc::new($inner)))
            }
        }
    };
}

// Use the macro to define the classes
define_py_agg_class!(PyAggOfi, AggOfi, "AggOfi");
define_py_agg_class!(PyAggObOfi, AggObOfi, "AggObOfi");
define_py_agg_class!(PyAggBsIntensity, AggBsIntensity, "AggBsIntensity");
define_py_agg_class!(PyAggCancelRate, AggCancelRate, "AggCancelRate");

pub fn register_agg_facs(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PyAggOfi>()?;
    m.add_class::<PyAggObOfi>()?;
    m.add_class::<PyAggBsIntensity>()?;
    m.add_class::<PyAggCancelRate>()?;
    Ok(())
}
