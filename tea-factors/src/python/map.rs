use pyo3::prelude::*;
use crate::map::*;
use crate::prelude::*;
use super::PyFactor;
use std::sync::Arc;

macro_rules! define_py_typ_class {
    ($name:ident, $inner:ident, $str_inner: expr) => {
        #[pyclass(name=$str_inner, extends=PyFactor)]
        pub struct $name;

        #[pymethods]
        impl $name {
            #[new]
            #[pyo3(signature = (param=Param::None))]
            fn new(param: Param) -> (Self, PyFactor) {
                let fac = $inner::new(param);
                ($name, PyFactor(Arc::new(fac)))
            }

            fn fac_name(&self) -> String {
                Factor::<$inner>::fac_name().to_string()
            }
        }
    };
}

define_py_typ_class!(PyTyp, Typ, "Typ");