use pyo3::prelude::*;
use tea_data_loader::factors::map::*;
use tea_data_loader::factors::*;
use super::PyFactor;
use std::sync::Arc;

use crate::utils::Wrap;

trait PyClassName {
    const PY_CLASS_NAME: String;
}


macro_rules! define_py_typ_class {
    ($name:ident, $inner:ident) => {
        impl PyClassName for $inner {
            const PY_CLASS_NAME: String = stringify!($name);
        }

        #[pyclass(name=$inner::PY_CLASS_NAME, extends=PyFactor)]
        pub struct $name;

        #[pymethods]
        impl $name {
            #[new]
            #[pyo3(signature = (param=Wrap(Param::None)))]
            fn new(param: Wrap<Param>) -> (Self, PyFactor) {
                let fac = $inner::new(param.0);
                ($name, PyFactor(Arc::new(fac)))
            }

            fn fac_name(&self) -> String {
                Factor::<$inner>::fac_name().to_string()
            }
        }
    };
}

define_py_typ_class!(PyTyp, Typ);