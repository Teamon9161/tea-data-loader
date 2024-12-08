use std::ops::{Deref, DerefMut};

use pyo3::prelude::*;
use pyo3_polars::{PyDataFrame, PyLazyFrame};
use tea_data_loader::prelude::Frame;

pub fn frame_into_py(df: Frame, py: Python) -> PyObject {
    match df {
        Frame::Eager(df) => PyDataFrame(df).into_py(py),
        Frame::Lazy(lf) => PyLazyFrame(lf).into_py(py),
    }
}

#[repr(transparent)]
pub struct Wrap<T>(pub T);

impl<T> Deref for Wrap<T> {
    type Target = T;
    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<T> DerefMut for Wrap<T> {
    #[inline]
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl<T> From<T> for Wrap<T> {
    #[inline]
    fn from(value: T) -> Self {
        Self(value)
    }
}
