mod pyloader;
use pyloader::PyLoader;
use pyo3::prelude::*;

#[pymodule]
fn loader(_py: Python, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PyLoader>()?;
    Ok(())
}
