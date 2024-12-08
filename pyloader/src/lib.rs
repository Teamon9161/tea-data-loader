mod from_py;
mod group_by;
mod pyfactors;
mod pyloader;
mod utils;
mod with_facs;

use group_by::PyDataLoaderGroupBy;
use pyloader::PyLoader;
use pyo3::prelude::*;

#[pymodule]
fn loader(_py: Python, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PyLoader>()?;
    m.add_class::<PyDataLoaderGroupBy>()?;
    Ok(())
}
