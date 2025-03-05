mod fac_analyse;
mod from_py;
mod group_by;
// mod pyfactors;
mod pyloader;
mod utils;
mod with_facs;
mod with_strategies;

use group_by::PyDataLoaderGroupBy;
use pyloader::PyLoader;
use pyo3::prelude::*;
pub use tea_data_loader::pyfactors;

#[pymodule]
fn loader(_py: Python, m: &Bound<'_, PyModule>) -> PyResult<()> {
    pyfactors::register_agg_facs(m)?;
    m.add_class::<pyfactors::PyAggFactor>()?;
    m.add_class::<PyLoader>()?;
    m.add_class::<PyDataLoaderGroupBy>()?;
    Ok(())
}
