use pyo3::prelude::*;
use tea_data_loader::prelude::*;

use crate::pyloader::PyLoader;
use crate::utils::Wrap;

#[pymethods]
impl PyLoader {
    /// Adds factors to the DataLoader using the specified backend.
    ///
    /// This method processes a list of factor names, parses them according to the chosen backend,
    /// and adds the resulting factors to each DataFrame in the DataLoader.
    ///
    /// # Arguments
    ///
    /// * `facs` - A slice of factor names to be added.
    /// * `backend` - The backend to use for factor calculation (Polars or Tevec).
    ///
    /// # Returns
    ///
    /// A `Result` containing the modified `DataLoader` with new factors added, or an error.
    fn with_facs(&self, facs: Vec<String>, backend: Wrap<Backend>) -> Result<Self> {
        Ok(PyLoader(self.0.clone().with_facs(&facs, backend.0)?))
    }
}
