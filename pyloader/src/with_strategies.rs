use pyo3::prelude::*;
use tea_data_loader::prelude::*;
use crate::pyloader::PyLoader;


#[pymethods]
impl PyLoader {
    /// Adds strategies to the DataLoader.
    ///
    /// This method applies a list of strategies to the data in the DataLoader,
    /// calculating new columns based on the provided strategy definitions.
    ///
    /// # Arguments
    ///
    /// * `strategies` - A vector of strings, each representing a strategy to be applied.
    ///
    /// # Returns
    ///
    /// A `Result` containing the modified `PyLoader` if successful, or an error if the operation fails.
    ///
    /// # Behavior
    ///
    /// - Filters out strategies that already exist in the schema.
    /// - Parses the strategies into `StrategyWork` objects.
    /// - Calculates necessary factors for the strategies.
    /// - Applies the strategies in parallel to each DataFrame in the DataLoader.
    /// - Adds the resulting series as new columns to the DataFrames.
    ///
    /// # Errors
    ///
    /// This method can return an error if:
    /// - There's an issue parsing the strategies.
    /// - There's a problem calculating factors or applying strategies.
    /// - Any other data processing error occurs.
    #[pyo3(signature = (strategies))]
    fn with_strategies(&self, strategies: Vec<String>) -> Result<Self> {
        Ok(PyLoader(self.0.clone().with_strategies(&strategies)?))
    }
}
