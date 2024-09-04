use polars::prelude::*;

use crate::path_finder::*;
use crate::prelude::*;

impl DataLoader {
    /// Adds spread data to the DataLoader.
    ///
    /// This method joins spread data with the existing data in the DataLoader.
    /// It's specifically designed for minute-frequency data and uses tick spread information.
    ///
    /// # Arguments
    ///
    /// * `flag` - A boolean flag to determine whether the spread data should be added.
    ///
    /// # Returns
    ///
    /// A `Result` containing the modified `DataLoader` if successful, or an error if the operation fails.
    ///
    /// # Behavior
    ///
    /// - Skips processing if the flag is false or if "twap_spread" column already exists.
    /// - Only supports minute-frequency data; throws an unimplemented error for other frequencies.
    /// - Uses the PathFinder to locate the spread data.
    /// - Performs a left join with the spread data based on the time column.
    ///
    /// # Errors
    ///
    /// This method can return an error if:
    /// - There's an issue creating the PathFinder or finding the spread data path.
    /// - The frequency of the data is not "min".
    /// - Any other IO or data processing error occurs during the join operation.
    pub fn with_spread(mut self, flag: bool) -> Result<Self> {
        if !flag || self.contains("twap_spread") {
            return Ok(self);
        }
        let path_config = PathConfig {
            config: CONFIG.path_finder.clone(),
            freq: "tick_spread".into(),
            typ: self.typ.to_string(),
            tier: Tier::Lead,
            adjust: Adjust::None,
        };
        let spread_path = PathFinder::new(path_config)?.path()?;
        if self.freq.as_deref().unwrap() != "min" {
            unimplemented!("Only support join spread for minute data for now")
        }
        self.left_join(spread_path, [col("time")], [col("datetime")], flag)
    }
}
