use polars::prelude::*;

use crate::prelude::*;

impl DataLoader {
    /// Concatenates all DataFrames in the DataLoader into a single LazyFrame.
    ///
    /// This method performs the following operations:
    /// 1. Iterates through all DataFrames in the DataLoader.
    /// 2. For each DataFrame, it checks if a 'symbol' column exists.
    /// 3. If 'symbol' column doesn't exist, it adds one using the symbol associated with the DataFrame.
    /// 4. Converts each DataFrame to a LazyFrame.
    /// 5. Concatenates all LazyFrames vertically.
    ///
    /// # Returns
    ///
    /// Returns a `Result<LazyFrame>` which is the concatenated LazyFrame of all DataFrames in the DataLoader.
    ///
    /// # Errors
    ///
    /// This function will return an error if there are issues with DataFrame operations or concatenation.
    pub fn concat(self) -> Result<LazyFrame> {
        let dfs = self
            .into_iter()
            .map(|(symbol, mut df)| {
                let schema = df.schema()?;
                if !schema.contains("symbol") {
                    Ok(df.with_column(symbol.lit().alias("symbol"))?.lazy())
                } else {
                    Ok(df.lazy())
                }
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(concat(&dfs, UnionArgs::default())?)
    }
}
