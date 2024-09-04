use polars::lazy::dsl::cols;
use polars::prelude::*;

use super::super::utils::get_preprocess_exprs;
use crate::path_finder::{PathConfig, PathFinder};
use crate::prelude::*;

/// Columns to be extracted from the no-adjusted data
const NOADJ_COLS: [&str; 4] = ["open", "high", "low", "close"];

impl DataLoader {
    /// Joins no-adjusted kline data with the existing kline data in the DataLoader.
    ///
    /// This method adds no-adjusted (unadjusted) price data to the existing DataLoader
    /// for future contracts. It's useful when you need both adjusted and unadjusted
    /// price data in the same DataFrame.
    ///
    /// # Arguments
    ///
    /// * `freq` - An optional frequency for the no-adjusted data. If None, uses the DataLoader's frequency.
    /// * `memory_map` - Whether to use memory mapping when reading the data files.
    /// * `flag` - A boolean flag to determine whether the operation should be performed.
    ///
    /// # Returns
    ///
    /// A `Result` containing the modified `DataLoader` if successful, or an error if the operation fails.
    ///
    /// # Behavior
    ///
    /// - Only processes future contracts (checks if `self.typ` is "future").
    /// - Skips processing if the flag is false or if "close_noadj" column already exists.
    /// - Reads no-adjusted data from IPC files, applies necessary transformations, and joins with existing data.
    /// - Adds "_noadj" suffix to the columns from no-adjusted data.
    /// - If no-adjusted data is not found for a symbol, prints a warning message.
    ///
    /// # Errors
    ///
    /// This method can return an error if:
    /// - There's an issue creating the PathFinder or finding the data path.
    /// - There's a problem reading or processing the IPC files.
    /// - Any other IO or data processing error occurs.
    pub fn with_noadj(mut self, freq: Option<&str>, memory_map: bool, flag: bool) -> Result<Self> {
        if !flag || self.contains("close_noadj") || (self.typ.as_ref() != "future") {
            return Ok(self);
        }
        let new_freq = if let Some(freq) = freq {
            freq.to_owned()
        } else {
            self.freq.as_deref().unwrap().to_owned()
        };

        let filter_cond = self.time_filter_cond(new_freq.as_str())?;
        let rename_table = self.rename_table(Tier::Lead);
        let preprocess_exprs = get_preprocess_exprs("__base__");
        let finder_config = PathConfig {
            config: CONFIG.path_finder.clone(),
            typ: "future".into(),
            freq: new_freq,
            tier: Tier::Lead,
            adjust: Adjust::None,
        };
        let path = PathFinder::new(finder_config)?.path()?;
        let mut out = self.empty_copy();
        for (symbol, df) in self {
            let file_path = path.join(symbol.to_string() + ".feather");
            if file_path.exists() {
                let mut df_noadj = LazyFrame::scan_ipc(
                    &file_path,
                    ScanArgsIpc {
                        rechunk: true,
                        memory_map,
                        ..Default::default()
                    },
                )?;
                // apply rename condition
                if let Some(table) = &rename_table {
                    df_noadj =
                        df_noadj.rename(table.keys(), table.values().map(|v| v.as_str().unwrap()));
                };
                // apply filter condition
                if let Some(cond) = filter_cond.clone() {
                    df_noadj = df_noadj.filter(cond)
                };
                df_noadj = df_noadj
                    .with_columns(&preprocess_exprs)
                    .select([cols(NOADJ_COLS).name().suffix("_noadj")]);
                out.dfs.push(
                    concat_lf_horizontal(
                        [df.lazy(), df_noadj.lazy()],
                        UnionArgs {
                            rechunk: true,
                            ..Default::default()
                        },
                    )?
                    .into(),
                )
            } else {
                eprintln!("no no-adjusted data found for symbol: {}", symbol);
            }
        }
        Ok(out)
    }
}
