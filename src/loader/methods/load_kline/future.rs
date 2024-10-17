use std::path::PathBuf;

use glob::glob;
use polars::prelude::*;

use crate::path_finder::{PathConfig, PathFinder};
use crate::prelude::*;
use crate::utils::get_preprocess_exprs;

impl DataLoader {
    /// Loads kline data for futures.
    ///
    /// # Arguments
    ///
    /// * `path_config` - The path configuration for the data.
    /// * `memory_map` - Whether to use memory mapping when reading files.
    ///
    /// # Returns
    ///
    /// Returns a `Result<Self>` containing the updated `DataLoader`.
    pub(super) fn load_future_kline(
        mut self,
        path_config: PathConfig,
        memory_map: bool,
    ) -> Result<Self> {
        let finder = PathFinder::new(path_config)?;
        self.kline_path = Some(finder.path()?);
        let all_files: Vec<PathBuf> = if let Some(symbols) = self.symbols.as_ref() {
            symbols
                .iter()
                .map(|x| {
                    self.kline_path
                        .clone()
                        .unwrap()
                        .join(x.to_string() + ".feather")
                })
                .collect()
        } else {
            glob(
                self.kline_path
                    .as_ref()
                    .unwrap()
                    .join("*.feather")
                    .to_str()
                    .unwrap(),
            )?
            .map(|x| x.unwrap())
            .collect()
        };
        let filter_cond = self.time_filter_cond(finder.freq.as_str())?;
        let rename_table = self.rename_table(finder.tier);
        let preprocess_exprs = get_preprocess_exprs(&self.typ, &finder.freq);
        self.dfs = all_files
            .iter()
            .map(|file| -> Result<_> {
                let mut ldf = LazyFrame::scan_ipc(
                    file,
                    ScanArgsIpc {
                        rechunk: true,
                        memory_map,
                        ..Default::default()
                    },
                )?;
                // apply rename condition
                if let Some(table) = &rename_table {
                    ldf = ldf.rename(table.keys(), table.values().map(|v| v.as_str().unwrap()));
                };
                // apply filter condition
                if let Some(cond) = filter_cond.clone() {
                    ldf = ldf.filter(cond)
                };
                ldf = ldf.with_columns(&preprocess_exprs);
                let ldf_schema = ldf.schema()?;
                if finder.freq != "tick" && ldf_schema.get_names().contains(&"close") {
                    // calculate return
                    ldf = ldf
                        .with_column(
                            (col("close") / col("close").shift(lit(1)) - lit(1)).alias("ret"),
                        )
                        .with_column(
                            when(col("ret").is_finite().and(col("ret").is_not_nan()))
                                .then("ret")
                                .otherwise(lit(NULL))
                                .alias("ret"),
                        );
                }
                Ok(ldf)
            })
            .collect::<Result<Frames>>()?;
        if self.dfs.is_empty() {
            eprintln!("No data found in the path: {:?}", self.kline_path);
        }
        if self.symbols.is_none() {
            self.symbols = Some(
                all_files
                    .into_iter()
                    .map(|x| x.file_stem().unwrap().to_str().unwrap().into())
                    .collect(),
            )
        }
        Ok(self)
    }
}
