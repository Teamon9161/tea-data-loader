use std::path::PathBuf;

use glob::glob;
use polars::prelude::*;

use crate::path_finder::{PathConfig, PathFinder};
use crate::prelude::*;
use crate::utils::get_preprocess_exprs;

impl DataLoader {
    /// Loads kline data for xbond.
    ///
    /// # Arguments
    ///
    /// * `path_config` - The path configuration for the data.
    /// * `memory_map` - Whether to use memory mapping when reading files.
    /// * `concat` - Whether to concatenate the loaded dataframes.
    ///
    /// # Returns
    ///
    /// Returns a `Result<Self>` containing the updated `DataLoader`.
    pub(super) fn load_xbond_kline(
        mut self,
        path_config: PathConfig,
        memory_map: bool,
        concat: bool,
    ) -> Result<Self> {
        let finder = PathFinder::new(path_config)?;
        self.kline_path = Some(finder.path()?);
        if let Some(freq) = self.freq.as_deref() {
            if freq == "tick" {
                let all_files: Vec<PathBuf> = glob(
                    self.kline_path
                        .as_ref()
                        .unwrap()
                        .join("*.feather")
                        .to_str()
                        .unwrap(),
                )?
                .map(|x| x.unwrap())
                .collect();
                if all_files.is_empty() {
                    eprintln!("No xbond data found in the path: {:?}", self.kline_path);
                }
                let filter_cond = self.time_filter_cond(finder.freq.as_str())?;
                let rename_table = self.rename_table(finder.tier);
                let preprocess_exprs = get_preprocess_exprs(&self.typ, &finder.freq);
                let mut columns: Option<Vec<_>> = None;
                let dates: Vec<Arc<str>> = all_files
                    .iter()
                    .map(|x| x.file_stem().unwrap().to_str().unwrap().into())
                    .collect::<Vec<_>>();
                let dfs: Vec<_> = all_files
                    .into_iter()
                    .map(|path| -> Result<_> {
                        let mut ldf = LazyFrame::scan_ipc(
                            &path,
                            ScanArgsIpc {
                                rechunk: true,
                                memory_map,
                                ..Default::default()
                            },
                        )?;
                        let schema = ldf.schema()?;
                        if let Some(columns) = columns.as_ref() {
                            if columns.len() != schema.len() {
                                eprintln!(
                                    "{:?} columns length mismatch: {} != {}",
                                    &path,
                                    columns.len(),
                                    schema.len()
                                );
                            }
                            ldf = ldf.select([cols(columns)]);
                        } else {
                            columns = Some(schema.iter_names().cloned().collect());
                        }
                        Ok(ldf)
                    })
                    .try_collect()?;
                if concat {
                    let mut df = dsl::concat(
                        dfs,
                        UnionArgs {
                            rechunk: true,
                            ..Default::default()
                        },
                    )?;
                    // apply rename condition
                    if let Some(table) = &rename_table {
                        df = df.rename(table.keys(), table.values().map(|v| v.as_str().unwrap()));
                    };
                    df = df.sort(["time", "symbol"], Default::default());
                    // apply filter condition
                    if let Some(cond) = filter_cond.clone() {
                        df = df.filter(cond)
                    };
                    self.dfs = vec![df.with_columns(&preprocess_exprs)].into();
                } else {
                    self.dfs = dfs
                        .into_iter()
                        .map(|mut df| {
                            // apply rename condition
                            if let Some(table) = &rename_table {
                                df = df.rename(
                                    table.keys(),
                                    table.values().map(|v| v.as_str().unwrap()),
                                );
                            };
                            // apply filter condition
                            if let Some(cond) = filter_cond.clone() {
                                df = df.filter(cond)
                            };
                            df.with_columns(&preprocess_exprs)
                        })
                        .collect::<Vec<_>>()
                        .into();
                    self.symbols = Some(dates);
                }
                return Ok(self);
            }
        }
        bail!("Unsupported freq: {:?} for xbond", self.freq);
    }
}
