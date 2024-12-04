use std::path::PathBuf;

use glob::glob;
use polars::prelude::*;

use crate::path_finder::{PathConfig, PathFinder};
use crate::prelude::*;
use crate::utils::get_preprocess_exprs;

const SSE_SELECT_COLUMNS: [&str; 45] = [
    "symbol",
    "trading_date",
    "time",
    "ask1",
    "ask2",
    "ask3",
    "ask4",
    "ask5",
    "ask6",
    "ask7",
    "ask8",
    "ask9",
    "ask10",
    "bid1",
    "bid2",
    "bid3",
    "bid4",
    "bid5",
    "bid6",
    "bid7",
    "bid8",
    "bid9",
    "bid10",
    "ask1_vol",
    "ask2_vol",
    "ask3_vol",
    "ask4_vol",
    "ask5_vol",
    "ask6_vol",
    "ask7_vol",
    "ask8_vol",
    "ask9_vol",
    "ask10_vol",
    "bid1_vol",
    "bid2_vol",
    "bid3_vol",
    "bid4_vol",
    "bid5_vol",
    "bid6_vol",
    "bid7_vol",
    "bid8_vol",
    "bid9_vol",
    "bid10_vol",
    // "last_price",
    // "total_count",
    "total_vol",
    "total_amt",
    // "total_buy_vol",
    // "total_sell_vol",
];

impl DataLoader {
    pub(super) fn load_sse_bond_kline(mut self, path_config: PathConfig) -> Result<Self> {
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
                    eprintln!("No sse bond data found in the path: {:?}", self.kline_path);
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
                        let mut ldf = LazyFrame::scan_ipc(&path, Default::default())?;
                        let schema = ldf.collect_schema()?;
                        if let Some(columns) = columns.as_ref() {
                            if columns.len() != schema.len() {
                                eprintln!(
                                    "{:?} columns length mismatch: {} != {}",
                                    &path,
                                    columns.len(),
                                    schema.len()
                                );
                            }
                            ldf = ldf.select([cols(columns.clone())]);
                        } else {
                            columns = Some(schema.iter_names().cloned().collect());
                        }
                        Ok(ldf)
                    })
                    .collect::<Result<_>>()?;
                self.dfs = dfs
                    .into_iter()
                    .map(|mut df| {
                        // apply rename condition
                        if let Some(table) = &rename_table {
                            df = df.rename(
                                table.keys(),
                                table.values().map(|v| v.as_str().unwrap()),
                                false,
                            );
                        };
                        // apply filter condition
                        if let Some(cond) = filter_cond.clone() {
                            df = df.filter(cond)
                        };
                        df.with_columns(&preprocess_exprs)
                            .select([cols(SSE_SELECT_COLUMNS)])
                    })
                    .collect::<Vec<_>>()
                    .into();
                self.symbols = Some(dates);
                return Ok(self);
            }
        }
        bail!("Unsupported freq: {:?} for sse bond", self.freq);
    }
}
