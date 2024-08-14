use std::path::PathBuf;

use anyhow::bail;
use glob::glob;
use polars::prelude::*;
use toml::{Table, Value};

use super::super::utils::{get_filter_cond, get_preprocess_exprs};
use crate::path_finder::{PathConfig, PathFinder};
use crate::prelude::*;

#[derive(Clone, Debug, Copy)]
pub struct KlineOpt<'a> {
    freq: &'a str,
    tier: Option<Tier>,
    adjust: Option<Adjust>,
    memory_map: bool,
    concat_tick_df: bool,
}

impl Default for KlineOpt<'_> {
    fn default() -> Self {
        Self {
            freq: "daily",
            tier: None,
            adjust: None,
            memory_map: true,
            concat_tick_df: false,
        }
    }
}

impl<'a> KlineOpt<'a> {
    fn with_default_tier(mut self, typ: &str) -> Self {
        if self.tier.is_none() {
            let tier = match typ {
                "future" => Tier::Lead,
                _ => Tier::None,
            };
            self.tier = Some(tier);
        }
        self
    }

    fn with_default_adjust(mut self, typ: &str) -> Self {
        if self.adjust.is_none() {
            let adjust = match typ {
                "future" => {
                    if self.tier.is_none() {
                        self = self.with_default_adjust(typ);
                    }
                    let tier = self.tier.unwrap();
                    if tier != Tier::SubLead {
                        Adjust::Pre
                    } else {
                        Adjust::None
                    }
                },
                _ => Adjust::None,
            };
            self.adjust = Some(adjust);
        }
        self
    }

    #[inline]
    pub fn path_config(&self, typ: &str) -> PathConfig {
        let opt = self.with_default_tier(typ).with_default_adjust(typ);
        PathConfig {
            config: CONFIG.path_finder.clone(),
            typ: typ.to_string(),
            freq: self.freq.into(),
            tier: opt.tier.unwrap(),
            adjust: opt.adjust.unwrap(),
        }
    }

    #[inline]
    pub fn new(freq: &'a str) -> Self {
        Self {
            freq,
            ..Default::default()
        }
    }

    #[inline]
    pub fn freq(freq: &'a str) -> Self {
        Self {
            freq,
            ..Default::default()
        }
    }
}

impl DataLoader {
    #[inline]
    pub fn time_filter_cond(&self, freq: &str) -> Result<Option<Expr>> {
        if freq == "min" || freq.contains("compose_") {
            Ok(get_filter_cond(self.start, self.end, "trading_date"))
        } else if freq == "daily" {
            Ok(get_filter_cond(self.start, self.end, "date"))
        } else if freq == "tick" {
            Ok(get_filter_cond(self.start, self.end, "time"))
        } else {
            bail!("Unsupported freq: {}", freq)
        }
    }

    #[inline]
    pub fn rename_table(&self, tier: Tier) -> Option<Table> {
        let rename_config = &CONFIG.loader.rename;
        parse_rename_config(
            rename_config,
            Some(self.typ.as_ref()),
            Some(self.freq.as_deref().unwrap()),
            Some(tier.as_str()),
        )
    }

    fn load_xbond_kline(
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
                let preprocess_exprs = get_preprocess_exprs(&self.typ);
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
                    )?
                    .sort(["time", "SECURITYID"], Default::default());
                    // apply rename condition
                    if let Some(table) = &rename_table {
                        df = df.rename(table.keys(), table.values().map(|v| v.as_str().unwrap()));
                    };
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

    fn load_future_kline(mut self, path_config: PathConfig, memory_map: bool) -> Result<Self> {
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
        let preprocess_exprs = get_preprocess_exprs(&self.typ);
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
                ldf = ldf
                    .with_column((col("close") / col("close").shift(lit(1)) - lit(1)).alias("ret"))
                    .with_column(
                        when(col("ret").is_finite().and(col("ret").is_not_nan()))
                            .then("ret")
                            .otherwise(lit(NULL))
                            .alias("ret"),
                    );
                Ok(ldf)
            })
            .try_collect()?;
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

    pub fn kline(mut self, opt: KlineOpt) -> Result<Self> {
        let path_config = opt.path_config(&self.typ);
        self.freq = Some(opt.freq.into());
        match self.typ.as_ref() {
            "future" => self.load_future_kline(path_config, opt.memory_map),
            "xbond" => self.load_xbond_kline(path_config, opt.memory_map, opt.concat_tick_df),
            _ => bail!("Load Unsupported typ: {:?} kline", self.typ),
        }
    }
}

#[inline]
fn get_rename_map(config: Option<&Value>) -> Option<Table> {
    if let Some(config) = config {
        if let Some(config) = config.as_table() {
            let mut map = Table::new();
            for (k, v) in config.iter() {
                if !v.is_table() {
                    map.insert(k.clone(), v.clone());
                }
            }
            if !map.is_empty() {
                return Some(map);
            }
        }
    }
    None
}

fn parse_rename_config(
    config: &Table,
    typ: Option<&str>,
    freq: Option<&str>,
    tier: Option<&str>,
) -> Option<Table> {
    match (typ, freq, tier) {
        (Some(typ), Some(freq), Some(tier)) => {
            if let Some(map) = config.get(typ) {
                if let Some(map) = map.get(freq) {
                    if let Some(map) = map.get(tier) {
                        map.as_table().cloned()
                    } else {
                        parse_rename_config(config, Some(typ), Some(freq), None)
                    }
                } else {
                    parse_rename_config(config, Some(typ), None, None)
                }
            } else {
                parse_rename_config(config, None, None, None)
            }
        },
        (Some(typ), Some(freq), None) => {
            if let Some(map) = config.get(typ) {
                if let Some(map) = get_rename_map(map.get(freq)) {
                    Some(map.clone())
                } else {
                    parse_rename_config(config, Some(typ), None, None)
                }
            } else {
                parse_rename_config(config, None, None, None)
            }
        },
        (Some(typ), None, None) => get_rename_map(config.get(typ)),
        _ => None,
    }
}
