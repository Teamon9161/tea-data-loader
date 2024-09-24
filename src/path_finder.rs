use std::path::PathBuf;

use anyhow::{bail, Result};

use super::enums::{Adjust, Tier};
use crate::configs::MainPathConfig;
/// Configuration for path finding.
pub struct PathConfig {
    /// The main path configuration.
    pub(crate) config: MainPathConfig,
    /// The type of data (e.g., "future", "rf", "xbond").
    pub typ: String,
    /// The frequency of data (e.g., "daily", "min", "tick").
    pub freq: String,
    /// The tier of contracts (e.g., lead, sub-lead).
    pub tier: Tier,
    /// The adjustment method for prices.
    pub adjust: Adjust,
}

impl Default for PathConfig {
    fn default() -> Self {
        Self {
            config: MainPathConfig::default(),
            typ: "".to_string(),
            freq: "".to_string(),
            tier: Tier::None,
            adjust: Adjust::None,
        }
    }
}

impl PathConfig {
    pub fn new(typ: &str, freq: &str) -> Self {
        use crate::configs::CONFIG;
        Self {
            config: CONFIG.path_finder.clone(),
            typ: typ.to_string(),
            freq: freq.to_string(),
            ..Default::default()
        }
    }
}

/// Struct for finding and constructing file paths.
pub(crate) struct PathFinder {
    /// The main path for data files.
    pub main_path: PathBuf,
    /// The type of data.
    pub typ: String,
    /// The frequency of data.
    pub freq: String,
    /// The tier of contracts.
    pub tier: Tier,
    /// The adjustment method for prices.
    pub adjust: Adjust,
}

impl PathFinder {
    /// Creates a new PathFinder instance.
    ///
    /// # Arguments
    ///
    /// * `config` - The configuration for path finding.
    ///
    /// # Returns
    ///
    /// A Result containing the PathFinder instance or an error.
    #[inline]
    pub fn new(config: PathConfig) -> Result<Self> {
        let path_config = config.config;
        // get source type from config
        let source = path_config
            .type_source
            .get(&config.typ)
            .ok_or_else(|| anyhow::Error::msg(format!("Unknown type: {}", config.typ)))?
            .as_str()
            .unwrap();
        // get main path by source type
        let main_path = path_config
            .main_path
            .get(source)
            .ok_or_else(|| {
                anyhow::Error::msg(format!("Source: {} don't have a main path", config.typ))
            })?
            .as_str()
            .unwrap()
            .to_owned();
        Ok(Self {
            main_path: PathBuf::from(main_path),
            typ: config.typ,
            freq: config.freq,
            tier: config.tier,
            adjust: config.adjust,
        })
    }

    /// Returns the type of data.
    #[inline]
    pub fn get_typ(&self) -> &str {
        self.typ.as_str()
    }

    /// Returns the frequency of data.
    #[inline]
    pub fn get_freq(&self) -> &str {
        self.freq.as_str()
    }

    /// Constructs and returns the full path for the data file.
    ///
    /// # Returns
    ///
    /// A Result containing the PathBuf of the data file or an error.
    #[inline]
    pub fn path(&self) -> Result<PathBuf> {
        let path = match self.get_typ() {
            "future" => match self.get_freq() {
                "info" => self.main_path.join(self.get_typ()).join("info.feather"),
                "ticksize" | "tick_size" => {
                    self.main_path.join(self.get_typ()).join("ticksize.feather")
                },
                "tick_spread" => self
                    .main_path
                    .join("processed")
                    .join(self.get_typ())
                    .join("tick_to_min/base"),
                "spot" => self.main_path.join(self.get_typ()).join("future_spot.csv"),
                // 库存
                "st_stock" => self
                    .main_path
                    .join(self.get_typ())
                    .join("future_ststock_ak.csv"),
                // 米筐仓单
                "warrant" => self.main_path.join(self.get_typ()).join("warehouse_stock"),
                // 可用库存的品种列表
                "ststock_symbols" => self
                    .main_path
                    .join(self.get_typ())
                    .join("products.com.ststock.csv"),
                // 会员持仓数据
                "member_rank" => self.main_path.join(self.get_typ()).join("member_rank/hot"),
                freq_str => {
                    if freq_str.starts_with("compose_") {
                        let method = freq_str.replace("compose_", "");
                        self.main_path
                            .join(self.get_typ())
                            .join("syn_kline")
                            .join(method)
                    } else {
                        self.main_path
                            .join(self.get_typ())
                            .join(freq_str)
                            .join(self.tier.as_str())
                            .join(self.adjust.as_str())
                    }
                },
            },
            "rf" => self.main_path.join("rf.feather"),
            "xbond" => match self.get_freq() {
                "tick" => self.main_path.join("tick"),
                _ => bail!("Unknown freq: {} for xbond", self.get_freq()),
            },
            "ddb-xbond" => match self.get_freq() {
                "tick" => self.main_path.join("tick"),
                "trade" => self.main_path.join("bond_trade.feather"),
                _ => bail!("Unknown freq: {} for ddb-xbond", self.get_freq()),
            },
            "ddb-future" => match self.get_freq() {
                "tick" => self.main_path.join("tick"),
                _ => bail!("Unknown freq: {} for ddb-future", self.get_freq()),
            },
            typ => {
                if typ.starts_with("coin_") {
                    self.main_path
                        .join(typ.replace("coin_", ""))
                        .join(self.get_freq())
                } else {
                    bail!("Unknown type: {}", typ)
                }
            },
        };
        Ok(path)
    }
}
