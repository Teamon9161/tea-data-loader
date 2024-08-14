use std::path::PathBuf;

use anyhow::{bail, Result};

use super::enums::{Adjust, Tier};
use crate::configs::MainPathConfig;

pub struct PathConfig {
    pub(crate) config: MainPathConfig,
    pub typ: String,
    pub freq: String,
    pub tier: Tier,
    pub adjust: Adjust,
}

pub(crate) struct PathFinder {
    pub main_path: PathBuf,
    pub typ: String,
    pub freq: String,
    pub tier: Tier,
    pub adjust: Adjust,
}

impl PathFinder {
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

    #[inline]
    pub fn get_typ(&self) -> &str {
        self.typ.as_str()
    }

    #[inline]
    pub fn get_freq(&self) -> &str {
        self.freq.as_str()
    }

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
