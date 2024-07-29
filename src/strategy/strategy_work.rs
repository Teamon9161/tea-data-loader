use std::str::FromStr;

use anyhow::{bail, Result};
use polars::prelude::*;

use super::{Strategy, STRATEGY_MAP};
use crate::factors::{parse_pl_fac, Params};
use crate::prelude::{GetName, PlFactor};

// const close_filter_symbol: char = '*';
// const filter_symbol: &str = "~";
// const weight_func_symbol: &str = "@";

pub struct StrategyWork {
    pub fac: Arc<str>,
    pub strategy: Arc<dyn Strategy>,
    pub filters: Option<Arc<str>>, // params: Params,
    pub name: Option<Arc<str>>,
}

impl GetName for StrategyWork {
    #[inline]
    fn name(&self) -> String {
        if let Some(name) = &self.name {
            name.to_string()
        } else {
            format!("{}__{}", self.fac, self.strategy.name())
        }
    }
}

impl StrategyWork {
    #[inline]
    pub fn pl_fac(&self) -> Result<Arc<dyn PlFactor>> {
        parse_pl_fac(self.fac.as_ref())
    }

    #[inline]
    pub fn eval(&self, df: &DataFrame) -> Result<Series> {
        self.strategy.eval(&self.fac, df, None)
    }
}

impl FromStr for StrategyWork {
    type Err = anyhow::Error;
    fn from_str(strategy_name: &str) -> Result<Self> {
        let full_name = strategy_name;
        let (fac, strategy_name) = strategy_name.split_once("__").unwrap();
        let (strategy_name, params) =
            if let Some((strategy_name, strategy_params)) = strategy_name.split_once("_(") {
                let params = "(".to_owned() + strategy_params;
                let params: Params = params.parse()?;
                (strategy_name, params)
            } else {
                bail!("Strategy params should be a tuple")
            };
        let exists_flag = STRATEGY_MAP.lock().contains_key(strategy_name);
        if exists_flag {
            let strategy = STRATEGY_MAP.lock()[strategy_name](params);
            Ok(StrategyWork {
                fac: fac.into(),
                strategy,
                filters: None,
                name: Some(full_name.into()),
            })
        } else {
            bail!("Strategy {} not found", strategy_name);
        }
    }
}
