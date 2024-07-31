use std::str::FromStr;

use anyhow::{bail, Result};
use polars::lazy::dsl;
use polars::prelude::*;

use super::{Strategy, STRATEGY_MAP};
use crate::factors::{parse_pl_fac, Params};
use crate::prelude::{GetName, PlFactor};
use crate::strategy::Filter;

// const close_filter_symbol: char = '*';
const FILTER_SYMBOL: char = '~';
// const weight_func_symbol: &str = "@";

pub struct StrategyWork {
    pub fac: Arc<str>,
    pub strategy: Arc<dyn Strategy>,
    pub filters: Option<Filter>, // params: Params,
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
        let open_filter_expr = self.filters.as_ref().map(|f| f.expr()).transpose()?;
        let filters = open_filter_expr.map(|filters| {
            [
                filters[0].clone(),
                dsl::repeat(false, dsl::len()),
                filters[1].clone(),
                dsl::repeat(false, dsl::len()),
            ]
        });
        self.strategy.eval(&self.fac, df, filters)
    }
}

impl FromStr for StrategyWork {
    type Err = anyhow::Error;
    fn from_str(strategy_name: &str) -> Result<Self> {
        let full_name = strategy_name;
        let (fac, mut strategy_name) = strategy_name.split_once("__").unwrap();
        // parse open pos filter
        let filters = if strategy_name.contains(FILTER_SYMBOL) {
            let (name, filters) = strategy_name.split_once(FILTER_SYMBOL).unwrap();
            strategy_name = name;
            Some(filters.parse()?)
        } else {
            None
        };
        // parse strategy and strategy params
        let (strategy_name, params) =
            if let Some((strategy_name, strategy_params)) = strategy_name.split_once("_(") {
                let params = "(".to_owned() + strategy_params;
                let params: Params = params.parse()?;
                (strategy_name, params)
            } else {
                bail!("Strategy params should be a tuple")
            };
        if let Some(strategy) = STRATEGY_MAP.lock().get(strategy_name) {
            let strategy = strategy(params);
            Ok(StrategyWork {
                fac: fac.into(),
                strategy,
                filters,
                name: Some(full_name.into()),
            })
        } else {
            bail!("Strategy {} not found", strategy_name);
        }
    }
}
