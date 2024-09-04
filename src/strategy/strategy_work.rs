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

/// Represents a strategy work unit that combines a factor, strategy, and optional filters.
pub struct StrategyWork {
    /// The factor used in the strategy, represented as an `Arc<str>`.
    pub fac: Arc<str>,
    /// The strategy to be applied, represented as an `Arc<dyn Strategy>`.
    pub strategy: Arc<dyn Strategy>,
    /// Optional filters to be applied to the strategy, represented as `Option<Filter>`.
    pub filters: Option<Filter>,
    /// Optional name for the strategy work, represented as `Option<Arc<str>>`.
    pub name: Option<Arc<str>>,
}

impl GetName for StrategyWork {
    /// Returns the name of the strategy work.
    ///
    /// If a custom name is set, it returns that name.
    /// Otherwise, it combines the factor and strategy names, or just returns the strategy name if there's no factor.
    #[inline]
    fn name(&self) -> String {
        if let Some(name) = &self.name {
            name.to_string()
        } else if !self.is_null_fac() {
            format!("{}__{}", self.fac, self.strategy.name())
        } else {
            self.strategy.name()
        }
    }
}

impl StrategyWork {
    /// Checks if the factor is null (empty).
    #[inline]
    pub fn is_null_fac(&self) -> bool {
        (*self.fac).is_empty()
    }

    /// Parses the factor into a `PlFactor` if it's not null.
    #[inline]
    pub fn pl_fac(&self) -> Result<Option<Arc<dyn PlFactor>>> {
        if !self.is_null_fac() {
            parse_pl_fac(self.fac.as_ref()).map(Some)
        } else {
            Ok(None)
        }
    }

    /// Evaluates the strategy on the given DataFrame.
    ///
    /// This method applies the strategy, considering any filters, to the input DataFrame.
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

    /// Parses a string into a `StrategyWork` instance.
    ///
    /// The string should be in the format: "factor__strategy_name_(params)~filters".
    /// Each component is optional except for the strategy name and params.
    fn from_str(strategy_name: &str) -> Result<Self> {
        let full_name = strategy_name;
        let (fac, mut strategy_name) =
            if let Some((fac, strategy_name)) = strategy_name.split_once("__") {
                (fac, strategy_name)
            } else {
                ("", strategy_name)
            };
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
