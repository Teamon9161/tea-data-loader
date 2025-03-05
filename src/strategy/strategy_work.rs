use std::str::FromStr;

use anyhow::{Result, bail};
use polars::lazy::dsl;
use polars::prelude::*;

use super::filters::FILTER_SYMBOL;
use super::stop_filters::STOP_FILTER_SYMBOL;
use super::{STRATEGY_MAP, Strategy};
use crate::factors::{GetName, Params, parse_pl_fac};
use crate::prelude::PlFactor;
use crate::strategy::{Filters, StopFilters};
// const weight_func_symbol: &str = "@";

/// Represents a strategy work unit that combines a factor, strategy, and optional filters.
pub struct StrategyWork {
    /// The factor used in the strategy, represented as an `Arc<str>`.
    pub fac: Arc<str>,
    /// The strategy to be applied, represented as an `Arc<dyn Strategy>`.
    pub strategy: Arc<dyn Strategy>,
    /// Optional filters to be applied to the strategy, represented as `Option<Filters>`.
    pub filters: Option<Filters>,
    /// Optional stop filters to be applied to the strategy, represented as `Option<Filters>`.
    pub stop_filters: Option<StopFilters>,
    /// Optional name for the strategy work, represented as `Option<Arc<str>>`.
    pub name: Option<Arc<str>>,
}

impl std::fmt::Debug for StrategyWork {
    /// Returns the name of the strategy work.
    ///
    /// If a custom name is set, it returns that name.
    /// Otherwise, it combines the factor and strategy names, or just returns the strategy name if there's no factor.
    #[inline]
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let name = if let Some(name) = &self.name {
            format!("{}", name)
        } else if !self.is_null_fac() {
            format!("{}__{}", self.fac, self.strategy.name())
        } else {
            self.strategy.name()
        };
        let name = if let Some(filters) = &self.filters {
            format!("{}{}{}", name, FILTER_SYMBOL, filters)
        } else {
            name
        };
        let name = if let Some(stop_filters) = &self.stop_filters {
            format!("{}{}{}", name, STOP_FILTER_SYMBOL, stop_filters)
        } else {
            name
        };
        write!(f, "{}", name)
    }
}

impl GetName for StrategyWork {}

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
        let stop_filter_expr = self.stop_filters.as_ref().map(|f| f.expr()).transpose()?;
        let filters = match (open_filter_expr, stop_filter_expr) {
            (Some(open_filters), Some(stop_filters)) => Some([
                open_filters[0].clone(),
                stop_filters[0].clone(),
                open_filters[1].clone(),
                stop_filters[1].clone(),
            ]),
            (Some(open_filters), None) => Some([
                open_filters[0].clone(),
                dsl::repeat(false, dsl::len()),
                open_filters[1].clone(),
                dsl::repeat(false, dsl::len()),
            ]),
            (None, Some(stop_filters)) => Some([
                dsl::repeat(true, dsl::len()),
                stop_filters[0].clone(),
                dsl::repeat(true, dsl::len()),
                stop_filters[1].clone(),
            ]),
            (None, None) => None,
        };
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
        // parse stop filters
        let stop_filters = if strategy_name.contains(STOP_FILTER_SYMBOL) {
            let (name, stop_filters) = strategy_name.split_once(STOP_FILTER_SYMBOL).unwrap();
            strategy_name = name;
            Some(stop_filters.parse()?)
        } else {
            None
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
                stop_filters,
                name: Some(full_name.into()),
            })
        } else {
            bail!("Strategy {} not found", strategy_name);
        }
    }
}
