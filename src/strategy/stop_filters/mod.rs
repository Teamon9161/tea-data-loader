use std::str::FromStr;
use std::sync::Arc;

use anyhow::{bail, Result};
use polars::prelude::*;

use crate::prelude::Params;

pub(crate) const STOP_FILTER_SYMBOL: char = '*';

#[derive(Clone)]
/// Represents a filter used in strategy operations.
pub struct StopFilter {
    /// The name of the filter.
    pub name: Arc<str>,
    /// The parameters associated with the filter.
    pub params: Params,
}

impl std::fmt::Debug for StopFilter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}_{:?}", self.name, self.params)
    }
}

impl std::fmt::Display for StopFilter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}_{}", self.name, self.params)
    }
}

impl FromStr for StopFilter {
    type Err = anyhow::Error;
    fn from_str(filter_name: &str) -> Result<Self> {
        let mut name_nodes: Vec<_> = filter_name.split('_').collect();
        let params: Params = name_nodes.pop().unwrap().parse()?;
        let filter_name = name_nodes.join("_");
        Ok(StopFilter {
            name: filter_name.into(),
            params,
        })
    }
}

impl StopFilter {
    pub fn expr(&self) -> Result<[Expr; 2]> {
        let [long_stop_cond, short_stop_cond] = match self.name.as_ref() {
            "market_stop" => self.market_stop(),
            name => bail!("unsupported stop filter: {}", name),
        };
        Ok([long_stop_cond, short_stop_cond])
    }

    /// 处理后的平仓条件，当触发止损信号后，确保平仓条件一直持续到再次触发开仓信号
    pub fn preprocessed_expr(
        &self,
        long_open_cond: Expr,
        short_open_cond: Expr,
    ) -> Result<[Expr; 2]> {
        let [long_stop_cond, short_stop_cond] = self.expr()?;
        Ok(self.process_stop_cond(
            long_open_cond,
            short_open_cond,
            long_stop_cond,
            short_stop_cond,
        ))
    }

    /// 处理止损条件，当触发止损信号后，需要确保平仓信号一直持续到再次触发开仓信号
    fn process_stop_cond(
        &self,
        long_open_cond: Expr,
        short_open_cond: Expr,
        point_long_stop_cond: Expr,
        point_short_stop_cond: Expr,
    ) -> [Expr; 2] {
        let long_stop_cond = when(long_open_cond).then(false.lit()).otherwise(NULL.lit());
        let short_stop_cond = when(short_open_cond)
            .then(false.lit())
            .otherwise(NULL.lit());
        let long_stop_cond = when(point_long_stop_cond)
            .then(true.lit())
            .otherwise(long_stop_cond)
            .forward_fill(None);
        let short_stop_cond = when(point_short_stop_cond)
            .then(true.lit())
            .otherwise(short_stop_cond)
            .forward_fill(None);
        [long_stop_cond, short_stop_cond]
    }

    /// 在当天收盘前n期平仓
    pub fn market_stop(&self) -> [Expr; 2] {
        let n = self.params[0].as_i32();
        let stop_cond = col("trading_date").neq(col("trading_date").shift((-n).lit()));
        [stop_cond.clone(), stop_cond]
    }
}

#[derive(Clone)]
pub struct StopFilters(pub Vec<StopFilter>);

impl std::fmt::Debug for StopFilters {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut s = String::new();
        for filter in self.0.iter() {
            s.push_str(&format!("{}{}", filter, STOP_FILTER_SYMBOL));
        }
        s.pop();
        write!(f, "{}", s)
    }
}

impl std::fmt::Display for StopFilters {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Debug::fmt(self, f)
    }
}

impl StopFilters {
    /// Generates the combined expression for all filters in the collection.
    ///
    /// This method iterates through all filters in the collection and combines their
    /// expressions using logical AND operations. It produces separate expressions
    /// for long and short open conditions.
    ///
    /// # Returns
    ///
    /// * `Result<[Expr; 2]>` - An array containing two `Expr`:
    ///   - The first `Expr` represents the combined condition for opening long positions.
    ///   - The second `Expr` represents the combined condition for opening short positions.
    ///
    /// # Errors
    ///
    /// This method will return an error if any of the individual filter expressions fail to generate.
    pub fn expr(&self) -> Result<[Expr; 2]> {
        let mut long_stop_cond: Option<Expr> = None;
        let mut short_stop_cond: Option<Expr> = None;
        // TODO：不同filter应该有不同的逻辑连接符，不一定均为or
        for filter in self.0.iter() {
            let [lsc, ssc] = filter.expr()?;
            if let Some(long_cond) = long_stop_cond {
                long_stop_cond = Some(long_cond.or(lsc));
            } else {
                long_stop_cond = Some(lsc);
            }
            if let Some(short_cond) = short_stop_cond {
                short_stop_cond = Some(short_cond.or(ssc));
            } else {
                short_stop_cond = Some(ssc);
            }
        }
        Ok([long_stop_cond.unwrap(), short_stop_cond.unwrap()])
    }
}

impl FromStr for StopFilters {
    type Err = anyhow::Error;

    fn from_str(filter_names: &str) -> Result<Self> {
        let filters = filter_names
            .split(STOP_FILTER_SYMBOL)
            .map(|name| {
                name.parse()
                    .map_err(|_| anyhow::anyhow!("invalid stop filter: {}", name))
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(StopFilters(filters))
    }
}
