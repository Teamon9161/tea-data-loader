use std::str::FromStr;
use std::sync::Arc;

use anyhow::{bail, Result};
use polars::prelude::*;

use crate::prelude::Params;

pub(crate) const FILTER_SYMBOL: char = '~';

/// Represents a filter used in strategy operations.
#[derive(Clone)]
pub struct Filter {
    /// The name of the filter.
    pub name: Arc<str>,
    /// The parameters associated with the filter.
    pub params: Params,
}

impl std::fmt::Debug for Filter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}_{:?}", self.name, self.params)
    }
}

impl std::fmt::Display for Filter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}_{}", self.name, self.params)
    }
}

impl FromStr for Filter {
    type Err = anyhow::Error;

    /// Parses a string into a `Filter` instance.
    ///
    /// # Arguments
    ///
    /// * `filter_name` - A string slice that contains the filter name and parameters.
    ///
    /// # Returns
    ///
    /// * `Result<Self>` - A Result containing the parsed Filter or an error.
    fn from_str(filter_name: &str) -> Result<Self> {
        let mut name_nodes: Vec<_> = filter_name.split('_').collect();
        let params: Params = name_nodes.pop().unwrap().parse()?;
        let filter_name = name_nodes.join("_");
        Ok(Filter {
            name: filter_name.into(),
            params,
        })
    }
}

impl Filter {
    /// Generates the expression for the filter.
    ///
    /// # Returns
    ///
    /// * `Result<[Expr; 2]>` - A Result containing an array of two Expr or an error.
    pub fn expr(&self) -> Result<[Expr; 2]> {
        match self.name.as_ref() {
            "trend" => Ok(self.trend(false, "close")),
            "trend_rev" => Ok(self.trend(true, "close")),
            "mid_trend" => Ok(self.trend(false, "mid")),
            "mid_trend_rev" => Ok(self.trend(true, "mid")),
            "ytm_spread" => Ok(self.ytm_spread()),
            "vol" => Ok(self.vol(false, "close")),
            "vol_rev" => Ok(self.vol(true, "close")),
            "mid_vol" => Ok(self.vol(false, "mid")),
            "mid_vol_rev" => Ok(self.vol(true, "mid")),
            _ => bail!("unsupported filter: {}", self.name),
        }
    }

    /// Generates trend-based filter expressions.
    ///
    /// # Arguments
    ///
    /// * `rev` - A boolean indicating whether to reverse the filter.
    /// * `fac` - A string slice representing the factor to use (e.g., "close" or "mid").
    ///
    /// # Returns
    ///
    /// * `[Expr; 2]` - An array of two Expr representing long open and short open conditions.
    pub fn trend(&self, rev: bool, fac: &str) -> [Expr; 2] {
        let n = self.params[0].as_usize();
        let m = if self.params.len() > 1 {
            self.params[1].as_f64()
        } else {
            0.
        };
        let middle = col(fac).rolling_mean(RollingOptionsFixedWindow {
            window_size: n,
            min_periods: n / 2,
            ..Default::default()
        });
        let width = col(fac).rolling_std(RollingOptionsFixedWindow {
            window_size: n,
            min_periods: n / 2,
            ..Default::default()
        });
        let fac = (col(fac) - middle) / width;
        if !rev {
            [fac.clone().gt_eq(m), fac.lt_eq(-m)]
        } else {
            [fac.clone().lt_eq(-m), fac.gt_eq(m)]
        }
    }

    pub fn vol(&self, rev: bool, fac: &str) -> [Expr; 2] {
        let n = self.params[0].as_usize();
        let m = if self.params.len() > 1 {
            self.params[1].as_f64()
        } else {
            0.
        };
        let vol = col(fac).rolling_std(RollingOptionsFixedWindow {
            window_size: n,
            min_periods: n / 2,
            ..Default::default()
        });
        let vol_mean = vol.clone().rolling_mean(RollingOptionsFixedWindow {
            window_size: 5 * n,
            min_periods: 5 * n / 2,
            ..Default::default()
        });
        let fac = vol - vol_mean;
        if !rev {
            [fac.clone().lt_eq(m), fac.lt_eq(m)]
        } else {
            [fac.clone().gt_eq(m), fac.gt_eq(m)]
        }
    }

    /// Generates yield-to-maturity (YTM) spread filter expressions.
    ///
    /// This function is only available when the "order-book-fac" feature is enabled.
    ///
    /// # Arguments
    ///
    /// * `self` - The current instance of the struct containing the filter parameters.
    ///
    /// # Returns
    ///
    /// * `[Expr; 2]` - An array of two identical Expr representing the YTM spread condition.
    ///
    /// # Details
    ///
    /// The function creates a condition where the YTM spread is less than or equal to
    /// a maximum value specified in the filter parameters (in basis points). This condition is then
    /// returned as both the long open and short open conditions.
    #[cfg(feature = "order-book-fac")]
    pub fn ytm_spread(&self) -> [Expr; 2] {
        use crate::factors::tick::order_book::YTM_SPREAD;
        use crate::factors::PlFactor;
        let max_ytm_spread = self.params[0].as_f64();
        let cond = (YTM_SPREAD.expr() * lit(100)).lt_eq(max_ytm_spread);
        [cond.clone(), cond]
    }
}

/// A collection of filters used in a trading strategy.
///
/// This struct represents a set of filters that can be applied to trading decisions.
/// Each filter in the collection contributes to determining when to open long or short positions.
///
/// # Fields
///
/// * `0` - A vector of `Filter` objects representing individual filtering criteria.
#[derive(Clone)]
pub struct Filters(pub Vec<Filter>);

impl std::fmt::Debug for Filters {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut s = String::new();
        for filter in self.0.iter() {
            s.push_str(&format!("{}{}", filter, FILTER_SYMBOL));
        }
        s.pop();
        write!(f, "{}", s)
    }
}

impl std::fmt::Display for Filters {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Debug::fmt(self, f)
    }
}

impl Filters {
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
        let mut long_open_cond: Option<Expr> = None;
        let mut short_open_cond: Option<Expr> = None;
        for filter in self.0.iter() {
            let [loc, soc] = filter.expr()?;
            if let Some(long_cond) = long_open_cond {
                long_open_cond = Some(long_cond.and(loc));
            } else {
                long_open_cond = Some(loc);
            }
            if let Some(short_cond) = short_open_cond {
                short_open_cond = Some(short_cond.and(soc));
            } else {
                short_open_cond = Some(soc);
            }
        }
        Ok([long_open_cond.unwrap(), short_open_cond.unwrap()])
    }
}

impl FromStr for Filters {
    type Err = anyhow::Error;

    fn from_str(filter_names: &str) -> Result<Self> {
        let filters = filter_names
            .split(FILTER_SYMBOL)
            .map(|name| {
                name.parse()
                    .map_err(|_| anyhow::anyhow!("invalid filter: {}", name))
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(Filters(filters))
    }
}
