use std::str::FromStr;
use std::sync::Arc;

use anyhow::{bail, Result};
use polars::prelude::*;

use crate::prelude::Params;

pub struct Filter {
    pub name: Arc<str>,
    pub params: Params,
}

impl FromStr for Filter {
    type Err = anyhow::Error;
    fn from_str(strategy_name: &str) -> Result<Self> {
        let name_nodes: Vec<_> = strategy_name.split('_').collect();
        let params: Params = name_nodes[name_nodes.len() - 1].parse()?;
        let filter_name = name_nodes[..name_nodes.len() - 1].join("_");
        Ok(Filter {
            name: filter_name.into(),
            params,
        })
    }
}

impl Filter {
    pub fn expr(&self) -> Result<[Expr; 2]> {
        match self.name.as_ref() {
            "trend" => Ok(self.trend(false, "close")),
            "trend_rev" => Ok(self.trend(true, "close")),
            "mid_trend" => Ok(self.trend(false, "mid")),
            "mid_trend_rev" => Ok(self.trend(true, "mid")),
            _ => bail!("unsupported filter: {}", self.name),
        }
    }

    /// return long open & short_open conditions
    pub fn trend(&self, rev: bool, fac: &str) -> [Expr; 2] {
        let n = self.params[0].as_i32() as usize;
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
}
