use anyhow::Result;
use polars::prelude::*;

use crate::prelude::{GetName, Params};

pub trait StrategyBase: Sized {
    fn strategy_name() -> Arc<str>;

    fn new<P: Into<Params>>(params: P) -> Self;
}

pub trait Strategy: GetName + Send + Sync + 'static {
    fn eval_to_fac(&self, fac: &Series, filters: Option<DataFrame>) -> Result<Series>;

    fn eval(&self, fac: &str, df: &DataFrame, filters: Option<[Expr; 4]>) -> Result<Series> {
        let fac = df.column(fac)?.clone();
        if let Some(filters) = filters {
            let filters = [
                filters[0].clone().alias("__long_open"),
                filters[1].clone().alias("__long_close"),
                filters[2].clone().alias("__short_open"),
                filters[3].clone().alias("__short_close"),
            ];
            let filters = df.clone().lazy().select(filters).collect()?;
            self.eval_to_fac(&fac, Some(filters))
        } else {
            self.eval_to_fac(&fac, None)
        }
    }
}
