use polars::prelude::*;

use crate::factors::Params;

const close_filter_symbol: char = '*';
const filter_symbol: &str = "~";
const weight_func_symbol: &str = "@";

pub struct StrategyWork<'a> {
    fac: &'a str,
    strategy: &'a str,
    params: Params,
}

impl<'a> StrategyWork<'a> {}
