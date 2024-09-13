use polars::prelude::*;
use smartstring::alias::String;

use crate::prelude::DataLoader;

#[derive(Clone, Debug)]
pub struct Summary {
    pub facs: Vec<String>,
    pub labels: Vec<String>,

    pub symbol_ic: Vec<DataLoader>, // 每个元素是一个因子的ic，loader里面是不同symbol的ic
    pub ic_overall: Vec<DataFrame>,
    pub ts_ic: Vec<DataFrame>, // 每个表格是一个因子的时序ic，每一列是关于一个label的ic
    pub symbol_ts_group_rets: Vec<DataLoader>,
    pub ts_group_rets: Vec<DataFrame>, // 按一定时间计算的分组收益，最后再取平均
}

impl Default for Summary {
    fn default() -> Self {
        Self {
            facs: vec![],
            labels: vec![],
            symbol_ic: vec![],
            ic_overall: vec![],
            ts_ic: vec![],
            symbol_ts_group_rets: vec![],
            ts_group_rets: vec![],
        }
    }
}

impl Summary {
    pub fn new(facs: Vec<String>, labels: Vec<String>) -> Self {
        Self {
            facs,
            labels,
            ..Default::default()
        }
    }

    pub fn with_symbol_ic(mut self, symbol_ic: Vec<DataLoader>) -> Self {
        self.symbol_ic = symbol_ic;
        self
    }

    pub fn with_ic_overall(mut self, ic_overall: Vec<DataFrame>) -> Self {
        self.ic_overall = ic_overall;
        self
    }

    pub fn with_ts_ic(mut self, ts_ic: Vec<DataFrame>) -> Self {
        self.ts_ic = ts_ic;
        self
    }

    pub fn with_symbol_ts_group_rets(mut self, symbol_ts_group_rets: Vec<DataLoader>) -> Self {
        self.symbol_ts_group_rets = symbol_ts_group_rets;
        self
    }

    pub fn with_ts_group_rets(mut self, ts_group_rets: Vec<DataFrame>) -> Self {
        self.ts_group_rets = ts_group_rets;
        self
    }
}
