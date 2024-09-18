use std::ops::Index;

use anyhow::Result;
use polars::prelude::*;
use smartstring::alias::String;
use tea_strategy::tevec::export::arrow::legacy::utils::CustomIterTools;

use crate::prelude::DataLoader;

#[derive(Clone, Debug)]
pub struct Summary {
    pub facs: Vec<String>,
    pub labels: Vec<String>,

    pub symbol_ic: Vec<DataLoader>, // 每个元素是一个因子的ic，loader里面是不同symbol的ic
    pub ic_overall: Vec<DataFrame>,
    pub ts_ic: Vec<DataFrame>, // 每个表格是一个因子的时序ic，每一列是关于一个label的ic
    pub symbol_ts_group_rets: Vec<DataLoader>,
    pub ts_group_rets: Vec<DataFrame>, // 按一定时间计算的分组收益，最后再取平均(一般用于计算分组的资金曲线)
    pub symbol_group_rets: Vec<DataLoader>, // 每个因子在每个group的平均收益，尚未在品种间平均
    pub group_rets: Vec<DataFrame>,    // 每个group的平均收益
}

#[derive(Clone, Debug)]
pub struct FacSummary {
    pub fac: String,
    pub labels: Arc<Vec<String>>,
    pub symbol_ic: Option<DataLoader>,
    pub ic_overall: Option<DataFrame>,
    pub ts_ic: Option<DataFrame>,
    pub symbol_ts_group_rets: Option<DataLoader>,
    pub ts_group_rets: Option<DataFrame>,
    pub symbol_group_rets: Option<DataLoader>,
    pub group_rets: Option<DataFrame>,
}

pub struct SummaryReport(Vec<FacSummary>);

impl<'a> Index<&'a str> for SummaryReport {
    type Output = FacSummary;

    fn index(&self, index: &str) -> &Self::Output {
        let idx = self.0.iter().position(|f| &f.fac == index).unwrap();
        &self.0[idx]
    }
}

impl Index<usize> for SummaryReport {
    type Output = FacSummary;

    fn index(&self, index: usize) -> &Self::Output {
        &self.0[index]
    }
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
            symbol_group_rets: vec![],
            group_rets: vec![],
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

    pub fn finish(self) -> SummaryReport {
        let len = self.facs.len();
        let labels = Arc::new(self.labels);
        let fac_summaries = (0..len)
            .map(|i| FacSummary {
                fac: self.facs[i].clone(),
                labels: labels.clone(),
                symbol_ic: self.symbol_ic.get(i).cloned(),
                ic_overall: self.ic_overall.get(i).cloned(),
                ts_ic: self.ts_ic.get(i).cloned(),
                symbol_ts_group_rets: self.symbol_ts_group_rets.get(i).cloned(),
                ts_group_rets: self.ts_group_rets.get(i).cloned(),
                symbol_group_rets: self.symbol_group_rets.get(i).cloned(),
                group_rets: self.group_rets.get(i).cloned(),
            })
            .collect::<Vec<_>>();
        SummaryReport(fac_summaries)
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

    pub fn with_symbol_group_rets(mut self, symbol_group_rets: Vec<DataLoader>) -> Self {
        self.symbol_group_rets = symbol_group_rets;
        self
    }

    pub fn with_group_rets(mut self, group_rets: Vec<DataFrame>) -> Self {
        self.group_rets = group_rets;
        self
    }
}

fn concat_fac_res(dfs: &[DataFrame], facs: Series, expr: Expr) -> Result<DataFrame> {
    use polars::lazy::dsl::concat;
    let dfs: Vec<_> = dfs
        .iter()
        .map(|df| df.clone().lazy().select([expr.clone()]))
        .collect();
    Ok(concat(&dfs, Default::default())?
        .with_column(facs.lit().alias("fac"))
        .collect()?)
}

impl SummaryReport {
    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn labels(&self) -> &[String] {
        self[0].labels.as_ref()
    }

    pub fn fac_series(&self) -> Series {
        let facs: StringChunked = self.0.iter().map(|f| f.fac.as_str()).collect_trusted();
        facs.into_series().with_name("fac")
    }

    pub fn ts_ic(&self) -> Vec<DataFrame> {
        self.0.iter().map(|f| f.ts_ic.clone().unwrap()).collect()
    }

    pub fn ic(&self) -> Result<DataFrame> {
        concat_fac_res(&self.ts_ic(), self.fac_series(), cols(self.labels()).mean())
    }

    pub fn ic_std(&self) -> Result<DataFrame> {
        concat_fac_res(&self.ts_ic(), self.fac_series(), cols(self.labels()).std(1))
    }

    pub fn ir(&self) -> Result<DataFrame> {
        let ic_df = self.ic()?;
        let ic_std_df = self.ic_std()?;
        let ir_df = &ic_df / &ic_std_df;
        Ok(ir_df?)
    }

    pub fn ic_skew(&self) -> Result<DataFrame> {
        concat_fac_res(
            &self.ts_ic(),
            self.fac_series(),
            cols(self.labels()).skew(false),
        )
    }

    pub fn ic_kurt(&self) -> Result<DataFrame> {
        concat_fac_res(
            &self.ts_ic(),
            self.fac_series(),
            cols(self.labels()).kurtosis(true, false),
        )
    }

    fn get_ic_overall(&self) -> Vec<DataFrame> {
        self.0
            .iter()
            .map(|f| f.ic_overall.clone().unwrap())
            .collect()
    }

    pub fn ic_overall(&self) -> Result<DataFrame> {
        concat_fac_res(
            &self.get_ic_overall(),
            self.fac_series(),
            cols(self.labels()).mean(),
        )
    }

    pub fn group_rets(&self) -> Vec<DataFrame> {
        self.0
            .iter()
            .map(|f| f.group_rets.clone().unwrap())
            .collect()
    }
}

impl FacSummary {
    #[cfg(feature = "plotlars-plot")]
    pub fn plot_group(&self, label: &str, save_path: impl AsRef<std::path::Path>) -> Result<()> {
        let save_path = save_path.as_ref();
        if !save_path.parent().map(|p| p.exists()).unwrap_or(true) {
            std::fs::create_dir(save_path.parent().unwrap())?;
        }
        let df = self.group_rets.clone().unwrap();
        use plotlars::{BarPlot, Plot};
        BarPlot::builder()
            .data(&df)
            .labels("group")
            .values(label)
            .y_title(label)
            .x_title("group")
            .build()
            .write_html(save_path.to_str().unwrap());
        Ok(())
    }
}
