use super::super::export::*;

/// 典型价格指标 (Typical Price)
///
/// 典型价格是一种技术分析指标，用于计算一个交易周期内的平均价格。
/// 它通常被定义为最高价、最低价和收盘价的算术平均值。
///
/// 计算公式：
/// TYP = (High + Low + Close) / 3
///
/// 在本实现中，我们还包括了开盘价，使用了四个价格点的平均值：
/// TYP = (Open + High + Low + Close) / 4
///
/// 参数：
/// - Param: 用于可能的未来扩展，目前在计算中未使用
#[derive(FactorBase, FromParam, Default, Clone, Copy)]
pub struct Typ;

impl PlFactor for Typ {
    #[inline]
    fn try_expr(&self) -> Result<Expr> {
        Ok(dsl::mean_horizontal([
            col("open"),
            col("high"),
            col("close"),
            col("low")
        ],
        true)?)
    }
}

impl TFactor for Typ {
    #[inline]
    fn eval(&self, df: &DataFrame) -> Result<Series> {
        use polars::prelude::{SumMeanHorizontal, NullStrategy};
        df.select(["close", "open", "high", "low"])?
            .mean_horizontal(NullStrategy::Ignore)?
            .map(|s| s.as_materialized_series().clone())
            .ok_or_else(|| anyhow::Error::msg("Can not find data columns"))
    }
}

#[ctor::ctor]
fn register() {
    register_fac::<Typ>().unwrap()
}
