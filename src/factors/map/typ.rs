use super::super::export::*;

/// 典型价格指标
#[derive(FactorBase, Default, Debug, Clone)]
pub struct Typ(pub Param);

impl PlFactor for Typ {
    #[inline]
    fn try_expr(&self) -> Result<Expr> {
        Ok(dsl::mean_horizontal([
            col("open"),
            col("high"),
            col("close"),
            col("low"),
        ])?)
    }
}

impl TFactor for Typ {
    #[inline]
    fn eval(&self, df: &DataFrame) -> Result<Series> {
        use polars::frame::NullStrategy;
        df.select(["close", "open", "high", "low"])?
            .mean_horizontal(NullStrategy::Ignore)?
            .ok_or_else(|| anyhow::Error::msg("Can not find data columns"))
    }
}

#[ctor::ctor]
fn register() {
    register_fac::<Typ>().unwrap()
}
