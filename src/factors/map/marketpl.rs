use super::super::export::*;

/// 市场盈亏指标，衡量收盘价与平均成交价的关系
/// 注意量纲会受到合约乘数的影响，如果要对不同品种
/// 进行比较需要进一步的处理
#[derive(FactorBase, Default, Debug, Clone)]
pub struct MarketPl(pub Param);

impl PlFactor for MarketPl {
    #[inline]
    fn try_expr(&self) -> Result<Expr> {
        let amt_ema = AMT.expr().ts_ewm(self.0.into(), None);
        let vol_ema = VOLUME.expr().ts_ewm(self.0.into(), None);
        Ok(CLOSE.expr() * vol_ema / amt_ema)
    }
}

#[ctor::ctor]
fn register() {
    register_pl_fac::<MarketPl>().unwrap()
}
