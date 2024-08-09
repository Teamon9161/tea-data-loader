use super::super::export::*;

/// 主要通过最低价和收盘价之间的关系，来判断股市的超买超卖现象，
/// 预测股价中短期的走势。它主要是利用振荡点来反映市场的超
/// 买超卖行为，分析多空双方力量的对比，从而提出有效的信号
/// 来研判市场中短期行为的走势。
#[derive(FactorBase, Default, Debug, Clone)]
pub struct Wr(pub Param);

impl PlFactor for Wr {
    #[inline]
    fn try_expr(&self) -> Result<Expr> {
        let hh = HIGH.expr().rolling_max(self.0.into());
        let ll = LOW.expr().rolling_min(self.0.into());
        let rsv = when(hh.clone().gt(ll.clone()))
            .then((CLOSE.expr() - ll.clone()) / (hh - ll))
            .otherwise(lit(NULL));
        Ok(rsv)
    }
}

#[ctor::ctor]
fn register() {
    register_pl_fac::<Wr>().unwrap()
}
