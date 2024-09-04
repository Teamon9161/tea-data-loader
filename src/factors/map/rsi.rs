use super::super::export::*;

/// RSI 指标
///
/// CLOSEUP=IF(CLOSE>REF(CLOSE,1),CLOSE-REF(CLOSE,1),0)
/// CLOSEDOWN=IF(CLOSE<REF(CLOSE,1),ABS(CLOSE-REF(CLOSE,1)),0)
/// CLOSEUP_MA=SMA(CLOSEUP,N,1)
/// CLOSEDOWN_MA=SMA(CLOSEDOWN,N,1)
/// RSI=100*CLOSEUP_MA/(CLOSEUP_MA+CLOSEDOWN_MA)
///
/// RSI 反映一段时间内平均收益与平均亏损的对比。通常认为当RSI大于70,
/// 市场处于强势上涨甚至达到超买的状态;当RSI小于30,市场处于强势下跌甚至达到超卖的状态。
/// 当RSI跌到30以下又上穿30时,通常认为股价要从超卖的状态反弹;
/// 当RSI超过70又下穿70时,通常认为市场要从超买的状态回落了
#[derive(FactorBase, Default, Debug, Clone)]
pub struct Rsi(pub Param);

impl PlFactor for Rsi {
    #[inline]
    fn try_expr(&self) -> Result<Expr> {
        let diff = CLOSE.expr().diff(1, Default::default());
        let up = when(diff.clone().gt(0)).then(diff.clone()).otherwise(0);
        let down = when(diff.clone().lt(0)).then(diff.abs()).otherwise(0);
        let up_ma = up.rolling_mean(self.0.into());
        let down_ma = down.rolling_mean(self.0.into());
        Ok(up_ma.clone() / (up_ma + down_ma))
    }
}

#[ctor::ctor]
fn register() {
    register_pl_fac::<Rsi>().unwrap()
}
