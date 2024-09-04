use super::super::export::*;

/// 相对强弱指标（Relative Strength Index，RSI）
///
/// RSI是一种动量指标，用于衡量价格变动的速度和变化。它通过比较一定时期内价格上涨和下跌的幅度来计算。
///
/// 计算公式：
///
/// CLOSEUP = IF(CLOSE > REF(CLOSE,1), CLOSE - REF(CLOSE,1), 0)
///
/// CLOSEDOWN = IF(CLOSE < REF(CLOSE,1), ABS(CLOSE - REF(CLOSE,1)), 0)
///
/// CLOSEUP_MA = SMA(CLOSEUP, N, 1)
///
/// CLOSEDOWN_MA = SMA(CLOSEDOWN, N, 1)
///
/// RSI = 100 * CLOSEUP_MA / (CLOSEUP_MA + CLOSEDOWN_MA)
///
/// 其中：
/// - CLOSE: 当前收盘价
/// - REF(CLOSE,1): 前一期收盘价
/// - N: 计算周期，由Param参数指定
/// - SMA: 简单移动平均
///
/// 指标解读：
/// - RSI取值范围：[0, 100]
/// - RSI > 70: 可能表示市场处于超买状态，价格可能会回落
/// - RSI < 30: 可能表示市场处于超卖状态，价格可能会反弹
/// - RSI = 50: 表示市场处于中性状态
///
/// 使用注意：
/// - RSI反映一段时间内平均收益与平均亏损的对比
/// - 当RSI从低于30上穿30时，可能是买入信号
/// - 当RSI从高于70下穿70时，可能是卖出信号
/// - RSI还可以用来判断趋势强度和寻找背离
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
