use super::super::export::*;

/// Williams %R 指标 (Williams Percent Range)
///
/// Williams %R 是一个动量指标，用于衡量当前收盘价在最近 N 个交易日的高低价格范围内的相对位置。
///
/// 计算公式：
/// WR = (HH - C) / (HH - LL) * -100
///
/// 其中：
/// WR: Williams %R 值
/// HH: N 日内的最高价
/// LL: N 日内的最低价
/// C: 当前收盘价
/// N: 回看周期（通常为 14 天）
///
/// 指标解读：
/// - 0 到 -20：被认为是超买区
/// - -80 到 -100：被认为是超卖区
/// - -50：被视为中性
///
/// Williams %R 主要通过最高价、最低价和收盘价之间的关系，来判断股市的超买超卖现象，
/// 预测股价中短期的走势。它利用振荡点来反映市场的超买超卖行为，分析多空双方力量的对比，
/// 从而提出有效的信号来研判市场中短期行为的走势。
///
/// 注意：
/// - Williams %R 与 Stochastic Oscillator（随机指标）非常相似，但计算方式略有不同
/// - 在强势趋势市场中，指标可能长期处于超买或超卖区域
#[derive(FactorBase, FromParam, Default, Clone, Copy)]
pub struct Wr(pub usize);

impl PlFactor for Wr {
    #[inline]
    fn try_expr(&self) -> Result<Expr> {
        let hh = HIGH.max(self.0);
        let ll = LOW.min(self.0);
        let rsv = (CLOSE - ll) / (hh - ll);
        rsv.try_expr()
    }
}

#[ctor::ctor]
fn register() {
    register_pl_fac::<Wr>().unwrap()
}
