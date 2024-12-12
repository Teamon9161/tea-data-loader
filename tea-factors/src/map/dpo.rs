use super::super::export::*;

/// 去趋势价格震荡指标（Detrended Price Oscillator，DPO）
///
/// DPO是一种用于识别价格周期和超买超卖条件的技术指标。它通过消除长期趋势来突出短期价格波动。
///
/// 计算公式：
/// DPO = 收盘价 / 移动平均线的收盘价
///
/// 其中：
/// - 收盘价：当前周期的收盘价
/// - 移动平均线的收盘价：(N/2 + 1)个周期之前的收盘价的移动平均
/// - N：DPO的周期，由 Param 参数指定
///
/// 指标解读：
/// - DPO > 0：当前价格高于移动平均线，可能表示上升趋势
/// - DPO < 0：当前价格低于移动平均线，可能表示下降趋势
/// - DPO 在 0 附近波动：价格可能处于横盘整理状态
#[derive(FactorBase, FromParam, Default, Clone, Copy)]
pub struct Dpo(pub usize);

impl PlFactor for Dpo {
    #[inline]
    fn try_expr(&self) -> Result<Expr> {
        let dpo = CLOSE / CLOSE.shift(self.0 as i64 / 2 + 1) - 1;
        dpo.try_expr()
    }
}

#[ctor::ctor]
fn register() {
    register_pl_fac::<Dpo>().unwrap()
}
