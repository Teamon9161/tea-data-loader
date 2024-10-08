use super::super::export::*;

/// 市场盈亏比率（Market Profit-Loss Ratio，MPL）
///
/// 这个因子衡量当前收盘价与平均成交价的关系，可以用来评估市场参与者的整体盈亏状况。
///
/// 计算公式：
/// MPL = Close * EMA(Volume, N) / EMA(Amount, N)
///
/// 其中：
/// - Close: 当前收盘价
/// - EMA(Volume, N): 成交量的N期指数移动平均
/// - EMA(Amount, N): 成交额的N期指数移动平均
/// - N: 计算EMA的周期，由Param参数指定
///
/// 指标解读：
/// - MPL > 1: 表示当前价格高于平均成交价，市场整体处于盈利状态
/// - MPL < 1: 表示当前价格低于平均成交价，市场整体处于亏损状态
/// - MPL = 1: 表示当前价格等于平均成交价，市场整体处于盈亏平衡状态
///
/// 使用注意：
/// - 该指标的量纲会受到合约乘数的影响，如果要对不同品种进行比较，需要进行标准化处理
/// - 可以用来判断市场情绪和潜在的支撑/阻力位
/// - 结合其他技术指标使用，可以提供更全面的市场分析
#[derive(FactorBase, FromParam, Default, Clone, Copy)]
pub struct Mpl(pub usize);

impl PlFactor for Mpl {
    #[inline]
    fn try_expr(&self) -> Result<Expr> {
        let amt_ema = AMT.ewm(self.0);
        let vol_ema = VOLUME.ewm(self.0);
        (CLOSE * vol_ema / amt_ema).try_expr()
    }
}

#[ctor::ctor]
fn register() {
    register_pl_fac::<Mpl>().unwrap()
}
