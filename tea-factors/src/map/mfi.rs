use super::super::export::*;
/// 资金流量指标（Money Flow Index，MFI）
///
/// MFI是一种结合价格和成交量的动量指标，用于衡量买卖压力。它被认为是成交量加权的相对强弱指标（RSI）。
///
/// 计算公式：
/// MFI = 100 - (100 / (1 + Money Flow Ratio))
///
/// Money Flow Ratio = Positive Money Flow / Negative Money Flow
///
/// 其中：
/// - TYP = (开盘价 + 最高价 + 最低价 + 收盘价) / 4
/// - Money Flow = 典型价格 * 成交量
/// - Positive Money Flow: 当典型价格上升时的Money Flow之和
/// - Negative Money Flow: 当典型价格下降时的Money Flow之和
///
/// 指标解读：
/// - MFI > 80: 可能表示超买
/// - MFI < 20: 可能表示超卖
/// - MFI与价格的背离可能预示趋势反转
///
/// 使用注意：
/// - MFI可以用来确认趋势、预测反转和识别超买超卖区域
/// - 本实现中的典型价格计算包含了开盘价，这可能与某些传统MFI实现有所不同
#[derive(FactorBase, FromParam, Default, Clone, Copy)]
pub struct Mfi(pub usize);

impl PlFactor for Mfi {
    #[inline]
    fn try_expr(&self) -> Result<Expr> {
        let tp_s = TYP.shift(1);
        let mf_in = iif(TYP.gt(tp_s), TYP * VOLUME, 0.).sum_opt(self.0, 1);
        let mf_out = iif(TYP.lt(tp_s), TYP * VOLUME, 0.).sum_opt(self.0, 1);
        let mfi = mf_in / mf_out;
        mfi.try_expr()
    }
}

/// 成交额资金流量指标（Amount-based Money Flow Index，AmtMFI）
///
/// AmtMFI是MFI的一个变体，使用成交额（AMT）代替了传统MFI中的成交量。
///
/// 计算公式：
/// AmtMFI = Money Flow Ratio = Positive Money Flow / Negative Money Flow
///
/// 其中：
/// - Positive Money Flow: 当典型价格上升时的成交额之和
/// - Negative Money Flow: 当典型价格下降时的成交额之和
///
/// 指标解读：
/// - AmtMFI > 0.8: 可能表示超买
/// - AmtMFI < 0.2: 可能表示超卖
/// - AmtMFI与价格的背离可能预示趋势反转
#[derive(FactorBase, FromParam, Default, Clone, Copy)]
pub struct AmtMfi(pub usize);

impl PlFactor for AmtMfi {
    #[inline]
    fn try_expr(&self) -> Result<Expr> {
        let tp_s = TYP.shift(1);
        let mf_in = iif(TYP.gt(tp_s), AMT, 0.).sum_opt(self.0, 1);
        let mf_out = iif(TYP.lt(tp_s), AMT, 0.).sum_opt(self.0, 1);
        let mfi = mf_in / mf_out;
        mfi.try_expr()
    }
}

#[ctor::ctor]
fn register() {
    register_pl_fac::<Mfi>().unwrap();
    register_pl_fac::<AmtMfi>().unwrap();
}
