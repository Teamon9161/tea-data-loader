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
#[derive(FactorBase, Default, Clone)]
pub struct Mfi(pub Param);

impl PlFactor for Mfi {
    #[inline]
    fn try_expr(&self) -> Result<Expr> {
        let tp = TYP.expr();
        let vol = VOLUME.expr();
        let tp_s = tp.clone().shift(lit(1));
        let mf_in = when(tp.clone().gt(tp_s.clone()))
            .then(tp.clone() * vol.clone())
            .otherwise(0.);
        let mf_out = when(tp.clone().lt(tp_s)).then(tp * vol).otherwise(0.);
        let mf_in = mf_in.rolling_sum(self.0.into());
        let mf_out = mf_out.rolling_sum(self.0.into());
        Ok(when(mf_out.clone().gt(EPS))
            .then(mf_in / mf_out)
            .otherwise(lit(NULL)))
    }
}

#[ctor::ctor]
fn register() {
    register_pl_fac::<Mfi>().unwrap()
}
