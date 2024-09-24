use super::super::export::*;

/// 乖离率（Bias）指标
///
/// 乖离率是一种用于衡量价格偏离移动平均线程度的技术指标。它反映了当前价格与某一期移动平均线之间的偏离程度。
///
/// 计算公式：
/// Bias = (Close - MA) / MA * 100%
///
/// 其中：
/// - Close: 当前收盘价
/// - MA: N日移动平均线
/// - N: 计算移动平均线的周期，由 Param 参数指定
///
/// 指标解读：
/// - 正值：表示价格高于移动平均线，可能处于上升趋势
/// - 负值：表示价格低于移动平均线，可能处于下降趋势
/// - 绝对值越大：表示价格偏离均线越远，可能存在回归均线的趋势
///
/// 使用注意：
/// - 乖离率常用于判断市场的超买超卖状态
#[derive(FactorBase, Default, Clone)]
pub struct Bias(pub Param);

impl PlFactor for Bias {
    #[inline]
    fn try_expr(&self) -> Result<Expr> {
        let close = CLOSE.try_expr()?;
        let ma = close.clone().rolling_mean(self.0.into());
        let bias = close / ma - lit(1.);
        Ok(bias)
    }
}

#[ctor::ctor]
fn register() {
    register_pl_fac::<Bias>().unwrap()
}
