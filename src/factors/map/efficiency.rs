use super::super::export::*;

/// 效率因子（Efficiency Factor）
///
/// 效率因子用于衡量价格在一定时间段内的变动效率。它反映了价格变动的直接性和一致性。
///
/// 计算公式：
/// Efficiency = |Close[t] - Close[t-n]| / Sum(|Close[i] - Close[i-1]|, i=t-n+1 to t)
///
/// 其中：
/// - Close[t]: 当前收盘价
/// - Close[t-n]: n期前的收盘价
/// - n: 计算周期，由 Param 参数指定
///
/// 指标解读：
/// - 取值范围：[0, 1]
/// - 接近1：表示价格在该时间段内呈现强劲的单向趋势（可能是上升或下降）
/// - 接近0：表示价格在该时间段内波动较大或无明显趋势
///
/// 使用注意：
/// - 效率因子可以用来识别趋势的强度和持续性
/// - 可以与其他技术指标结合使用，如移动平均线或动量指标
/// - 不同市场和不同时间框架可能需要调整参数和解读标准
/// - 该指标不区分上升趋势和下降趋势，只反映趋势的强度
#[derive(FactorBase, Default, Clone)]
pub struct Efficiency(pub Param);

impl PlFactor for Efficiency {
    #[inline]
    fn try_expr(&self) -> Result<Expr> {
        CLOSE.efficiency(self.0).try_expr()
    }
}

/// 带符号的效率因子（Efficiency Factor with Sign）
///
/// 带符号的效率因子在普通效率因子的基础上增加了方向信息，可以区分上升趋势和下降趋势。
///
/// 计算公式：
/// EfficiencySign = (Close[t] - Close[t-n]) / Sum(|Close[i] - Close[i-1]|, i=t-n+1 to t)
///
/// 其中：
/// - Close[t]: 当前收盘价
/// - Close[t-n]: n期前的收盘价
/// - n: 计算周期，由 Param 参数指定
///
/// 指标解读：
/// - 取值范围：[-1, 1]
/// - 正值：表示上升趋势，数值越接近1，趋势越强
/// - 负值：表示下降趋势，数值越接近-1，趋势越强
/// - 接近0：表示价格在该时间段内波动较大或无明显趋势
///
/// 使用注意：
/// - 相比普通效率因子，带符号的效率因子可以提供趋势方向的信息
/// - 可以用来识别趋势的强度、持续性和方向
/// - 在交易策略中，可以根据正负值来判断买入或卖出信号
#[derive(FactorBase, Default, Clone)]
pub struct EfficiencySign(pub Param);

impl PlFactor for EfficiencySign {
    #[inline]
    fn try_expr(&self) -> Result<Expr> {
        CLOSE.efficiency_sign(self.0).try_expr()
    }
}

#[ctor::ctor]
fn register() {
    register_pl_fac::<Efficiency>().unwrap();
    register_pl_fac::<EfficiencySign>().unwrap();
}
