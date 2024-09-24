use super::super::export::*;

/// 过去n期收盘价变动比例
///
/// 计算公式：
/// Ret = (Close[t] - Close[t-n]) / Close[t-n]
///
/// 其中：
/// - Close[t]: 当前时间点的收盘价
/// - Close[t-n]: n期前的收盘价
/// - n: 回看的期数，由 Param 参数指定
///
/// 这个因子可以用来衡量股票在特定时间段内的价格变动幅度，
/// 常用于分析股票的短期或中期表现，以及在动量策略中作为信号指标。
///
/// 注意：
/// - 当 n = 1 时，即计算相邻两个交易日之间的收益率
/// - 结果以小数形式表示，例如 0.05 表示 5% 的涨幅
#[derive(FactorBase, Default, Clone)]
pub struct Ret(pub Param);

impl PlFactor for Ret {
    #[inline]
    fn try_expr(&self) -> Result<Expr> {
        Ok(CLOSE.expr().pct_change(lit(self.0.as_i32())))
    }
}

/// 过去n期的对数收益率
///
/// 计算公式：
/// LogRet = ln(Close[t] / Close[t-n])
///
/// 其中：
/// - Close[t]: 当前时间点的收盘价
/// - Close[t-n]: n期前的收盘价
/// - n: 回看的期数，由 Param 参数指定
///
/// 对数收益率相比普通收益率有以下优点：
/// 1. 可加性：多期对数收益率可以直接相加
/// 2. 对称性：涨跌幅度在数值上更加对称
/// 3. 正态分布：对数收益率通常更接近正态分布，便于统计分析
///
/// 注意：
/// - 当价格变动较小时，对数收益率近似等于普通收益率
/// - 结果以小数形式表示，需要乘以100才能得到百分比形式
#[derive(FactorBase, Default, Clone)]
pub struct LogRet(pub Param);

impl PlFactor for LogRet {
    #[inline]
    fn try_expr(&self) -> Result<Expr> {
        let close = CLOSE.expr();
        Ok((close.clone() / close.shift(lit(self.0.as_i32()))).log(f64::EPSILON))
    }
}

#[ctor::ctor]
fn register() {
    register_pl_fac::<Ret>().unwrap();
    register_pl_fac::<LogRet>().unwrap();
}
