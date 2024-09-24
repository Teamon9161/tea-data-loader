use super::super::export::*;

/// RSRS (Resistance Support Relative Strength) 指标
///
/// RSRS指标是一种用于衡量市场支撑位和阻力位相对强度的技术分析工具。
///
/// 计算原理：
/// 1. 使用最高价（High）和最低价（Low）进行线性回归
/// 2. 回归方程：High = α + β * Low + ε，其中ε ~ N(0, σ)
/// 3. β值（斜率）即为RSRS指标值
///
/// 计算步骤：
/// 1. 选取过去N个交易日的最高价和最低价数据
/// 2. 使用最小二乘法进行线性回归，得到β值
/// 3. β值即为当前的RSRS指标值
///
/// 参数说明：
/// - N: 回看期数，由Param参数指定
///
/// 指标解读：
/// - β > 1: 表示阻力较强，上涨趋势可能较强
/// - β < 1: 表示支撑较强，下跌趋势可能较强
/// - β ≈ 1: 表示支撑和阻力相当，市场可能处于盘整状态
///
/// 使用注意：
/// - RSRS指标通常需要结合其他技术指标和市场分析一起使用
/// - N值的选择会影响指标的灵敏度，较小的N值会使指标更敏感但可能产生更多噪音
/// - 可以通过标准化RSRS指标值来增强其使用效果
///
/// 优势：
/// 1. 考虑了价格的动态变化，而不仅仅是静态的价格水平
/// 2. 通过线性回归减少了单个价格点的噪音影响
/// 3. 可以反映市场的支撑和阻力的相对强弱，有助于判断趋势强度
///
/// 局限性：
/// 1. 作为滞后指标，可能在快速变化的市场中反应不够及时
/// 2. 需要合理选择参数N，以平衡指标的灵敏度和稳定性
#[derive(FactorBase, Default, Clone)]
pub struct Rsrs(pub Param);

impl PlFactor for Rsrs {
    #[inline]
    fn try_expr(&self) -> Result<Expr> {
        let rsrs = HIGH.expr().ts_regx_beta(LOW.expr(), self.0.into(), None);
        Ok(rsrs)
    }
}

#[ctor::ctor]
fn register() {
    register_pl_fac::<Rsrs>().unwrap()
}
