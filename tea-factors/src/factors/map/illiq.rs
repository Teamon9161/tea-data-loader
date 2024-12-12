use super::super::export::*;
use super::Ret;

/// 非流动性指标（Illiquidity）
///
/// Illiq 是一种用于衡量资产流动性的指标，由 Amihud (2002) 提出。
/// 它衡量了每单位交易量对价格的影响程度，值越高表示流动性越低。
///
/// 计算公式：
/// Illiq = |R| / (AMT * 1e8)
///
/// 其中：
/// - R: 资产在给定周期内的收益率
/// - AMT: 资产在给定周期内的成交额
/// - 1e8: 缩放因子，用于调整数值大小
///
/// 指标解读：
/// - 较高的 Illiq 值表示较低的流动性
/// - 较低的 Illiq 值表示较高的流动性
///
/// 使用注意：
/// - Illiq 值可能会随时间和不同市场条件而变化
/// - 应该与其他指标结合使用，以获得更全面的流动性评估
#[derive(FactorBase, FromParam, Default, Clone, Copy)]
pub struct Illiq(pub usize);

impl PlFactor for Illiq {
    #[inline]
    fn try_expr(&self) -> Result<Expr> {
        // (Ret(1).abs() / AMT * 1e8).mean(self.0).try_expr()
        (Ret(self.0 as i64).abs() / AMT.sum(self.0) * 1e8).try_expr()
    }
}

/// 带符号的非流动性指标（Signed Illiquidity）
///
/// IlliqSign 是 Illiq 的一个变体，保留了收益率的符号信息。
/// 这个指标不仅反映了流动性，还包含了价格变动的方向。
///
/// 计算公式：
/// IlliqSign = R / (AMT * 1e8)
///
/// 其中：
/// - R: 资产在给定周期内的收益率（保留符号）
/// - AMT: 资产在给定周期内的成交额
/// - 1e8: 缩放因子，用于调整数值大小
///
/// 指标解读：
/// - 正值表示在正收益率下的非流动性
/// - 负值表示在负收益率下的非流动性
/// - 绝对值越大表示流动性越低
///
/// 使用注意：
/// - IlliqSign 可以提供比 Illiq 更多的信息，因为它考虑了价格变动的方向
/// - 在分析市场微观结构或流动性对价格影响时特别有用
#[derive(FactorBase, FromParam, Default, Clone, Copy)]
pub struct IlliqSign(pub usize);

impl PlFactor for IlliqSign {
    #[inline]
    fn try_expr(&self) -> Result<Expr> {
        // (Ret::fac(1) / AMT * 1e8).mean(self.0).try_expr()
        (Ret::fac(self.0) / AMT.sum(self.0) * 1e8).try_expr()
    }
}

#[ctor::ctor]
fn register() {
    register_pl_fac::<Illiq>().unwrap();
    register_pl_fac::<IlliqSign>().unwrap();
}
