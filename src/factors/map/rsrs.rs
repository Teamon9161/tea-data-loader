use super::super::export::*;

/// 我们用相对位置变化程度，即类似`delta(high)/delta(low)`的值来
/// 描述支撑位与阻力位的相对强度，即最低价每变动1的时候，最高价变动
/// 的幅度。实际上，`delta(high)/delta(low)`是连接高低价格平面上的两
/// 点 (low[0], high[0]) 与 (low[11, high[1]) 的斜率。由于市场量价本身
/// 噪音的存在,通过两点得到的斜率也包含了太大的噪音。我们考虑通过最
/// 近N个 (low, high) 的数据点来得到信噪比较高的最高最低价相对变化程度，
/// 自然而然的想法即是使用线性回归。如果我们建立如下般最高价与最低价
/// 之间的线性模型
/// `high = alpha + beta*low + epsilon, epsilon ~N(0,sigma)`
/// 那么 beta 值就是我们所需要的斜率。其中N的取法不能太小，不然不能过
/// 滤掉足够多的噪音;但也不能太大，因为我们希望得到的是体现目前市场的
/// 支撑阻力相对强度，若取值太大，则滞后性太高。
#[derive(FactorBase, Default, Debug, Clone)]
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
