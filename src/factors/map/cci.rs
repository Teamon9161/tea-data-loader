use super::super::export::*;

/// 商品通道指数（Commodity Channel Index，CCI）
///
/// CCI指标是根据统计学原理，引进价格与固定期间的股价平均区间的偏离
/// 程度的概念，强调股价平均绝对偏差在股市技术分析中的重要性，是一种
/// 比较独特的技术指标。
///
/// 计算公式：
/// CCI = (TYP - MA) / (0.015 * MD)
///
/// 其中：
/// - TYP: 典型价格，通常为（最高价 + 最低价 + 收盘价）/ 3
/// - MA: TYP的N日简单移动平均
/// - MD: TYP的N日平均绝对偏差
/// - N: 计算周期，由Param参数指定
///
/// 指标解读：
/// - CCI > 100: 表示超买
/// - CCI < -100: 表示超卖
/// - CCI由负转正: 可能预示着上涨趋势
/// - CCI由正转负: 可能预示着下跌趋势
///
/// 使用注意：
/// - CCI对于发现周期性的市场极端具有重要意义
/// - 可以用来辨别市场趋势的强弱、超买超卖，以及可能的反转点
#[derive(FactorBase, FromParam, Default, Clone, Copy)]
pub struct Cci(pub usize);

impl PlFactor for Cci {
    #[inline]
    fn try_expr(&self) -> Result<Expr> {
        let ma = TYP.mean_opt(self.0, 1);
        let md = (TYP - ma).abs().mean(self.0);
        let cci = (TYP - ma) / (md * 0.015);
        cci.try_expr()
    }
}

#[ctor::ctor]
fn register() {
    register_pl_fac::<Cci>().unwrap()
}
