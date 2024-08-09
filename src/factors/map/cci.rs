use super::super::export::*;

/// CCI指标是根据统计学原理，引进价格与固定期间的股价平均区间的偏离
/// 程度的概念，强调股价平均绝对偏差在股市技术分析中的重要性，是一种
/// 比较独特的技术指标。
#[derive(FactorBase, Default, Debug, Clone)]
pub struct Cci(pub Param);

impl PlFactor for Cci {
    #[inline]
    fn try_expr(&self) -> Result<Expr> {
        let typ = TYP.try_expr()?;
        let ma = typ.clone().rolling_mean(self.0.into());
        let md = (typ.clone() - ma.clone()).abs().rolling_mean(self.0.into());
        let cci = (typ - ma) / (lit(0.015) * md);
        Ok(cci)
    }
}

#[ctor::ctor]
fn register() {
    register_pl_fac::<Cci>().unwrap()
}
