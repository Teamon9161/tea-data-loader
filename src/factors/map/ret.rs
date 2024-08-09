use super::super::export::*;

/// 过去n期收盘价变动比例
#[derive(FactorBase, Default, Debug, Clone)]
pub struct Ret(pub Param);

impl PlFactor for Ret {
    #[inline]
    fn try_expr(&self) -> Result<Expr> {
        Ok(CLOSE.expr().pct_change(lit(self.0.as_i32())))
    }
}

/// 过去n期的对数收益率
#[derive(FactorBase, Default, Debug, Clone)]
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
