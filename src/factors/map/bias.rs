use super::super::export::*;

/// 乖离率反转因子, 价格偏离均线的比例。
#[derive(FactorBase, Default, Debug, Clone)]
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
