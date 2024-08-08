use super::super::export::*;

#[derive(FactorBase, Default, Debug)]
pub struct Bias(pub Param);

impl PlFactor for Bias {
    #[inline]
    fn try_expr(&self) -> Result<Expr> {
        let ma = col("close").rolling_mean(self.0.into());
        let bias = col("close") / ma - lit(1.);
        Ok(bias)
    }
}

#[ctor::ctor]
fn register() {
    register_pl_fac::<Bias>().unwrap()
}
