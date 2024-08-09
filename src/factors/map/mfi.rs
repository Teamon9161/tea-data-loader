use super::super::export::*;
use crate::tevec::prelude::EPS;

/// 资金流不平衡指标
#[derive(FactorBase, Default, Debug, Clone)]
pub struct Mfi(pub Param);

impl PlFactor for Mfi {
    #[inline]
    fn try_expr(&self) -> Result<Expr> {
        let tp = TYP.expr();
        let vol = VOLUME.expr();
        let tp_s = tp.clone().shift(lit(1));
        let mf_in = when(tp.clone().gt(tp_s.clone()))
            .then(tp.clone() * vol.clone())
            .otherwise(0.);
        let mf_out = when(tp.clone().lt(tp_s)).then(tp * vol).otherwise(0.);
        let mf_in = mf_in.rolling_sum(self.0.into());
        let mf_out = mf_out.rolling_sum(self.0.into());
        Ok(when(mf_out.clone().gt(EPS))
            .then(mf_in / mf_out)
            .otherwise(lit(NULL)))
    }
}

#[ctor::ctor]
fn register() {
    register_pl_fac::<Mfi>().unwrap()
}
