use super::super::export::*;

#[derive(FactorBase, Default, Debug, Clone)]
pub struct Efficiency(pub Param);

impl PlFactor for Efficiency {
    #[inline]
    fn try_expr(&self) -> Result<Expr> {
        CLOSE.efficiency(self.0).try_expr()
    }
}

#[derive(FactorBase, Default, Debug, Clone)]
pub struct EfficiencySign(pub Param);

impl PlFactor for EfficiencySign {
    #[inline]
    fn try_expr(&self) -> Result<Expr> {
        CLOSE.efficiency_sign(self.0).try_expr()
    }
}

#[ctor::ctor]
fn register() {
    register_pl_fac::<Efficiency>().unwrap();
    register_pl_fac::<EfficiencySign>().unwrap();
}
