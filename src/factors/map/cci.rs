use super::super::export::*;

#[derive(FactorBase, Default, Debug)]
pub struct Cci(pub Param);

impl PlFactor for Cci {
    #[inline]
    fn try_expr(&self) -> Result<Expr> {
        use super::typ::Typ;
        let typ = Typ(Param::None).try_expr()?;
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
