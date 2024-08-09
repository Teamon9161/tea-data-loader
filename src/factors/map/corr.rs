use super::super::export::*;

/// 价格和成交量相关系数
#[derive(FactorBase, Default, Debug, Clone)]
pub struct PVCorr(pub Param);

impl PlFactor for PVCorr {
    #[inline]
    fn try_expr(&self) -> Result<Expr> {
        Ok(dsl::rolling_corr(
            CLOSE.expr(),
            VOLUME.expr(),
            self.0.into(),
        ))
    }
}

/// 收益率和成交量变动相关系数
#[derive(FactorBase, Default, Debug, Clone)]
pub struct PrVrCorr(pub Param);

impl PlFactor for PrVrCorr {
    #[inline]
    fn try_expr(&self) -> Result<Expr> {
        Ok(dsl::rolling_corr(
            CLOSE.expr().pct_change(lit(1)),
            VOLUME.expr().pct_change(lit(1)),
            self.0.into(),
        ))
    }
}

/// 收益率和成交量的相关系数
#[derive(FactorBase, Default, Debug, Clone)]
pub struct PrVCorr(pub Param);

impl PlFactor for PrVCorr {
    #[inline]
    fn try_expr(&self) -> Result<Expr> {
        Ok(dsl::rolling_corr(
            CLOSE.expr().pct_change(lit(1)),
            VOLUME.expr(),
            self.0.into(),
        ))
    }
}

/// 价格和成交量变动的相关系数
#[derive(FactorBase, Default, Debug, Clone)]
pub struct PVrCorr(pub Param);

impl PlFactor for PVrCorr {
    #[inline]
    fn try_expr(&self) -> Result<Expr> {
        Ok(dsl::rolling_corr(
            CLOSE.expr(),
            VOLUME.expr().pct_change(lit(1)),
            self.0.into(),
        ))
    }
}

#[ctor::ctor]
fn register() {
    register_pl_fac::<PVCorr>().unwrap();
    register_pl_fac::<PrVrCorr>().unwrap();
    register_pl_fac::<PrVCorr>().unwrap();
    register_pl_fac::<PVrCorr>().unwrap();
}
