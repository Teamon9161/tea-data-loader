use dsl::GetOutput;
use polars::prelude::{DataType as PolarsDataType, *};
use tea_strategy::tevec::prelude::Vec1View;

use super::super::export::*;

#[derive(FactorBase, FromParam, Default, Clone, Copy)]
pub struct Vol(pub usize);

impl PlFactor for Vol {
    #[inline]
    fn try_expr(&self) -> Result<Expr> {
        CLOSE.std(self.0).try_expr()
    }
}

fn condition_rolling_std(fac: Expr, cond: Expr, window: usize) -> Expr {
    fac.apply_many(
        move |series| {
            let fac = series[0].as_materialized_series().cast_f64().unwrap();
            let cond = series[1].as_materialized_series().cast_bool().unwrap();
            let out: Float64Chunked = fac
                .f64()
                .unwrap()
                .rolling2_custom(
                    cond.bool().unwrap(),
                    window,
                    |fac_w, mask| fac_w.filter(&mask).unwrap().std(1),
                    None,
                )
                .unwrap();
            Ok(Some(out.into_column()))
        },
        &[cond],
        GetOutput::from_type(PolarsDataType::Float64),
    )
}

#[derive(FactorBase, FromParam, Default, Clone, Copy)]
pub struct UpVol(pub usize);

impl PlFactor for UpVol {
    #[inline]
    fn try_expr(&self) -> Result<Expr> {
        let cond = CLOSE.gt_eq(CLOSE.mean_opt(self.0, 1)).try_expr()?;
        Ok(condition_rolling_std(CLOSE.expr(), cond, self.0))
    }
}
#[derive(FactorBase, FromParam, Default, Clone, Copy)]
pub struct DownVol(pub usize);

impl PlFactor for DownVol {
    #[inline]
    fn try_expr(&self) -> Result<Expr> {
        let cond = CLOSE.lt_eq(CLOSE.mean_opt(self.0, 1)).try_expr()?;
        Ok(condition_rolling_std(CLOSE.expr(), cond, self.0))
    }
}

#[ctor::ctor]
fn register() {
    register_pl_fac::<Vol>().unwrap();
    register_pl_fac::<UpVol>().unwrap();
    register_pl_fac::<DownVol>().unwrap();
}
