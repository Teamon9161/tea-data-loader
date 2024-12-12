use polars::prelude::DataType;

use crate::export::*;

pub struct AverageVol;

impl std::fmt::Debug for AverageVol {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "AverageVol")
    }
}

impl GetName for AverageVol {}

impl PlAggFactor for AverageVol {
    fn agg_fac_name(&self) -> Option<String> {
        None
    }

    fn agg_fac_expr(&self) -> Result<Option<Expr>> {
        Ok(None)
    }

    fn agg_expr(&self) -> Result<Expr> {
        let order_count = ORDER_VOL.agg(FactorAggMethod::Count);
        Ok(col(ORDER_VOL.name()).cast(DataType::Float64).sum() / order_count.agg_expr()?)
    }
}
