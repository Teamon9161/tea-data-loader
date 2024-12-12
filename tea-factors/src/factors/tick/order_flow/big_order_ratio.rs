use polars::prelude::*;

#[allow(unused_imports)]
use super::is_simple_order_tier;
use crate::factors::export::*;

#[derive(FactorBase, FromParam, Default, Clone)]
pub struct BigOrderRatio(pub usize);

impl PlFactor for BigOrderRatio {
    fn try_expr(&self) -> Result<Expr> {
        let is_big_order = is_simple_order_tier(super::SimpleOrderTier::Big);
        // let is_big_order = ORDER_AMT.expr().gt_eq(col("order_amt_quantile_0.3"));
        let big_order_vol = (ORDER_AMT * iif(is_big_order.fac(), 1, 0)).sum_opt(self.0, 1);
        let all_vol = ORDER_AMT.sum_opt(self.0, 1);
        let ratio = big_order_vol / all_vol;
        ratio.try_expr()
    }
}

#[derive(Default, FactorBase, Clone, Copy)]
pub struct AggBigOrderRatio;

impl PlAggFactor for AggBigOrderRatio {
    #[inline]
    fn agg_expr(&self) -> Result<Expr> {
        let is_big_order = is_simple_order_tier(super::SimpleOrderTier::Big);
        let big_order_vol = (ORDER_AMT * iif(is_big_order.fac(), 1, 0)).expr().sum();
        let all_vol = ORDER_AMT.expr().sum();
        let ratio = big_order_vol.protect_div(all_vol);
        Ok(ratio)
    }
}

#[ctor::ctor]
fn register() {
    register_pl_fac::<BigOrderRatio>().unwrap()
}
