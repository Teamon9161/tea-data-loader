use polars::prelude::*;

use super::ORDER_AMT;
use crate::factors::export::*;

#[derive(Clone, Copy, Debug)]
pub enum OrderTier {
    UltraLarge,
    Large,
    MediumLarge,
    MediumSmall,
    Small,
    Micro,
}

pub fn is_order_tier(tier: OrderTier) -> Expr {
    match tier {
        OrderTier::UltraLarge => ORDER_AMT.expr().gt_eq(col("order_amt_quantile_0.9")),
        OrderTier::Large => ORDER_AMT
            .expr()
            .lt(col("order_amt_quantile_0.9"))
            .and(ORDER_AMT.expr().gt_eq(col("order_amt_quantile_0.8"))),
        OrderTier::MediumLarge => ORDER_AMT
            .expr()
            .lt(col("order_amt_quantile_0.8"))
            .and(ORDER_AMT.expr().gt_eq(col("order_amt_quantile_0.5"))),
        OrderTier::MediumSmall => ORDER_AMT
            .expr()
            .lt(col("order_amt_quantile_0.5"))
            .and(ORDER_AMT.expr().gt_eq(col("order_amt_quantile_0.3"))),
        OrderTier::Small => ORDER_AMT
            .expr()
            .lt(col("order_amt_quantile_0.3"))
            .and(ORDER_AMT.expr().gt_eq(col("order_amt_quantile_0.2"))),
        OrderTier::Micro => ORDER_AMT.expr().lt(col("order_amt_quantile_0.2")),
    }
}

#[derive(Clone, Copy, Debug)]
pub enum SimpleOrderTier {
    Big,
    Small,
}

pub fn is_simple_order_tier(tier: SimpleOrderTier) -> Expr {
    match tier {
        SimpleOrderTier::Big => ORDER_AMT.expr().gt_eq(col("order_amt_quantile_0.9")),
        SimpleOrderTier::Small => ORDER_AMT.expr().lt(col("order_amt_quantile_0.2")),
    }
}
