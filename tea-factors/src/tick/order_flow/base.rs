// use polars::prelude::DataType;

pub use super::vwap::Vwap;
use crate::export::*;

define_base_fac!(
    OrderPrice: "成交的价格",
    OrderYtm: "成交的收益率",
    OrderAmt: "成交名义金额",
    OrderVol: "成交数量",
    OrderTime: "成交的时间",
    IsBuy: "是否是买单"
);

// #[derive(FactorBase, FromParam, Default, Clone, Copy)]
// pub struct OrderAmt;

// impl PlFactor for OrderAmt {
//     #[inline]
//     fn try_expr(&self) -> Result<Expr> {
//         (ORDER_PRICE * ORDER_VOL)
//             .try_expr()
//             .map(|e| e.cast(DataType::Float64))
//     }
// }

// pub const ORDER_AMT: Factor<OrderAmt> = Factor(OrderAmt);
