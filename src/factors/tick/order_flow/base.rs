pub use super::vwap::Vwap;
use crate::factors::export::*;

define_base_fac!(
    OrderPrice: "成交的价格",
    OrderYtm: "成交的收益率",
    OrderAmt: "成交名义金额",
    OrderTime: "成交的时间",
    IsBuy: "是否是买单"
);
