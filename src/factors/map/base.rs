use super::super::export::*;
use super::{Ret, Typ};

define_base_fac!(
    Open: "开盘价，代表每个交易周期的起始价格。",
    High: "最高价，代表每个交易周期内的最高交易价格。",
    Low: "最低价，代表每个交易周期内的最低交易价格。",
    Close: "收盘价，代表每个交易周期的结束价格。",
    Volume: "成交量，代表每个交易周期内的交易数量。",
    Amt: "成交额，代表每个交易周期内的交易金额。"
);

/// 典型价格
pub const TYP: Typ = Typ(Param::None);

/// 收益率
pub const RET: Ret = Ret(Param::None);
