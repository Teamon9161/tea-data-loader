use super::Mid;
pub(crate) use super::{AskCumVol, BidCumVol};
use crate::factors::export::*;

define_base_fac!(
    // 挂单价格
    Ask1: "卖一价，代表订单簿中最低的卖出价格。",
    Bid1: "买一价，代表订单簿中最高的买入价格。",
    Ask2: "卖二价，代表订单簿中第二低的卖出价格。",
    Bid2: "买二价，代表订单簿中第二高的买入价格。",
    Ask3: "卖三价，代表订单簿中第三低的卖出价格。",
    Bid3: "买三价，代表订单簿中第三高的买入价格。",
    Ask4: "卖四价，代表订单簿中第四低的卖出价格。",
    Bid4: "买四价，代表订单簿中第四高的买入价格。",
    Ask5: "卖五价，代表订单簿中第五低的卖出价格。",
    Bid5: "买五价，代表订单簿中第五高的买入价格。",

    // 挂单量
    Ask1Vol: "卖一价对应的挂单量。",
    Bid1Vol: "买一价对应的挂单量。",
    Ask2Vol: "卖二价对应的挂单量。",
    Bid2Vol: "买二价对应的挂单量。",
    Ask3Vol: "卖三价对应的挂单量。",
    Bid3Vol: "买三价对应的挂单量。",
    Ask4Vol: "卖四价对应的挂单量。",
    Bid4Vol: "买四价对应的挂单量。",
    Ask5Vol: "卖五价对应的挂单量。",
    Bid5Vol: "买五价对应的挂单量。",

    // 挂单ytm
    Ask1Ytm: "卖一的ytm报价",
    Bid1Ytm: "买一的ytm报价",
    Ask2Ytm: "卖二的ytm报价",
    Bid2Ytm: "买二的ytm报价",
    Ask3Ytm: "卖三的ytm报价",
    Bid3Ytm: "买三的ytm报价",
    Ask4Ytm: "卖四的ytm报价",
    Bid4Ytm: "买四的ytm报价",
    Ask5Ytm: "卖五的ytm报价"
);

pub const MID: Mid = Mid(Param::None);
