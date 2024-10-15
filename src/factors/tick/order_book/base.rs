pub(crate) use super::{AskCumVol, BidCumVol};
use super::{Mid, MidYtm, Spread, YtmSpread};
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
    Ask5Ytm: "卖五的ytm报价",
    Bid5Ytm: "买五的ytm报价"
);

pub const MID: Factor<Mid> = Factor(Mid);
pub const MID_YTM: Factor<MidYtm> = Factor(MidYtm);
pub const SPREAD: Factor<Spread> = Factor(Spread);
pub const YTM_SPREAD: Factor<YtmSpread> = Factor(YtmSpread);

/// Represents the ask (sell) price at a specific level in the order book.
///
/// The `Param` field specifies the level (1-5) of the ask price to retrieve.
#[derive(FactorBase, FromParam, Default, Clone, Copy)]
pub struct Ask(pub usize);

impl PlFactor for Ask {
    fn try_expr(&self) -> Result<Expr> {
        match self.0 {
            1 => Ok(ASK1.expr()),
            2 => Ok(ASK2.expr()),
            3 => Ok(ASK3.expr()),
            4 => Ok(ASK4.expr()),
            5 => Ok(ASK5.expr()),
            p => bail!("level must be 1,2,3,4,5, find {}", p),
        }
    }
}
/// Represents the ask (sell) volume at a specific level in the order book.
///
/// The `Param` field specifies the level (1-5) of the ask volume to retrieve.
#[derive(FactorBase, FromParam, Default, Clone, Copy)]
pub struct AskVol(pub usize);

impl PlFactor for AskVol {
    fn try_expr(&self) -> Result<Expr> {
        match self.0 {
            1 => Ok(ASK1_VOL.expr()),
            2 => Ok(ASK2_VOL.expr()),
            3 => Ok(ASK3_VOL.expr()),
            4 => Ok(ASK4_VOL.expr()),
            5 => Ok(ASK5_VOL.expr()),
            p => bail!("level must be 1,2,3,4,5, find {}", p),
        }
    }
}

/// Represents the bid (buy) price at a specific level in the order book.
///
/// The `Param` field specifies the level (1-5) of the bid price to retrieve.
#[derive(FactorBase, FromParam, Default, Clone, Copy)]
pub struct Bid(pub usize);

impl PlFactor for Bid {
    fn try_expr(&self) -> Result<Expr> {
        match self.0 {
            1 => Ok(BID1.expr()),
            2 => Ok(BID2.expr()),
            3 => Ok(BID3.expr()),
            4 => Ok(BID4.expr()),
            5 => Ok(BID5.expr()),
            p => bail!("level must be 1,2,3,4,5, find {}", p),
        }
    }
}

/// Represents the bid (buy) volume at a specific level in the order book.
///
/// The `Param` field specifies the level (1-5) of the bid volume to retrieve.
#[derive(FactorBase, FromParam, Default, Clone, Copy)]
pub struct BidVol(pub usize);

impl PlFactor for BidVol {
    fn try_expr(&self) -> Result<Expr> {
        match self.0 {
            1 => Ok(BID1_VOL.expr()),
            2 => Ok(BID2_VOL.expr()),
            3 => Ok(BID3_VOL.expr()),
            4 => Ok(BID4_VOL.expr()),
            5 => Ok(BID5_VOL.expr()),
            p => bail!("level must be 1,2,3,4,5, find {}", p),
        }
    }
}
