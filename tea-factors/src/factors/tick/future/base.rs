pub(crate) use super::{AskCumVolF, BidCumVolF};
use super::{MidF, SpreadF};
use crate::factors::export::*;

define_base_fac!(
    // 挂单价格
    Ask1F: "期货卖一价，代表订单簿中最低的卖出价格。",
    Bid1F: "期货买一价，代表订单簿中最高的买入价格。",
    Ask2F: "期货卖二价，代表订单簿中第二低的卖出价格。",
    Bid2F: "期货买二价，代表订单簿中第二高的买入价格。",
    Ask3F: "期货卖三价，代表订单簿中第三低的卖出价格。",
    Bid3F: "期货买三价，代表订单簿中第三高的买入价格。",
    Ask4F: "期货卖四价，代表订单簿中第四低的卖出价格。",
    Bid4F: "期货买四价，代表订单簿中第四高的买入价格。",
    Ask5F: "期货卖五价，代表订单簿中第五低的卖出价格。",
    Bid5F: "期货买五价，代表订单簿中第五高的买入价格。",

    // 挂单量
    Ask1VolF: "期货卖一价对应的挂单量。",
    Bid1VolF: "期货买一价对应的挂单量。",
    Ask2VolF: "期货卖二价对应的挂单量。",
    Bid2VolF: "期货买二价对应的挂单量。",
    Ask3VolF: "期货卖三价对应的挂单量。",
    Bid3VolF: "期货买三价对应的挂单量。",
    Ask4VolF: "期货卖四价对应的挂单量。",
    Bid4VolF: "期货买四价对应的挂单量。",
    Ask5VolF: "期货卖五价对应的挂单量。",
    Bid5VolF: "期货买五价对应的挂单量。"
);

pub const MID_F: Factor<MidF> = Factor(MidF);
pub const SPREAD_F: Factor<SpreadF> = Factor(SpreadF);

/// Represents the ask (sell) price at a specific level in the order book.
///
/// The `Param` field specifies the level (1-5) of the ask price to retrieve.
#[derive(FactorBase, FromParam, Default, Clone, Copy)]
pub struct AskF(pub usize);

impl PlFactor for AskF {
    fn try_expr(&self) -> Result<Expr> {
        match self.0 {
            1 => Ok(ASK1_F.expr()),
            2 => Ok(ASK2_F.expr()),
            3 => Ok(ASK3_F.expr()),
            4 => Ok(ASK4_F.expr()),
            5 => Ok(ASK5_F.expr()),
            p => bail!("AskF level must be 1,2,3,4,5, find {}", p),
        }
    }
}

/// Represents the bid (buy) price at a specific level in the order book.
///
/// The `Param` field specifies the level (1-5) of the bid price to retrieve.
#[derive(FactorBase, FromParam, Default, Clone, Copy)]
pub struct BidF(pub usize);

impl PlFactor for BidF {
    fn try_expr(&self) -> Result<Expr> {
        match self.0 {
            1 => Ok(BID1_F.expr()),
            2 => Ok(BID2_F.expr()),
            3 => Ok(BID3_F.expr()),
            4 => Ok(BID4_F.expr()),
            5 => Ok(BID5_F.expr()),
            p => bail!("BidF level must be 1,2,3,4,5, find {}", p),
        }
    }
}
