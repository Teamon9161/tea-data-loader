use polars::prelude::*;

use crate::factors::export::*;

#[derive(FactorBase, FromParam, Default, Clone, Copy)]
pub struct ShapeVolImb;

fn get_ask_mean() -> impl FactorBase + PlFactor + Copy {
    (Ask::fac(1) * AskVol(1)
        + Ask::fac(2) * AskVol(2)
        + Ask::fac(3) * AskVol(3)
        + Ask::fac(4) * AskVol(4)
        + Ask::fac(5) * AskVol(5))
        / AskCumVol(5)
}

fn get_bid_mean() -> impl FactorBase + PlFactor + Copy {
    (Bid::fac(1) * BidVol(1)
        + Bid::fac(2) * BidVol(2)
        + Bid::fac(3) * BidVol(3)
        + Bid::fac(4) * BidVol(4)
        + Bid::fac(5) * BidVol(5))
        / BidCumVol(5)
}

impl PlFactor for ShapeVolImb {
    fn try_expr(&self) -> Result<Expr> {
        let ask_mean = get_ask_mean();
        let bid_mean = get_bid_mean();
        let ask_vol = (ASK1_VOL * (ASK1 - ask_mean).pow(2)
            + ASK2_VOL * (ASK2 - ask_mean).pow(2)
            + ASK3_VOL * (ASK3 - ask_mean).pow(2)
            + ASK4_VOL * (ASK4 - ask_mean).pow(2)
            + ASK5_VOL * (ASK5 - ask_mean).pow(2))
            / AskCumVol(5);
        let bid_vol = (BID1_VOL * (BID1 - bid_mean).pow(2)
            + BID2_VOL * (BID2 - bid_mean).pow(2)
            + BID3_VOL * (BID3 - bid_mean).pow(2)
            + BID4_VOL * (BID4 - bid_mean).pow(2)
            + BID5_VOL * (BID5 - bid_mean).pow(2))
            / BidCumVol(5);
        ask_vol.imb(bid_vol).try_expr()
    }
}

#[derive(FactorBase, FromParam, Default, Clone, Copy)]
pub struct ShapeSkewImb;

impl PlFactor for ShapeSkewImb {
    fn try_expr(&self) -> Result<Expr> {
        let ask_mean = get_ask_mean();
        let bid_mean = get_bid_mean();
        let ask_vol = (ASK1_VOL * (ASK1 - ask_mean).pow(3)
            + ASK2_VOL * (ASK2 - ask_mean).pow(3)
            + ASK3_VOL * (ASK3 - ask_mean).pow(3)
            + ASK4_VOL * (ASK4 - ask_mean).pow(3)
            + ASK5_VOL * (ASK5 - ask_mean).pow(3))
            / AskCumVol(5);
        let bid_vol = (BID1_VOL * (BID1 - bid_mean).pow(3)
            + BID2_VOL * (BID2 - bid_mean).pow(3)
            + BID3_VOL * (BID3 - bid_mean).pow(3)
            + BID4_VOL * (BID4 - bid_mean).pow(3)
            + BID5_VOL * (BID5 - bid_mean).pow(3))
            / BidCumVol(5);
        ask_vol.imb(bid_vol).try_expr()
    }
}

#[ctor::ctor]
fn register() {
    register_pl_fac::<ShapeVolImb>().unwrap();
    register_pl_fac::<ShapeSkewImb>().unwrap();
}
