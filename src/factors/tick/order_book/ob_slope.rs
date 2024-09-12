use polars::prelude::*;

use crate::factors::export::*;

/// Represents the slope of the order book.
///
/// This factor calculates the slope of the order book by comparing the volume
/// and price differences between bid and ask levels. The slope provides insight
/// into the liquidity and depth of the order book.
///
/// The `Param` field determines how many levels of the order book are considered:
/// - If `None`, only the first level (best bid and ask) is used.
/// - If `Some(n)`, where n is 1 to 5, it uses the first n levels of the order book.
#[derive(FactorBase, Default, Debug, Clone)]
pub struct ObSlope(pub Param);

impl PlFactor for ObSlope {
    fn try_expr(&self) -> Result<Expr> {
        let b1v = BID1VOL.expr();
        let b2v = BID2VOL.expr();
        let b3v = BID3VOL.expr();
        let b4v = BID4VOL.expr();
        let b5v = BID5VOL.expr();
        let a1v = ASK1VOL.expr();
        let a2v = ASK2VOL.expr();
        let a3v = ASK3VOL.expr();
        let a4v = ASK4VOL.expr();
        let a5v = ASK5VOL.expr();
        let b1 = BID1.expr();
        let b2 = BID2.expr();
        let b3 = BID3.expr();
        let b4 = BID4.expr();
        let b5 = BID5.expr();
        let a1 = ASK1.expr();
        let a2 = ASK2.expr();
        let a3 = ASK3.expr();
        let a4 = ASK4.expr();
        let a5 = ASK5.expr();
        if self.0.is_none() {
            return Ok((b1v - a1v).protect_div(b1 - a1));
        }
        let param = self.0.as_u32();

        match param {
            1 => Ok((b1v - a1v).protect_div(b1 - a1)),
            2 => Ok((b1v - a1v + b2v - a2v).protect_div(b1 - a1 + b2 - a2)),
            3 => Ok((b1v - a1v + b2v - a2v + b3v - a3v).protect_div(b1 - a1 + b2 - a2 + b3 - a3)),
            4 => Ok((b1v - a1v + b2v - a2v + b3v - a3v + b4v - a4v)
                .protect_div(b1 - a1 + b2 - a2 + b3 - a3 + b4 - a4)),
            5 => Ok((b1v - a1v + b2v - a2v + b3v - a3v + b4v - a4v + b5v - a5v)
                .protect_div(b1 - a1 + b2 - a2 + b3 - a3 + b4 - a4 + b5 - a5)),
            _ => bail!("invalid param for ObSlope: {}", param),
        }
    }
}

#[ctor::ctor]
fn register() {
    register_pl_fac::<ObSlope>().unwrap()
}
