use polars::prelude::*;

use crate::factors::export::*;

/// Represents the mid-price factor in an order book.
///
/// The mid-price is calculated as the average of the best ask and best bid prices.
/// This factor is useful for providing a central reference point for the current market price.
#[derive(FactorBase, FromParam, Default, Clone, Copy)]
pub struct Mid;

impl PlFactor for Mid {
    fn try_expr(&self) -> Result<Expr> {
        let f = (ASK1 + BID1) * 0.5;
        f.try_expr()
    }
}

/// Represents the mid-price yield-to-maturity factor in an order book.
///
/// The mid-price YTM is calculated as the average of the best ask YTM and best bid YTM.
/// This factor is useful for providing a central reference point for the current market yield,
/// particularly in bond markets or other fixed-income securities.
///
/// # Fields
/// * `Param` - A parameter that can be used to customize the mid-price YTM calculation if needed.
#[derive(FactorBase, FromParam, Default, Clone, Copy)]
pub struct MidYtm;

impl PlFactor for MidYtm {
    fn try_expr(&self) -> Result<Expr> {
        let mid_ytm = (ASK1_YTM + BID1_YTM) * 0.5;
        mid_ytm.try_expr()
    }
}

/// Reference: https://github.com/sstoikov/microprice/blob/master/Microprice%20-%20Big%20Data%20Conference.ipynb
#[derive(FactorBase, FromParam, Default, Clone, Copy)]
pub struct WMid;

impl PlFactor for WMid {
    fn try_expr(&self) -> Result<Expr> {
        let imb = BID1_VOL / (BID1_VOL + ASK1_VOL);
        let wmid: Factor<_> = BID1 * (1 - imb) + ASK1 * imb;
        wmid.try_expr()
    }
}

#[ctor::ctor]
fn register() {
    register_pl_fac::<Mid>().unwrap();
    register_pl_fac::<MidYtm>().unwrap();
    register_pl_fac::<WMid>().unwrap();
}
