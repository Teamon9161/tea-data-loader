use polars::prelude::*;

use crate::factors::export::*;

/// Represents the Order Book Imbalance (OBI) factor.
///
/// OBI is calculated as (BidVolume - AskVolume) / (BidVolume + AskVolume).
/// It measures the relative difference between buy and sell volumes, indicating potential price pressure.
///
/// # Fields
/// * `Param` - Determines the number of price levels to include:
///   - None or 1: Uses only the top bid and ask volumes
///   - 2 to 5: Includes volumes from the specified number of price levels on each side
///
/// # Examples
/// - Param(1) or None: (BidVol1 - AskVol1) / (BidVol1 + AskVol1)
/// - Param(3): (Sum of top 3 BidVols - Sum of top 3 AskVols) / (Sum of top 3 BidVols + Sum of top 3 AskVols)
#[derive(FactorBase, Default, Clone)]
pub struct Obi(pub Param);

impl PlFactor for Obi {
    fn try_expr(&self) -> Result<Expr> {
        let level = if self.0.is_none() {
            1
        } else {
            self.0.as_usize()
        };
        BidCumVol::new(level).imb(AskCumVol::new(level)).try_expr()
    }
}

/// Represents the Cumulative Order Book Imbalance (CumOBI) factor.
///
/// CumOBI is similar to OBI but uses cumulative volumes instead of instantaneous volumes.
/// It's calculated as (CumBidVolume - CumAskVolume) / (CumBidVolume + CumAskVolume).
///
/// # Fields
/// * `Param` - Determines the number of price levels to include in the cumulative volumes:
///   - None or 1: Uses only the cumulative top bid and ask volumes
///   - 2 to 5: Includes cumulative volumes from the specified number of price levels on each side
///
/// # Examples
/// - Param(1) or None: (CumBidVol1 - CumAskVol1) / (CumBidVol1 + CumAskVol1)
/// - Param(3): (Sum of top 3 CumBidVols - Sum of top 3 CumAskVols) / (Sum of top 3 CumBidVols + Sum of top 3 CumAskVols)
///
/// CumOBI provides a longer-term view of order book imbalance compared to the standard OBI.
#[derive(FactorBase, Default, Clone)]
pub struct CumObi(pub Param);

impl PlFactor for CumObi {
    fn try_expr(&self) -> Result<Expr> {
        use super::{CumAskCumVol, CumBidCumVol};
        let level = if self.0.is_none() {
            1
        } else {
            self.0.as_usize()
        };
        let bid_cum_vol = CumBidCumVol::new(level).try_expr()?;
        let ask_cum_vol = CumAskCumVol::new(level).try_expr()?;
        Ok(bid_cum_vol.imbalance(ask_cum_vol))
    }
}

#[ctor::ctor]
fn register() {
    register_pl_fac::<Obi>().unwrap();
    register_pl_fac::<CumObi>().unwrap();
}
