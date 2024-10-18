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
#[derive(FactorBase, FromParam, Default, Clone, Copy)]
pub struct Obi(pub Option<usize>);

impl PlFactor for Obi {
    fn try_expr(&self) -> Result<Expr> {
        let level = self.0.unwrap_or(1);
        BidCumVol::new(level).imb(AskCumVol::new(level)).try_expr()
    }
}

/// Represents the Cumulative Order Book Imbalance (CumOBI) factor.
///
/// CumOBI is similar to OBI but uses cumulative volumes instead of instantaneous volumes.
/// It's calculated as (CumBidVolume - CumAskVolume) / (CumBidVolume + CumAskVolume).
///
/// # Fields
/// * `usize` - Determines the window size for the time series z-score calculation.
///
/// # Implementation Details
/// - Uses cumulative volumes from the top price level (CumBidVol1 and CumAskVol1).
/// - Calculates the imbalance between cumulative bid and ask volumes.
/// - Applies a time series z-score transformation with the specified window size.
/// - Groups the calculation by trading date.
///
/// CumOBI provides a longer-term view of order book imbalance compared to the standard OBI,
/// with the added normalization through z-score calculation.
#[derive(FactorBase, FromParam, Default, Clone, Copy)]
pub struct CumObi(pub usize);

impl PlFactor for CumObi {
    fn try_expr(&self) -> Result<Expr> {
        use super::{CumAskCumVol, CumBidCumVol};
        let bid_cum_vol = CumBidCumVol(1).try_expr()?;
        let ask_cum_vol = CumAskCumVol(1).try_expr()?;
        Ok(bid_cum_vol
            .imbalance(ask_cum_vol)
            .ts_zscore(self.0, None)
            .over([col(&*TradingDate::fac_name())]))
    }
}

#[ctor::ctor]
fn register() {
    register_pl_fac::<Obi>().unwrap();
    register_pl_fac::<CumObi>().unwrap();
}
