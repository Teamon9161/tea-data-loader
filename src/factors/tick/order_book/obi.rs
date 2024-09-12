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
#[derive(FactorBase, Default, Debug, Clone)]
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

#[ctor::ctor]
fn register() {
    register_pl_fac::<Obi>().unwrap()
}
