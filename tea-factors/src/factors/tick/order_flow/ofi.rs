use factor_macro::FactorBaseNoDebug;
use polars::prelude::*;

use super::{is_order_tier, is_simple_order_tier, OrderTier, SimpleOrderTier};
use crate::factors::export::*;

/// Represents the Order Flow Imbalance (OFI) factor.
///
/// OFI is a measure of buying and selling pressure in the market, calculated using trade data.
/// It compares the volume of buy trades to sell trades over a specified window.
///
/// # Calculation
/// OFI = Buy Volume / (|Buy Volume| + |Sell Volume|)
///
/// Where:
/// - Buy Volume: Sum of trade amounts for buy orders in the window
/// - Sell Volume: Sum of trade amounts for sell orders in the window
///
/// # Interpretation
/// - OFI > 0.5: Indicates net buying pressure
/// - OFI < 0.5: Indicates net selling pressure
/// - OFI = 0.5: Indicates balance between buying and selling pressure
/// - Magnitude of deviation from 0.5: Reflects the strength of the imbalance
///
/// # Usage
/// OFI can be used to:
/// - Identify potential short-term price movements
/// - Confirm trends or predict reversals
/// - Gauge market sentiment and liquidity
///
/// # Parameters
/// - Window size: Number of trades or time period for calculation (specified in `Param`)
#[derive(FactorBase, FromParam, Default, Clone, Copy)]
pub struct Ofi(pub usize);

impl PlFactor for Ofi {
    fn try_expr(&self) -> Result<Expr> {
        let n = self.0;
        let buy_vol = (ORDER_AMT * iif(IS_BUY, 1, 0)).sum_opt(n, 1);
        let sell_vol = (ORDER_AMT * iif(!IS_BUY, 1, 0)).sum_opt(n, 1);
        let ofi = buy_vol / (buy_vol + sell_vol);
        ofi.try_expr()
    }
}

/// Cumulative Order Flow Indicator (CumOFI)
///
/// CumOFI is a variant of the Order Flow Indicator (OFI) that uses cumulative sums instead of rolling windows.
///
/// # Calculation
/// CumOFI = Cumulative Buy Volume / (|Cumulative Buy Volume| + |Cumulative Sell Volume|)
///
/// Where:
/// - Cumulative Buy Volume: Running sum of trade amounts for buy orders from the beginning
/// - Cumulative Sell Volume: Running sum of trade amounts for sell orders from the beginning
///
/// # Interpretation
/// - Positive CumOFI: Indicates net buying pressure over the entire period
/// - Negative CumOFI: Indicates net selling pressure over the entire period
/// - Magnitude of CumOFI: Reflects the strength of the overall imbalance
///
/// # Usage
/// CumOFI can be used to:
/// - Identify long-term trends in buying or selling pressure
/// - Analyze cumulative market sentiment
/// - Compare current market state to historical imbalances
///
/// # Parameters
/// - Param: Used for potential future extensions or configurations
#[derive(FactorBase, FromParam, Default, Clone, Copy)]
pub struct CumOfi(pub usize);

impl PlFactor for CumOfi {
    fn try_expr(&self) -> Result<Expr> {
        let buy_vol = (ORDER_AMT * iif(IS_BUY, 1, 0)).cum_sum().ffill();
        let sell_vol = (ORDER_AMT * iif(!IS_BUY, 1, 0)).cum_sum().ffill();
        let ofi = buy_vol / (buy_vol + sell_vol);
        Ok(ofi
            .try_expr()?
            .ts_zscore(self.0, Some(4))
            .over([col(&*TradingDate::fac_name())]))
    }
}

/// Aggregated Order Flow Indicator (AggOFI)
///
/// AggOFI is a variant of the Order Flow Indicator (OFI) that calculates the imbalance
/// between buy and sell volumes over an aggregated time period.
///
/// # Calculation
/// AggOFI = Aggregated Buy Volume / (Aggregated Buy Volume + Aggregated Sell Volume)
///
/// Where:
/// - Aggregated Buy Volume: Sum of trade amounts for buy orders within the aggregation period
/// - Aggregated Sell Volume: Sum of trade amounts for sell orders within the aggregation period
///
/// # Interpretation
/// - AggOFI > 0.5: Indicates net buying pressure over the aggregated period
/// - AggOFI < 0.5: Indicates net selling pressure over the aggregated period
/// - AggOFI = 0.5: Indicates balance between buying and selling pressure
///
/// # Usage
/// AggOFI can be used to:
/// - Analyze buying/selling pressure over specific time intervals
/// - Identify potential trend reversals or continuations
/// - Compare order flow imbalances across different time frames
///
/// This struct implements the `PlAggFactor` trait, allowing it to be used
/// in aggregated factor calculations within the factor framework.
#[derive(Default, FactorBase, Clone, Copy)]
pub struct AggOfi;

impl PlAggFactor for AggOfi {
    #[inline]
    fn agg_expr(&self) -> Result<Expr> {
        let buy_vol = (ORDER_AMT * iif(IS_BUY, 1, 0)).try_expr()?.sum();
        let sell_vol = (ORDER_AMT * iif(!IS_BUY, 1, 0)).try_expr()?.sum();
        let ofi = buy_vol.clone().protect_div(buy_vol + sell_vol);
        Ok(ofi.fill_nan(NONE))
    }
}

#[derive(FactorBaseNoDebug, Clone, Copy)]
pub struct TierOfi(pub OrderTier, pub usize);

impl std::fmt::Debug for TierOfi {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "TierOfi({:?}, {})", self.0, self.1)
    }
}

impl PlFactor for TierOfi {
    fn try_expr(&self) -> Result<Expr> {
        let n = self.1;
        let buy_vol =
            (ORDER_AMT * iif(IS_BUY & ExprFactor(is_order_tier(self.0)), 1, 0)).sum_opt(n, 1);
        let sell_vol =
            (ORDER_AMT * iif(!IS_BUY & ExprFactor(is_order_tier(self.0)), 1, 0)).sum_opt(n, 1);
        let ofi = buy_vol.clone() / (buy_vol + sell_vol);
        ofi.try_expr()
    }
}

#[derive(FactorBaseNoDebug, Clone, Copy)]
pub struct SimpleTierOfi(pub SimpleOrderTier, pub usize);

impl std::fmt::Debug for SimpleTierOfi {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "SimpleTierOfi({:?}, {})", self.0, self.1)
    }
}

impl PlFactor for SimpleTierOfi {
    fn try_expr(&self) -> Result<Expr> {
        let n = self.1;
        let buy_vol =
            (ORDER_AMT * iif(IS_BUY & is_simple_order_tier(self.0).fac(), 1, 0)).sum_opt(n, 1);
        let sell_vol =
            (ORDER_AMT * iif(!IS_BUY & is_simple_order_tier(self.0).fac(), 1, 0)).sum_opt(n, 1);
        let ofi = buy_vol.clone() / (buy_vol + sell_vol);
        ofi.try_expr()
    }
}

#[ctor::ctor]
fn register() {
    register_pl_fac::<Ofi>().unwrap();
    register_pl_fac::<CumOfi>().unwrap();
}
