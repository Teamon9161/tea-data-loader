use polars::prelude::*;

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
#[derive(FactorBase, FromParam, Default, Clone)]
pub struct Ofi(pub usize);

impl PlFactor for Ofi {
    fn try_expr(&self) -> Result<Expr> {
        let n = self.0;
        let buy_vol = (ORDER_AMT * iif(IS_BUY, 1, 0)).sum_opt(n, 1);
        let sell_vol = (ORDER_AMT * iif(!IS_BUY, 1, 0)).sum_opt(n, 1);
        let ofi = buy_vol.clone() / (buy_vol + sell_vol);
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
#[derive(FactorBase, FromParam, Default, Clone)]
pub struct CumOfi(pub Param);

impl PlFactor for CumOfi {
    fn try_expr(&self) -> Result<Expr> {
        let is_buy = IS_BUY.expr();
        let buy_vol = (ORDER_AMT.expr() * (when(is_buy.clone()).then(1.lit()).otherwise(0.lit())))
            .cum_sum(false)
            .forward_fill(None);
        let sell_vol = (ORDER_AMT.expr() * when(is_buy.not()).then(1.lit()).otherwise(0.lit()))
            .cum_sum(false)
            .forward_fill(None);
        let ofi = buy_vol.clone().protect_div(buy_vol.abs() + sell_vol.abs());
        Ok(ofi
            .ts_zscore(self.0.as_usize(), Some(4))
            .over([col(&TradingDate::fac_name())]))
    }
}

#[ctor::ctor]
fn register() {
    register_pl_fac::<Ofi>().unwrap();
    register_pl_fac::<CumOfi>().unwrap();
}
