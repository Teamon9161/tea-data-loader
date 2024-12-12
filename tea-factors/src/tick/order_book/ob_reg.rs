use anyhow::ensure;
use polars::prelude::*;

use crate::export::*;
/// This module implements order book regression tools and factors.

/// Trait representing a factor that can be used in regression calculations.
trait FactorT: FactorBase + PlFactor {}

impl<T: FactorBase + PlFactor> FactorT for T {}

/// A structure representing regression tools for order book analysis.
#[derive(Clone, Copy)]
struct RegTool<F1: FactorT, F2: FactorT, F3: FactorT, F4: FactorT, F5: FactorT> {
    /// Number of data points used in the regression.
    pub n: usize,
    /// Sum of x values.
    pub sum_x: Factor<F1>,
    /// Sum of y values.
    pub sum_y: Factor<F2>,
    /// Sum of the product of x and y values.
    pub sum_xy: Factor<F3>,
    /// Sum of squared x values.
    pub sum_x2: Factor<F4>,
    /// Sum of squared y values.
    pub sum_y2: Factor<F5>,
}

impl<F1: FactorT, F2: FactorT, F3: FactorT, F4: FactorT, F5: FactorT> RegTool<F1, F2, F3, F4, F5> {
    /// Calculates the beta (slope) of the regression line.
    ///
    /// The beta is calculated using the formula:
    /// β = (n Σxy - Σx Σy) / (n Σx² - (Σx)²)
    ///
    /// Where:
    /// n: number of data points
    /// Σxy: sum of the product of x and y
    /// Σx: sum of x values
    /// Σy: sum of y values
    /// Σx²: sum of squared x values
    ///
    /// # Returns
    /// An implementation of `FactorT` representing the calculated beta value.
    fn beta(self) -> Factor<impl FactorT> {
        (self.sum_xy * self.n - self.sum_x.clone() * self.sum_y)
            / (self.sum_x2 * self.n - self.sum_x.clone() * self.sum_x)
    }

    /// Calculates the alpha (intercept) of the regression line.
    ///
    /// The alpha is calculated using the formula:
    /// α = (Σy - β Σx) / n
    ///
    /// Where:
    /// n: number of data points
    /// Σy: sum of y values
    /// β: beta value
    /// Σx: sum of x values
    ///
    /// # Returns
    /// An implementation of `FactorT` representing the calculated alpha value.
    fn alpha(self) -> Factor<impl FactorT> {
        let beta = self.clone().beta();
        (self.sum_y - beta * self.sum_x) / self.n
    }

    /// Calculates the sum of squared errors (SSE) of the regression line.
    ///
    /// The SSE is calculated using the formula:
    /// SSE = Σy² - α Σy - β Σxy
    ///
    /// Where:
    /// Σy²: sum of squared y values
    /// α: alpha value
    /// Σy: sum of y values
    /// β: beta value
    /// Σxy: sum of the product of x and y values
    ///
    /// # Returns
    /// An implementation of `FactorT` representing the calculated SSE value.
    fn sse(self) -> Factor<impl FactorT> {
        let alpha = self.clone().alpha();
        let beta = self.clone().beta();
        self.sum_y2 - alpha * self.sum_y - beta * self.sum_xy
    }

    /// Calculates the R-squared (coefficient of determination) of the regression line.
    ///
    /// R-squared is calculated using the formula:
    /// R² = 1 - (SSE / SST)
    ///
    /// Where:
    /// SSE: Sum of Squared Errors
    /// SST: Total Sum of Squares (Σy² - (Σy)² / n)
    ///
    /// # Returns
    /// An implementation of `FactorT` representing the calculated R-squared value.
    fn r_squared(self) -> Factor<impl FactorT> {
        let sse = self.clone().sse();
        let sst = self.sum_y2 - (self.sum_y.clone() * self.sum_y) / self.n;
        1 - (sse / sst)
    }
}

/// Creates a RegTool instance for bid-side order book analysis.
///
/// # Arguments
/// * `n` - The number of price levels to consider (must be 5).
///
/// # Returns
/// A Result containing the RegTool instance for bid-side analysis.
fn get_reg_tool_bid(
    n: usize,
) -> Result<RegTool<impl FactorT, impl FactorT, impl FactorT, impl FactorT, impl FactorT>> {
    ensure!(n == 5, "n must be equal to 5");
    // paste::paste!()
    let sum_x = crate::hsum!(
        BidCumVol(1),
        BidCumVol(2),
        BidCumVol(3),
        BidCumVol(4),
        BidCumVol(5)
    );
    let sum_y = crate::hsum!(BID1, BID2, BID3, BID4, BID5);
    let sum_xy = crate::hsum!(
        BID1 * BidCumVol(1),
        BID2 * BidCumVol(2),
        BID3 * BidCumVol(3),
        BID4 * BidCumVol(4),
        BID5 * BidCumVol(5)
    );
    let sum_x2 = crate::hsum!(
        BidCumVol::fac(1) * BidCumVol(1),
        BidCumVol::fac(2) * BidCumVol(2),
        BidCumVol::fac(3) * BidCumVol(3),
        BidCumVol::fac(4) * BidCumVol(4),
        BidCumVol::fac(5) * BidCumVol(5)
    );
    let sum_y2 = crate::hsum!(
        BID1 * BID1,
        BID2 * BID2,
        BID3 * BID3,
        BID4 * BID4,
        BID5 * BID5
    );
    Ok(RegTool {
        n,
        sum_x,
        sum_y,
        sum_xy,
        sum_x2,
        sum_y2,
    })
}

/// Creates a RegTool instance for ask-side order book analysis.
///
/// # Arguments
/// * `n` - The number of price levels to consider (must be 5).
///
/// # Returns
/// A Result containing the RegTool instance for ask-side analysis.
fn get_reg_tool_ask(
    n: usize,
) -> Result<RegTool<impl FactorT, impl FactorT, impl FactorT, impl FactorT, impl FactorT>> {
    ensure!(n == 5, "n must be equal to 5");
    let sum_x = AskCumVol::fac(1)
        + AskCumVol::fac(2)
        + AskCumVol::fac(3)
        + AskCumVol::fac(4)
        + AskCumVol::fac(5);
    let sum_y = ASK1 + ASK2 + ASK3 + ASK4 + ASK5;
    let sum_xy = (ASK1 * AskCumVol(1))
        + (ASK2 * AskCumVol(2))
        + (ASK3 * AskCumVol(3))
        + (ASK4 * AskCumVol(4))
        + (ASK5 * AskCumVol(5));
    let sum_x2 = (AskCumVol::fac(1) * AskCumVol::fac(1))
        + (AskCumVol::fac(2) * AskCumVol::fac(2))
        + (AskCumVol::fac(3) * AskCumVol::fac(3))
        + (AskCumVol::fac(4) * AskCumVol::fac(4))
        + (AskCumVol::fac(5) * AskCumVol::fac(5));
    let sum_y2 = (ASK1 * ASK1) + (ASK2 * ASK2) + (ASK3 * ASK3) + (ASK4 * ASK4) + (ASK5 * ASK5);
    Ok(RegTool {
        n,
        sum_x,
        sum_y,
        sum_xy,
        sum_x2,
        sum_y2,
    })
}

/// A factor representing the slope of the order book regression.
#[derive(FactorBase, FromParam, Clone, Copy)]
pub struct ObRegSlope;

impl PlFactor for ObRegSlope {
    fn try_expr(&self) -> Result<Expr> {
        let beta_bid = get_reg_tool_bid(5)?.beta();
        let beta_ask = get_reg_tool_ask(5)?.beta();
        let slope = beta_bid + beta_ask;
        (slope * 1e9).try_expr()
    }
}

/// A factor representing the alpha (intercept) of the order book regression.
#[derive(FactorBase, FromParam, Clone, Copy)]
pub struct ObRegAlpha;

impl PlFactor for ObRegAlpha {
    fn try_expr(&self) -> Result<Expr> {
        let alpha_bid = get_reg_tool_bid(5)?.alpha();
        let alpha_ask = get_reg_tool_ask(5)?.alpha();
        let alpha = alpha_bid - alpha_ask;
        alpha.try_expr()
    }
}

/// A factor representing the sum of squared errors (SSE) of the order book regression.
#[derive(FactorBase, FromParam, Clone, Copy)]
pub struct ObRegSse;

impl PlFactor for ObRegSse {
    fn try_expr(&self) -> Result<Expr> {
        let sse_bid = get_reg_tool_bid(5)?.sse();
        let sse_ask = get_reg_tool_ask(5)?.sse();
        let sse = sse_bid - sse_ask;
        sse.try_expr()
    }
}

/// A factor representing the R-squared of the order book regression.
///
/// This factor calculates the difference between the R-squared values of the bid and ask sides
/// of the order book. The R-squared value measures the goodness of fit of the regression model
/// for each side of the order book.
///
/// A positive value indicates that the bid side has a better fit, while a negative value
/// indicates that the ask side has a better fit. Values closer to zero suggest similar
/// fit quality on both sides.
#[derive(FactorBase, FromParam, Clone, Copy)]
pub struct ObRegRSquared;

impl PlFactor for ObRegRSquared {
    fn try_expr(&self) -> Result<Expr> {
        let r_squared_bid = get_reg_tool_bid(5)?.r_squared();
        let r_squared_ask = get_reg_tool_ask(5)?.r_squared();
        let r_squared = r_squared_bid - r_squared_ask;
        r_squared.try_expr()
    }
}

#[ctor::ctor]
fn register() {
    register_pl_fac::<ObRegSlope>().unwrap();
    register_pl_fac::<ObRegAlpha>().unwrap();
    register_pl_fac::<ObRegSse>().unwrap();
    register_pl_fac::<ObRegRSquared>().unwrap();
}
