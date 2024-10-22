use polars::prelude::*;

use crate::factors::export::*;

#[derive(FactorBase, Default, Clone)]
pub struct OrderAmtQuantile(pub f64, pub &'static str);

impl PlFactor for OrderAmtQuantile {
    #[inline]
    fn try_expr(&self) -> Result<Expr> {
        let expr = ORDER_AMT.expr().rolling_quantile_by(
            TIME.expr(),
            QuantileInterpolOptions::Linear,
            self.0,
            RollingOptionsDynamicWindow {
                window_size: Duration::parse(self.1),
                min_periods: 1,
                closed_window: ClosedWindow::Right,
                fn_params: None,
            },
        );
        Ok(expr)
    }
}

#[derive(FactorBase, Default, Clone)]
pub struct OrderVolQuantile(pub f64, pub &'static str);

impl PlFactor for OrderVolQuantile {
    #[inline]
    fn try_expr(&self) -> Result<Expr> {
        let expr = ORDER_VOL.expr().rolling_quantile_by(
            TIME.expr(),
            QuantileInterpolOptions::Linear,
            self.0,
            RollingOptionsDynamicWindow {
                window_size: Duration::parse(self.1),
                min_periods: 1,
                closed_window: ClosedWindow::Right,
                fn_params: None,
            },
        );
        Ok(expr)
    }
}

// #[ctor::ctor]
// fn register() {
//     register_pl_fac::<OrderAmtQuantile>().unwrap()
// }
