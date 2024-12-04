use polars::prelude::*;
use rayon::prelude::*;
use tea_strategy::equity::{FutureRetKwargs, FutureRetSpreadKwargs};

use crate::prelude::*;

macro_rules! auto_cast {
    // for multiple expressions
    ($arm: ident ($($se: expr),*)) => {
        ($(
            if let DataType::$arm = $se.dtype() {
                $se.clone()
            } else {
                $se.cast(&DataType::$arm).unwrap()
            }
        ),*)
    };
}
/// Options for calculating future returns based on strategy signals and futures market data.
///
/// This struct contains various parameters used in the calculation of strategy returns
/// for futures trading, combining strategy signals with futures price data.
pub struct FutureRetOpt<'a> {
    /// Commission rate for trades.
    pub c_rate: CRate,
    /// Flag indicating whether the input is a next-period signal (true) or current-period position (false).
    pub is_signal: bool,
    /// Initial cash amount for the trading strategy.
    pub init_cash: usize,
    /// Column name for the opening price in the futures data.
    pub opening_cost: &'a str,
    /// Column name for the closing price in the futures data.
    pub closing_cost: &'a str,
    /// Column name for the contract change signal in the strategy data.
    pub contract_chg_signal: &'a str,
    /// Optional multiplier for contract size.
    pub multiplier: Option<f64>,
    /// Flag indicating whether to apply slippage in the return calculation.
    pub slippage_flag: bool,
    /// Suffix for output column names in the resulting DataFrame.
    pub suffix: &'a str,
}

impl Default for FutureRetOpt<'_> {
    #[inline]
    fn default() -> Self {
        FutureRetOpt {
            c_rate: Default::default(),
            is_signal: true,
            init_cash: 10_000_000,
            opening_cost: "open_noadj",
            closing_cost: "close_noadj",
            contract_chg_signal: "contract_chg_signal",
            multiplier: None,
            // commission_type: CommissionType::Percent,
            slippage_flag: true,
            suffix: "",
        }
    }
}

impl FutureRetOpt<'_> {
    /// Converts the `FutureRetOpt` instance to `FutureRetKwargs` for tea-strategy.
    ///
    /// This method creates a `FutureRetKwargs` struct based on the current `FutureRetOpt` settings,
    /// which is used to configure parameters for the tea-strategy library's future return calculations.
    ///
    /// # Arguments
    ///
    /// * `multiplier` - An optional f64 value to use as the multiplier if not set in the instance.
    ///
    /// # Returns
    ///
    /// A `FutureRetKwargs` instance with the configured settings for tea-strategy.
    #[inline]
    fn to_future_ret_kwargs(&self, multiplier: Option<f64>) -> FutureRetKwargs {
        let multiplier = if let Some(opt_multiplier) = self.multiplier {
            opt_multiplier
        } else {
            multiplier.unwrap_or(1.)
        };
        FutureRetKwargs {
            init_cash: self.init_cash,
            leverage: 1.,
            multiplier,
            commission_type: self.c_rate.get_type(),
            blowup: false,
            c_rate: self.c_rate.get_value(),
            slippage: 0.,
        }
    }

    /// Converts the `FutureRetOpt` instance to `FutureRetSpreadKwargs` for tea-strategy.
    ///
    /// This method creates a `FutureRetSpreadKwargs` struct based on the current `FutureRetOpt` settings,
    /// which is used to configure parameters for the tea-strategy library's future return calculations
    /// that include spread considerations.
    ///
    /// # Arguments
    ///
    /// * `multiplier` - An optional f64 value to use as the multiplier if not set in the instance.
    ///
    /// # Returns
    ///
    /// A `FutureRetSpreadKwargs` instance with the configured settings for tea-strategy.
    #[inline]
    fn to_future_ret_spread_kwargs(&self, multiplier: Option<f64>) -> FutureRetSpreadKwargs {
        let multiplier = if let Some(opt_multiplier) = self.multiplier {
            opt_multiplier
        } else {
            multiplier.unwrap_or(1.)
        };
        FutureRetSpreadKwargs {
            init_cash: self.init_cash,
            leverage: 1.,
            multiplier,
            commission_type: self.c_rate.get_type(),
            blowup: false,
            c_rate: self.c_rate.get_value(),
        }
    }
}

impl DataLoader {
    /// Calculates future returns for the given factors using the specified options.
    ///
    /// This method computes future returns for each factor provided in the `facs` array,
    /// applying the settings specified in the `FutureRetOpt` struct. It handles both
    /// regular future returns and those with spread considerations.
    ///
    /// # Arguments
    ///
    /// * `facs` - A slice of factors (as strings) for which to calculate future returns.
    /// * `opt` - A reference to `FutureRetOpt` containing calculation options and parameters.
    ///
    /// # Returns
    ///
    /// A `Result` containing a new `DataLoader` with the calculated future returns,
    /// or an error if the operation fails.
    ///
    /// # Details
    ///
    /// - If no multiplier is set, it attempts to set one using `with_multiplier()`.
    /// - Calculations are performed in parallel for each symbol and dataframe.
    /// - Handles both signal-based and non-signal-based calculations.
    /// - Supports slippage considerations when `opt.slippage_flag` is true.
    /// - The resulting columns are named by appending `opt.suffix` to the factor names.
    ///
    /// # Errors
    ///
    /// This function will return an error if:
    /// - Setting the multiplier fails
    /// - Any of the required columns are missing from the dataframes
    /// - The calculations encounter any issues (e.g., type mismatches, invalid data)
    pub fn calc_future_ret<F: AsRef<str>>(self, facs: &[F], opt: &FutureRetOpt) -> Result<Self> {
        let facs = facs.iter().map(|f| f.as_ref()).collect::<Vec<_>>();
        let mut out = self.empty_copy();
        if self.multiplier.is_none() {
            out = out.with_multiplier()?;
        }
        let multiplier_map = out.multiplier.as_ref().unwrap();
        let dfs = self
            .par_apply_with_symbol(|(symbol, df)| {
                let df = df.collect().unwrap();
                let ecs: Vec<Series> = facs
                    .par_iter()
                    .map(|f| {
                        let mut pos = df.column(f).unwrap().clone();
                        if opt.is_signal {
                            pos = pos.shift(1)
                        }
                        let open_vec = df.column(opt.opening_cost).unwrap();
                        let close_vec = df.column(opt.closing_cost).unwrap();
                        let contract_chg_signal_vec = df
                            .column(opt.contract_chg_signal)
                            .unwrap()
                            .as_materialized_series();
                        let contract_chg_signal_vec = contract_chg_signal_vec.cast_bool().unwrap();
                        let (pos, open_vec, close_vec) =
                            auto_cast!(Float64(pos, open_vec, close_vec));
                        let multiplier = multiplier_map.get(symbol).cloned();
                        let out: Float64Chunked = if opt.slippage_flag {
                            let slippage = (df.column("twap_spread").unwrap() * 0.5)
                                .take_materialized_series();
                            let slippage_vec = slippage.cast_f64().unwrap();
                            tea_strategy::equity::calc_future_ret_with_spread(
                                pos.f64().unwrap(),
                                open_vec.f64().unwrap(),
                                close_vec.f64().unwrap(),
                                slippage_vec.f64().unwrap(),
                                Some(contract_chg_signal_vec.bool().unwrap()),
                                // TODO(teamon): should be a correct multiplier
                                &opt.to_future_ret_spread_kwargs(multiplier),
                            )
                        } else {
                            tea_strategy::equity::calc_future_ret(
                                pos.f64().unwrap(),
                                open_vec.f64().unwrap(),
                                close_vec.f64().unwrap(),
                                Some(contract_chg_signal_vec.bool().unwrap()),
                                // TODO(teamon): should be a correct multiplier
                                &opt.to_future_ret_kwargs(multiplier),
                            )
                        };
                        out.with_name((f.to_string() + opt.suffix).into())
                            .into_series()
                    })
                    .collect();
                let ecs: Vec<_> = ecs.into_iter().map(lit).collect();
                Frame::Eager(df).with_columns(&ecs).unwrap()
            })
            .dfs;
        out.dfs = dfs;
        Ok(out)
    }
}
