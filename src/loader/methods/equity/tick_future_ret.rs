use polars::prelude::*;
use rayon::prelude::*;
use tea_strategy::equity::{calc_tick_future_ret, SignalType, TickFutureRetKwargs};

use crate::prelude::*;
/// Options for calculating tick-based future returns based on strategy signals and futures market data.
///
/// This struct contains various parameters used in the calculation of strategy returns
/// for futures trading on a tick-by-tick basis, combining strategy signals with futures price data.
pub struct TickFutureRetOpt<'a> {
    /// Commission rate for trades.
    pub c_rate: CRate,
    /// Flag indicating whether the input is a next-period signal (true) or current-period position (false).
    pub is_signal: bool,
    /// Initial cash amount for the trading strategy.
    pub init_cash: usize,
    /// Column name for the bid price in the futures data.
    pub bid: &'a str,
    /// Column name for the ask price in the futures data.
    pub ask: &'a str,
    /// Optional column name for the contract change signal in the strategy data.
    pub contract_chg_signal: Option<&'a str>,
    /// Optional multiplier for contract size.
    pub multiplier: Option<f64>,
    /// Type of signal used in the strategy (e.g., Absolute, Percent).
    pub signal_type: SignalType,
    /// Flag indicating whether to allow the strategy to blow up (i.e., go bankrupt).
    pub blowup: bool,
    /// Suffix for output column names in the resulting DataFrame.
    pub suffix: &'a str,
}

impl Default for TickFutureRetOpt<'_> {
    #[inline]
    fn default() -> Self {
        TickFutureRetOpt {
            c_rate: Default::default(),
            is_signal: true,
            init_cash: 10_000_000,
            bid: "b1",
            ask: "a1",
            contract_chg_signal: None,
            multiplier: None,
            // commission_type: CommissionType::Percent,
            signal_type: SignalType::Absolute,
            blowup: false,
            suffix: "",
        }
    }
}

impl TickFutureRetOpt<'_> {
    /// Converts the `TickFutureRetOpt` instance to `TickFutureRetKwargs` for tea-strategy.
    ///
    /// This method creates a `TickFutureRetKwargs` struct based on the current `TickFutureRetOpt` settings,
    /// which is used to configure parameters for the tea-strategy library's tick-based future return calculations.
    ///
    /// # Arguments
    ///
    /// * `multiplier` - An optional f64 value to use as the multiplier if not set in the instance.
    ///
    /// # Returns
    ///
    /// A `TickFutureRetKwargs` instance with the configured settings for tea-strategy.
    #[inline]
    fn to_tick_future_ret_kwargs(&self, multiplier: Option<f64>) -> TickFutureRetKwargs {
        // 优先使用自身指定的multiplier，然后才是传入的multiplier
        let multiplier = if let Some(opt_multiplier) = self.multiplier {
            opt_multiplier
        } else {
            multiplier.unwrap_or(1.)
        };
        TickFutureRetKwargs {
            init_cash: self.init_cash,
            multiplier,
            commission_type: self.c_rate.get_type(),
            signal_type: self.signal_type,
            blowup: self.blowup,
            c_rate: self.c_rate.get_value(),
        }
    }
}

impl DataLoader {
    /// Calculates tick-based future returns for the given factors using the specified options.
    ///
    /// This method computes tick-based future returns for each factor provided in the `facs` array,
    /// applying the settings specified in the `TickFutureRetOpt` struct.
    ///
    /// # Arguments
    ///
    /// * `facs` - A slice of factors (as strings) for which to calculate future returns.
    /// * `opt` - A reference to `TickFutureRetOpt` containing calculation options and parameters.
    ///
    /// # Returns
    ///
    /// A `Result` containing a new `DataLoader` with the calculated tick-based future returns,
    /// or an error if the operation fails.
    ///
    /// # Details
    ///
    /// - If no multiplier is set, it attempts to set one using `with_multiplier()`.
    /// - Calculations are performed in parallel for each symbol and dataframe.
    /// - Handles both signal-based and position-based calculations.
    /// - Supports contract change signals when specified in `opt.contract_chg_signal`.
    /// - The resulting columns are named by appending `opt.suffix` to the factor names.
    ///
    /// # Errors
    ///
    /// This function will return an error if:
    /// - Setting the multiplier fails
    /// - Any of the required columns are missing from the dataframes
    /// - The calculations encounter any issues (e.g., type mismatches, invalid data)
    pub fn calc_tick_future_ret<F: AsRef<str>>(
        self,
        facs: &[F],
        opt: &TickFutureRetOpt,
    ) -> Result<Self> {
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
                        let mut signal = df.column(f).unwrap().as_materialized_series().clone();
                        if !opt.is_signal {
                            // recover signal from position vector
                            signal = signal.shift(-1);
                        }
                        let signal = signal.cast_f64().unwrap();
                        let bid_vec = df
                            .column(opt.bid)
                            .unwrap()
                            .as_materialized_series()
                            .cast_f64()
                            .unwrap();
                        let ask_vec = df
                            .column(opt.ask)
                            .unwrap()
                            .as_materialized_series()
                            .cast_f64()
                            .unwrap();
                        let multiplier = multiplier_map.get(symbol).cloned();
                        let out: Float64Chunked = if let Some(contract_chg_signal) =
                            &opt.contract_chg_signal
                        {
                            let contract_chg_signal_vec = df.column(contract_chg_signal).unwrap();
                            let contract_chg_signal_vec = contract_chg_signal_vec
                                .as_materialized_series()
                                .cast_bool()
                                .unwrap();
                            calc_tick_future_ret(
                                signal.f64().unwrap(),
                                bid_vec.f64().unwrap(),
                                ask_vec.f64().unwrap(),
                                Some(contract_chg_signal_vec.bool().unwrap()),
                                &opt.to_tick_future_ret_kwargs(multiplier),
                            )
                        } else {
                            calc_tick_future_ret::<_, _, _, BooleanChunked>(
                                signal.f64().unwrap(),
                                bid_vec.f64().unwrap(),
                                ask_vec.f64().unwrap(),
                                None,
                                &opt.to_tick_future_ret_kwargs(multiplier),
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
