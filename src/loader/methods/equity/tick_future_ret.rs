use polars::prelude::*;
use rayon::prelude::*;
use tea_strategy::equity::{calc_tick_future_ret, CommissionType, SignalType, TickFutureRetKwargs};

use crate::prelude::*;

pub struct TickFutureRetOpt<'a> {
    pub c_rate: f64,
    pub is_signal: bool,
    pub init_cash: usize,
    pub bid: &'a str,
    pub ask: &'a str,
    pub contract_chg_signal: Option<&'a str>,
    pub multiplier: Option<f64>,
    pub commission_type: CommissionType,
    pub signal_type: SignalType,
    pub blowup: bool,
    pub suffix: &'a str,
}

impl Default for TickFutureRetOpt<'_> {
    #[inline]
    fn default() -> Self {
        TickFutureRetOpt {
            c_rate: 0.0003,
            is_signal: true,
            init_cash: 10_000_000,
            bid: "b1",
            ask: "a1",
            contract_chg_signal: None,
            multiplier: None,
            commission_type: CommissionType::Percent,
            signal_type: SignalType::Absolute,
            blowup: false,
            suffix: "",
        }
    }
}

impl TickFutureRetOpt<'_> {
    #[inline]
    fn to_tick_future_ret_kwargs(&self, multiplier: Option<f64>) -> TickFutureRetKwargs {
        let multiplier = if let Some(opt_multiplier) = self.multiplier {
            opt_multiplier
        } else {
            multiplier.unwrap_or(1.)
        };
        TickFutureRetKwargs {
            init_cash: self.init_cash,
            multiplier,
            commission_type: self.commission_type,
            signal_type: self.signal_type,
            blowup: self.blowup,
            c_rate: self.c_rate,
        }
    }
}

impl DataLoader {
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
                        let mut signal = df.column(f).unwrap().clone();
                        if !opt.is_signal {
                            // recover signal from position vector
                            signal = signal.shift(-1);
                        }
                        let signal = signal.cast_f64().unwrap();
                        let bid_vec = df.column(opt.bid).unwrap().cast_f64().unwrap();
                        let ask_vec = df.column(opt.ask).unwrap().cast_f64().unwrap();
                        let multiplier = multiplier_map.get(symbol).cloned();
                        let out: Float64Chunked = if let Some(contract_chg_signal) =
                            &opt.contract_chg_signal
                        {
                            let contract_chg_signal_vec = df.column(contract_chg_signal).unwrap();
                            let contract_chg_signal_vec =
                                contract_chg_signal_vec.cast_bool().unwrap();
                            calc_tick_future_ret(
                                signal.f64().unwrap(),
                                bid_vec.f64().unwrap(),
                                ask_vec.f64().unwrap(),
                                Some(contract_chg_signal_vec.bool().unwrap()),
                                &opt.to_tick_future_ret_kwargs(multiplier_map.get(symbol).cloned()),
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
                        out.with_name(&(f.to_string() + opt.suffix)).into_series()
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
