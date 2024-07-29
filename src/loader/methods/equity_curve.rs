use polars::prelude::*;
use rayon::prelude::*;
use tea_strategy::equity::{CommissionType, FutureRetKwargs, FutureRetSpreadKwargs};

use crate::prelude::*;

macro_rules! auto_cast {
    // for one expression
    ($arm: ident ($se: expr)) => {
        if let DataType::$arm = $se.dtype() {
            $se.clone()
        } else {
            $se.cast(&DataType::$arm).unwrap()
        }
    };
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

pub struct FutureRetOpt<'a> {
    c_rate: f64,
    is_signal: bool,
    init_cash: usize,
    opening_cost: &'a str,
    closing_cost: &'a str,
    contract_chg_signal: &'a str,
    commission_type: CommissionType,
    slippage_flag: bool,
    suffix: &'a str,
}

impl Default for FutureRetOpt<'_> {
    #[inline]
    fn default() -> Self {
        FutureRetOpt {
            c_rate: 0.0003,
            is_signal: true,
            init_cash: 10_000_000,
            opening_cost: "open_noadj",
            closing_cost: "close_noadj",
            contract_chg_signal: "contract_chg_signal",
            commission_type: CommissionType::Percent,
            slippage_flag: true,
            suffix: "",
        }
    }
}

impl FutureRetOpt<'_> {
    #[inline]
    fn to_future_ret_kwargs(&self, multiplier: f64) -> FutureRetKwargs {
        FutureRetKwargs {
            init_cash: self.init_cash,
            leverage: 1.,
            multiplier,
            commission_type: self.commission_type,
            blowup: false,
            c_rate: self.c_rate,
            slippage: 0.,
        }
    }

    #[inline]
    fn to_future_ret_spread_kwargs(&self, multiplier: f64) -> FutureRetSpreadKwargs {
        FutureRetSpreadKwargs {
            init_cash: self.init_cash,
            leverage: 1.,
            multiplier,
            commission_type: self.commission_type,
            blowup: false,
            c_rate: self.c_rate,
        }
    }
}

impl DataLoader {
    pub fn calc_future_ret<'a, F: AsRef<[&'a str]>>(
        self,
        facs: F,
        opt: &FutureRetOpt,
    ) -> Result<Self> {
        let facs = facs.as_ref();
        let mut out = self.empty_copy();
        if self.multiplier.is_none() {
            out = out.with_multiplier()?;
        }
        let multiplier_map = out.multiplier.as_ref().unwrap();
        let dfs: Vec<Frame> = self
            .into_par_iter()
            .map(|(symbol, df)| {
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
                        let contract_chg_signal_vec = df.column(opt.contract_chg_signal).unwrap();
                        let contract_chg_signal_vec = auto_cast!(Boolean(contract_chg_signal_vec));
                        let (pos, open_vec, close_vec) =
                            auto_cast!(Float64(pos, open_vec, close_vec));
                        let multiplier = *multiplier_map.get(symbol.as_str()).unwrap();
                        let out: Float64Chunked = if opt.slippage_flag {
                            let slippage = df.column("twap_spread").unwrap() * 0.5;
                            let slippage_vec = auto_cast!(Float64(slippage));
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
                        out.with_name(&(f.to_string() + opt.suffix)).into_series()
                    })
                    .collect();
                let ecs: Vec<_> = ecs.into_iter().map(lit).collect();
                Frame::Eager(df).with_columns(&ecs).unwrap()
            })
            .collect();
        out.dfs = dfs.into();
        Ok(out)
    }
}
