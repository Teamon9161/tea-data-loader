use anyhow::Result;
use factor_macro::StrategyBase;
use polars::prelude::*;
pub use tea_strategy::BollKwargs;

use crate::prelude::{register_strategy, GetName, Params};
use crate::strategy::{GetStrategyParamName, Strategy, StrategyBase};

#[derive(StrategyBase, Clone)]
pub struct Boll(pub BollKwargs);

impl GetStrategyParamName for Boll {
    #[inline]
    fn get_param_name(&self) -> Arc<str> {
        format!("{:?}", self.0.params).into()
    }
}

impl From<Params> for BollKwargs {
    fn from(value: Params) -> Self {
        match value.len() {
            0 => panic!("boll strategy need a param"),
            1 => BollKwargs::new(value[0].as_usize(), 0.),
            2 => BollKwargs::new(value[0].as_usize(), value[1].as_f64()),
            3 => BollKwargs {
                params: (
                    value[0].as_usize(),
                    value[1].as_f64(),
                    value[2].as_f64(),
                    None,
                ),
                ..Default::default()
            },
            4 => BollKwargs {
                params: (
                    value[0].as_usize(),
                    value[1].as_f64(),
                    value[2].as_f64(),
                    Some(value[3].as_f64()),
                ),
                ..Default::default()
            },
            _ => panic!("Too many params for boll strategy"),
        }
    }
}

impl From<Params> for Boll {
    #[inline]
    fn from(value: Params) -> Self {
        Boll(BollKwargs::from(value))
    }
}

impl Strategy for Boll {
    super::macros::impl_by_tea_strategy!(boll);
}

#[derive(Clone)]
pub struct NegBollKwargs(pub BollKwargs);

impl From<BollKwargs> for NegBollKwargs {
    fn from(kwargs: BollKwargs) -> Self {
        let mut kwargs = kwargs;
        kwargs.long_signal = -kwargs.long_signal;
        kwargs.short_signal = -kwargs.short_signal;
        NegBollKwargs(kwargs)
    }
}

impl From<Params> for NegBollKwargs {
    fn from(value: Params) -> Self {
        let kwargs = BollKwargs::from(value);
        kwargs.into()
    }
}

impl From<Params> for NegBoll {
    #[inline]
    fn from(value: Params) -> Self {
        NegBoll(NegBollKwargs::from(value))
    }
}

impl GetStrategyParamName for NegBoll {
    #[inline]
    fn get_param_name(&self) -> Arc<str> {
        format!("{:?}", self.0 .0.params).into()
    }
}

#[derive(StrategyBase, Clone)]
pub struct NegBoll(pub NegBollKwargs);

impl Strategy for NegBoll {
    fn eval_to_fac(&self, fac: &Column, filters: Option<DataFrame>) -> Result<Series> {
        let strategy = Boll(self.0 .0.clone());
        strategy.eval_to_fac(fac, filters)
    }
}

#[derive(Clone)]
pub struct BollLongKwargs(pub BollKwargs);

impl From<BollKwargs> for BollLongKwargs {
    fn from(kwargs: BollKwargs) -> Self {
        let mut kwargs = kwargs;
        kwargs.short_signal = kwargs.close_signal;
        BollLongKwargs(kwargs)
    }
}

impl From<Params> for BollLongKwargs {
    fn from(value: Params) -> Self {
        let kwargs = BollKwargs::from(value);
        kwargs.into()
    }
}

impl From<Params> for BollLong {
    #[inline]
    fn from(value: Params) -> Self {
        BollLong(BollLongKwargs::from(value))
    }
}

impl GetStrategyParamName for BollLong {
    #[inline]
    fn get_param_name(&self) -> Arc<str> {
        format!("{:?}", self.0 .0.params).into()
    }
}

#[derive(StrategyBase, Clone)]
pub struct BollLong(pub BollLongKwargs);

impl Strategy for BollLong {
    fn eval_to_fac(&self, fac: &Column, filters: Option<DataFrame>) -> Result<Series> {
        let strategy = Boll(self.0 .0.clone());
        strategy.eval_to_fac(fac, filters)
    }
}

#[derive(Clone)]
pub struct BollShortKwargs(pub BollKwargs);

impl From<Params> for BollShortKwargs {
    fn from(value: Params) -> Self {
        let mut kwargs = BollKwargs::from(value);
        kwargs.long_signal = kwargs.close_signal;
        BollShortKwargs(kwargs)
    }
}

impl From<BollKwargs> for BollShortKwargs {
    fn from(kwargs: BollKwargs) -> Self {
        let mut kwargs = kwargs;
        kwargs.long_signal = kwargs.close_signal;
        BollShortKwargs(kwargs)
    }
}

impl From<Params> for BollShort {
    fn from(value: Params) -> Self {
        BollShort(BollShortKwargs::from(value))
    }
}

impl GetStrategyParamName for BollShort {
    #[inline]
    fn get_param_name(&self) -> Arc<str> {
        format!("{:?}", self.0 .0.params).into()
    }
}

#[derive(StrategyBase, Clone)]
pub struct BollShort(pub BollShortKwargs);

impl Strategy for BollShort {
    fn eval_to_fac(&self, fac: &Column, filters: Option<DataFrame>) -> Result<Series> {
        let strategy = Boll(self.0 .0.clone());
        strategy.eval_to_fac(fac, filters)
    }
}

#[derive(Clone)]
pub struct BollDirectKwargs(pub BollKwargs);

impl From<BollKwargs> for BollDirectKwargs {
    fn from(kwargs: BollKwargs) -> Self {
        let mut kwargs = kwargs;
        kwargs.zscore = false;
        BollDirectKwargs(kwargs)
    }
}

impl From<Params> for BollDirectKwargs {
    fn from(value: Params) -> Self {
        let mut kwargs = BollKwargs::from(value);
        kwargs.zscore = false;
        BollDirectKwargs(kwargs)
    }
}

impl From<Params> for BollDirect {
    #[inline]
    fn from(value: Params) -> Self {
        BollDirect(BollDirectKwargs::from(value))
    }
}

impl GetStrategyParamName for BollDirect {
    #[inline]
    fn get_param_name(&self) -> Arc<str> {
        format!("{:?}", self.0 .0.params).into()
    }
}
#[derive(Clone)]
pub struct BollDirectLongKwargs(pub BollKwargs);

impl From<BollKwargs> for BollDirectLongKwargs {
    fn from(kwargs: BollKwargs) -> Self {
        let mut kwargs = kwargs;
        kwargs.zscore = false;
        kwargs.short_signal = kwargs.close_signal;
        BollDirectLongKwargs(kwargs)
    }
}

impl From<Params> for BollDirectLongKwargs {
    fn from(value: Params) -> Self {
        let mut kwargs = BollKwargs::from(value);
        kwargs.zscore = false;
        kwargs.short_signal = kwargs.close_signal;
        BollDirectLongKwargs(kwargs)
    }
}

impl From<Params> for BollDirectLong {
    #[inline]
    fn from(value: Params) -> Self {
        BollDirectLong(BollDirectLongKwargs::from(value))
    }
}

impl GetStrategyParamName for BollDirectLong {
    #[inline]
    fn get_param_name(&self) -> Arc<str> {
        format!("{:?}", self.0 .0.params).into()
    }
}

#[derive(StrategyBase, Clone)]
pub struct BollDirectLong(pub BollDirectLongKwargs);

impl Strategy for BollDirectLong {
    fn eval_to_fac(&self, fac: &Column, filters: Option<DataFrame>) -> Result<Series> {
        let strategy = Boll(self.0 .0.clone());
        strategy.eval_to_fac(fac, filters)
    }
}

#[derive(Clone)]
pub struct BollDirectShortKwargs(pub BollKwargs);

impl From<BollKwargs> for BollDirectShortKwargs {
    fn from(kwargs: BollKwargs) -> Self {
        let mut kwargs = kwargs;
        kwargs.zscore = false;
        kwargs.long_signal = kwargs.close_signal;
        BollDirectShortKwargs(kwargs)
    }
}

impl From<Params> for BollDirectShortKwargs {
    fn from(value: Params) -> Self {
        let mut kwargs = BollKwargs::from(value);
        kwargs.zscore = false;
        kwargs.long_signal = kwargs.close_signal;
        BollDirectShortKwargs(kwargs)
    }
}

impl From<Params> for BollDirectShort {
    #[inline]
    fn from(value: Params) -> Self {
        BollDirectShort(BollDirectShortKwargs::from(value))
    }
}

impl GetStrategyParamName for BollDirectShort {
    #[inline]
    fn get_param_name(&self) -> Arc<str> {
        format!("{:?}", self.0 .0.params).into()
    }
}

#[derive(StrategyBase, Clone)]
pub struct BollDirectShort(pub BollDirectShortKwargs);

impl Strategy for BollDirectShort {
    fn eval_to_fac(&self, fac: &Column, filters: Option<DataFrame>) -> Result<Series> {
        let strategy = Boll(self.0 .0.clone());
        strategy.eval_to_fac(fac, filters)
    }
}

#[derive(StrategyBase, Clone)]
pub struct BollDirect(pub BollDirectKwargs);

impl Strategy for BollDirect {
    fn eval_to_fac(&self, fac: &Column, filters: Option<DataFrame>) -> Result<Series> {
        let strategy = Boll(self.0 .0.clone());
        strategy.eval_to_fac(fac, filters)
    }
}

#[ctor::ctor]
fn register() {
    register_strategy::<Boll>().unwrap();
    register_strategy::<NegBoll>().unwrap();
    register_strategy::<BollLong>().unwrap();
    register_strategy::<BollShort>().unwrap();
    register_strategy::<BollDirect>().unwrap();
    register_strategy::<BollDirectLong>().unwrap();
    register_strategy::<BollDirectShort>().unwrap();
}
