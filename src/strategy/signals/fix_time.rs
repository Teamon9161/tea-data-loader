use factor_macro::StrategyBase;
use polars::prelude::*;
pub use tea_strategy::FixTimeKwargs;

use crate::prelude::{register_strategy, GetName, Params};
use crate::strategy::{GetStrategyParamName, Strategy, StrategyBase};

#[derive(StrategyBase, Clone)]
pub struct FixTime(pub FixTimeKwargs);

impl GetStrategyParamName for FixTime {
    #[inline]
    fn get_param_name(&self) -> Arc<str> {
        format!("{:?}", self.0.n).into()
    }
}

impl From<Params> for FixTimeKwargs {
    fn from(value: Params) -> Self {
        match value.len() {
            0 => panic!("fix time strategy need a param"),
            1 => FixTimeKwargs {
                n: value[0].as_usize(),
                pos_map: None,
                extend_time: true,
            },
            2 => panic!("fix time strategy does not support 2 params"),
            3 => FixTimeKwargs {
                n: value[0].as_usize(),
                pos_map: Some((
                    vec![value[1].as_f64(), value[2].as_f64()],
                    vec![-1., 0., 1.],
                )),
                extend_time: true,
            },
            _ => panic!("Too many params for fix time strategy"),
        }
    }
}

impl From<Params> for FixTime {
    #[inline]
    fn from(value: Params) -> Self {
        FixTime(FixTimeKwargs::from(value))
    }
}

impl Strategy for FixTime {
    super::macros::impl_by_tea_strategy!(fix_time{?});
}

#[derive(Clone)]
pub struct NegFixTimeKwargs(pub FixTimeKwargs);

impl From<FixTimeKwargs> for NegFixTimeKwargs {
    fn from(value: FixTimeKwargs) -> Self {
        NegFixTimeKwargs(value)
    }
}

#[derive(StrategyBase, Clone)]
pub struct NegFixTime(pub NegFixTimeKwargs);

impl GetStrategyParamName for NegFixTime {
    #[inline]
    fn get_param_name(&self) -> Arc<str> {
        format!("{:?}", self.0 .0.n).into()
    }
}

impl From<Params> for NegFixTimeKwargs {
    fn from(value: Params) -> Self {
        match value.len() {
            0 => panic!("fix time strategy need a param"),
            1 => FixTimeKwargs {
                n: value[0].as_usize(),
                pos_map: Some((
                    vec![value[1].as_f64(), value[2].as_f64()],
                    vec![-1., 0., 1.],
                )),
                extend_time: true,
            }
            .into(),
            2 => panic!("fix time strategy does not support 2 params"),
            3 => FixTimeKwargs {
                n: value[0].as_usize(),
                pos_map: Some((
                    vec![value[1].as_f64(), value[2].as_f64()],
                    vec![1., 0., -1.],
                )),
                extend_time: true,
            }
            .into(),
            _ => panic!("Too many params for fix time strategy"),
        }
    }
}

impl From<Params> for NegFixTime {
    #[inline]
    fn from(value: Params) -> Self {
        NegFixTime(NegFixTimeKwargs::from(value))
    }
}

impl Strategy for NegFixTime {
    fn eval_to_fac(&self, fac: &Series, filters: Option<DataFrame>) -> anyhow::Result<Series> {
        FixTime(self.0 .0.clone()).eval_to_fac(fac, filters)
    }
}

#[derive(Clone)]
pub struct NegFixTimeLongKwargs(pub FixTimeKwargs);

#[derive(StrategyBase, Clone)]
pub struct NegFixTimeLong(pub NegFixTimeLongKwargs);

impl GetStrategyParamName for NegFixTimeLong {
    #[inline]
    fn get_param_name(&self) -> Arc<str> {
        format!("{:?}", self.0 .0.n).into()
    }
}

impl From<Params> for NegFixTimeLongKwargs {
    fn from(value: Params) -> Self {
        let mut ori_kwargs = NegFixTimeKwargs::from(value);
        if let Some(mut pos_map) = ori_kwargs.0.pos_map {
            pos_map.1 = pos_map.1.into_iter().map(|x| x.max(0.)).collect();
            ori_kwargs.0.pos_map = Some(pos_map);
        }
        NegFixTimeLongKwargs(ori_kwargs.0)
    }
}

impl From<Params> for NegFixTimeLong {
    fn from(value: Params) -> Self {
        NegFixTimeLong(NegFixTimeLongKwargs::from(value))
    }
}

impl Strategy for NegFixTimeLong {
    fn eval_to_fac(&self, fac: &Series, filters: Option<DataFrame>) -> anyhow::Result<Series> {
        FixTime(self.0 .0.clone()).eval_to_fac(fac, filters)
    }
}
#[derive(Clone)]
pub struct NegFixTimeShortKwargs(pub FixTimeKwargs);

#[derive(StrategyBase, Clone)]
pub struct NegFixTimeShort(pub NegFixTimeShortKwargs);

impl GetStrategyParamName for NegFixTimeShort {
    #[inline]
    fn get_param_name(&self) -> Arc<str> {
        format!("{:?}", self.0 .0.n).into()
    }
}

impl From<Params> for NegFixTimeShortKwargs {
    fn from(value: Params) -> Self {
        let mut ori_kwargs = NegFixTimeKwargs::from(value);
        if let Some(mut pos_map) = ori_kwargs.0.pos_map {
            pos_map.1 = pos_map.1.into_iter().map(|x| x.min(0.)).collect();
            ori_kwargs.0.pos_map = Some(pos_map);
        }
        NegFixTimeShortKwargs(ori_kwargs.0)
    }
}

impl From<Params> for NegFixTimeShort {
    fn from(value: Params) -> Self {
        NegFixTimeShort(NegFixTimeShortKwargs::from(value))
    }
}

impl Strategy for NegFixTimeShort {
    fn eval_to_fac(&self, fac: &Series, filters: Option<DataFrame>) -> anyhow::Result<Series> {
        FixTime(self.0 .0.clone()).eval_to_fac(fac, filters)
    }
}

#[ctor::ctor]
fn register() {
    register_strategy::<FixTime>().unwrap();
    register_strategy::<NegFixTime>().unwrap();
    register_strategy::<NegFixTimeLong>().unwrap();
    register_strategy::<NegFixTimeShort>().unwrap();
}
