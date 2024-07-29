use factor_macro::StrategyBase;
use tea_strategy::BollKwargs;

use crate::prelude::{register_strategy, GetName, Params};
use crate::strategy::{Strategy, StrategyBase};

#[derive(StrategyBase)]
pub struct Boll(pub BollKwargs);

impl From<Params> for BollKwargs {
    fn from(value: Params) -> Self {
        match value.len() {
            0 => panic!("boll strategy need a param"),
            1 => BollKwargs::new(value[0].as_i32() as usize, 0.),
            2 => BollKwargs::new(value[0].as_i32() as usize, value[1].as_f64()),
            3 => BollKwargs {
                params: (
                    value[0].as_i32() as usize,
                    value[1].as_f64(),
                    value[2].as_f64(),
                    None,
                ),
                ..Default::default()
            },
            4 => BollKwargs {
                params: (
                    value[0].as_i32() as usize,
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

impl Strategy for Boll {
    super::macros::impl_by_tea_strategy!(boll);
}

#[ctor::ctor]
fn register() {
    register_strategy::<Boll>().unwrap()
}
