use tea_strategy::BollKwargs;

use crate::strategy::Strategy;

pub struct Boll {
    pub args: BollKwargs,
}

impl Strategy for Boll {
    super::macros::impl_by_tea_strategy!(boll);
}
