use std::sync::LazyLock;

use polars::prelude::*;
use tea_strategy::tevec::prelude::{Time, Timelike};

use crate::factors::export::*;

#[derive(FactorBase, FromParam, Default, Clone, Copy)]
pub struct AtTime;

const MORNING_START_TIME: Time = Time::from_hms(9, 30, 0);
const MORNING_END_TIME: Time = Time::from_hms(11, 30, 0);
const AFTERNOON_START_TIME: Time = Time::from_hms(13, 0, 0);
// const AFTERNOON_END_TIME: Time = Time::from_hms(15, 15, 0);
const SEC_PER_MIN: f64 = 60.0;
static MORNING_MINUTES: LazyLock<f64> =
    LazyLock::new(|| get_minutes_between(MORNING_START_TIME, MORNING_END_TIME) as f64);

fn get_minutes_between(start: Time, end: Time) -> i32 {
    (start.hour() as i32 - end.hour() as i32) * 60 + start.minute() as i32 - end.minute() as i32
}

impl PlFactor for AtTime {
    fn try_expr(&self) -> Result<Expr> {
        let morning_time = (col("time")
            - col("time")
                .dt()
                .combine(MORNING_START_TIME.lit(), TimeUnit::Milliseconds))
        .dt()
        .total_seconds();
        let afternoon_time = (col("time")
            - col("time")
                .dt()
                .combine(AFTERNOON_START_TIME.lit(), TimeUnit::Milliseconds))
        .dt()
        .total_seconds()
            + (*MORNING_MINUTES * SEC_PER_MIN).lit();
        let time = dsl::when(col("time").dt().time().lt_eq(MORNING_END_TIME.lit()))
            .then(morning_time)
            .otherwise(afternoon_time);
        Ok(time)
    }
}

#[ctor::ctor]
fn register() {
    register_pl_fac::<AtTime>().unwrap()
}
