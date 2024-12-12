mod boll;
use std::ops::Deref;

pub use boll::{Boll, BollKwargs, BollLong, BollLongKwargs, BollShort, BollShortKwargs};
pub(super) mod macros;

mod fix_time;
pub use fix_time::FixTime;

#[derive(Clone)]
#[repr(transparent)]
pub struct Wrap<T>(pub T);

impl<T> Deref for Wrap<T> {
    type Target = T;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<T> From<&Wrap<T>> for &T {
    fn from(value: &Wrap<T>) -> &T {
        &value.0
    }
}