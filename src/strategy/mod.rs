mod core_trait;
pub mod filters;
mod register;
mod signals;
pub mod stop_filters;
mod strategy_work;

pub use core_trait::{Strategy, StrategyBase};
pub use filters::{Filter, Filters};
pub use register::{register_strategy, STRATEGY_MAP};
pub use signals::*;
pub use stop_filters::{StopFilter, StopFilters};
pub use strategy_work::StrategyWork;
