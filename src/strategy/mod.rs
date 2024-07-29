mod core_trait;
mod register;
mod signals;
mod strategy_work;

pub use core_trait::{Strategy, StrategyBase};
pub use register::{register_strategy, STRATEGY_MAP};
pub use signals::*;
pub use strategy_work::StrategyWork;
