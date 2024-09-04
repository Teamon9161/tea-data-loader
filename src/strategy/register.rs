use std::collections::HashMap;
use std::sync::{Arc, LazyLock};

use anyhow::{bail, Result};
use parking_lot::Mutex;

use super::{Strategy, StrategyBase};
use crate::prelude::Params;
/// Type alias for a function that initializes a strategy.
/// It takes `Params` as input and returns an `Arc<dyn Strategy>`.
pub type StrategyInitFunc = Arc<dyn Fn(Params) -> Arc<dyn Strategy> + Send + Sync>;

/// A global map that stores strategy initialization functions.
/// The key is the strategy name, and the value is the initialization function.
pub static STRATEGY_MAP: LazyLock<Mutex<HashMap<Arc<str>, StrategyInitFunc>>> =
    LazyLock::new(|| Mutex::new(HashMap::with_capacity(30)));

/// Registers a new strategy in the global `STRATEGY_MAP`.
///
/// # Type Parameters
///
/// * `S`: A type that implements both `Strategy` and `StrategyBase` traits.
///
/// # Returns
///
/// * `Ok(())` if the strategy was successfully registered.
/// * `Err` with an error message if a strategy with the same name already exists.
#[inline]
pub fn register_strategy<S: Strategy + StrategyBase>() -> Result<()> {
    if STRATEGY_MAP
        .lock()
        .insert(
            S::strategy_name(),
            Arc::new(|params| Arc::new(S::new(params))),
        )
        .is_some()
    {
        bail!("Strategy {} already exists", &S::strategy_name());
    } else {
        Ok(())
    }
}
