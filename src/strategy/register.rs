use std::collections::HashMap;
use std::sync::{Arc, LazyLock};

use anyhow::{bail, Result};
use parking_lot::Mutex;

use super::{Strategy, StrategyBase};
use crate::prelude::Params;

pub type StrategyInitFunc = Arc<dyn Fn(Params) -> Arc<dyn Strategy> + Send + Sync>;
pub static STRATEGY_MAP: LazyLock<Mutex<HashMap<Arc<str>, StrategyInitFunc>>> =
    LazyLock::new(|| Mutex::new(HashMap::with_capacity(30)));

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
