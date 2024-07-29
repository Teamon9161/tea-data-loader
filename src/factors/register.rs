use std::collections::HashMap;
use std::sync::{Arc, LazyLock};

use anyhow::{bail, Result};
// use once_cell::sync::Lazy;
use parking_lot::Mutex;

use super::{FactorBase, Param, PlFactor, TFactor};

pub type PlFacInitFunc = Arc<dyn Fn(Param) -> Arc<dyn PlFactor> + Send + Sync>;
pub type TFacInitFunc = Arc<dyn Fn(Param) -> Arc<dyn TFactor> + Send + Sync>;

pub static POLARS_FAC_MAP: LazyLock<Mutex<HashMap<Arc<str>, PlFacInitFunc>>> =
    LazyLock::new(|| Mutex::new(HashMap::with_capacity(100)));

pub static T_FAC_MAP: LazyLock<Mutex<HashMap<Arc<str>, TFacInitFunc>>> =
    LazyLock::new(|| Mutex::new(HashMap::with_capacity(100)));

#[inline]
pub fn register_pl_fac<P: FactorBase + PlFactor>() -> Result<()> {
    if POLARS_FAC_MAP
        .lock()
        .insert(P::fac_name(), Arc::new(|param| Arc::new(P::new(param))))
        .is_some()
    {
        bail!("Factor {} already exists", &P::fac_name());
    } else {
        Ok(())
    }
}

#[inline]
pub fn register_t_fac<P: FactorBase + TFactor>() -> Result<()> {
    if T_FAC_MAP
        .lock()
        .insert(P::fac_name(), Arc::new(|param| Arc::new(P::new(param))))
        .is_some()
    {
        bail!("Factor {} already exists", &P::fac_name());
    } else {
        Ok(())
    }
}

#[inline]
pub fn register_fac<P: FactorBase + PlFactor + TFactor>() -> Result<()> {
    register_pl_fac::<P>()?;
    register_t_fac::<P>()?;
    Ok(())
}
