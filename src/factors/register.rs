use std::collections::HashMap;
use std::sync::Arc;

use anyhow::{bail, Result};
use once_cell::sync::Lazy;
use parking_lot::Mutex;

use super::{FactorBase, Param, PlFactor, TFactor};

pub type PlFacInitFunc = Arc<dyn Fn(Param) -> Arc<dyn PlFactor> + Send + Sync>;
pub type TFacInitFunc = Arc<dyn Fn(Param) -> Arc<dyn TFactor> + Send + Sync>;

pub static POLARS_FAC_MAP: Lazy<Mutex<HashMap<Arc<str>, PlFacInitFunc>>> =
    Lazy::new(|| Mutex::new(HashMap::with_capacity(100)));

pub static T_FAC_MAP: Lazy<Mutex<HashMap<Arc<str>, TFacInitFunc>>> =
    Lazy::new(|| Mutex::new(HashMap::with_capacity(100)));

#[inline]
pub fn register_pl_fac<P: FactorBase + PlFactor>() -> Result<()> {
    let exists_flag = POLARS_FAC_MAP.lock().contains_key(&P::fac_name());
    if exists_flag {
        bail!("Factor {} already exists", &P::fac_name());
    } else {
        POLARS_FAC_MAP
            .lock()
            .insert(P::fac_name(), Arc::new(|param| Arc::new(P::new(param))));
        Ok(())
    }
}

#[inline]
pub fn register_t_fac<P: FactorBase + TFactor>() -> Result<()> {
    let exists_flag = T_FAC_MAP.lock().contains_key(&P::fac_name());
    if exists_flag {
        bail!("Factor {} already exists", &P::fac_name());
    } else {
        T_FAC_MAP
            .lock()
            .insert(P::fac_name(), Arc::new(|param| Arc::new(P::new(param))));
        Ok(())
    }
}

#[inline]
pub fn register_fac<P: FactorBase + PlFactor + TFactor>() -> Result<()> {
    register_pl_fac::<P>()?;
    register_t_fac::<P>()?;
    Ok(())
}
