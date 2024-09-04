use std::collections::HashMap;
use std::sync::{Arc, LazyLock};

use anyhow::{bail, Result};
use parking_lot::Mutex;

use super::{FactorBase, Param, PlFactor, TFactor};

pub type PlFacInitFunc = Arc<dyn Fn(Param) -> Arc<dyn PlFactor> + Send + Sync>;
pub type TFacInitFunc = Arc<dyn Fn(Param) -> Arc<dyn TFactor> + Send + Sync>;
/// A global map storing Polars factor initialization functions.
///
/// This map associates factor names with their corresponding initialization functions.
/// It is lazily initialized and protected by a mutex for thread-safe access.
/// A global map storing Polars factor initialization functions.
///
/// This map associates factor names with their corresponding initialization functions.
/// It is lazily initialized and protected by a mutex for thread-safe access.
pub static POLARS_FAC_MAP: LazyLock<Mutex<HashMap<Arc<str>, PlFacInitFunc>>> =
    LazyLock::new(|| Mutex::new(HashMap::with_capacity(100)));

/// A global map storing T factor initialization functions.
///
/// This map associates factor names with their corresponding initialization functions.
/// It is lazily initialized and protected by a mutex for thread-safe access.
pub static T_FAC_MAP: LazyLock<Mutex<HashMap<Arc<str>, TFacInitFunc>>> =
    LazyLock::new(|| Mutex::new(HashMap::with_capacity(100)));

/// Registers a Polars factor.
///
/// This function adds a new Polars factor to the global `POLARS_FAC_MAP`.
/// If a factor with the same name already exists, it returns an error.
///
/// This function can be used by other crates to implement the necessary traits
/// and register new factors, allowing for easy extension of the factor system.
///
/// # Type Parameters
///
/// * `P`: A type that implements both `FactorBase` and `PlFactor` traits.
///
/// # Returns
///
/// * `Result<()>`: Ok if the registration is successful, Err if the factor already exists.
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

/// Registers a T factor.
///
/// This function adds a new T factor to the global `T_FAC_MAP`.
/// If a factor with the same name already exists, it returns an error.
///
/// This function can be used by other crates to implement the necessary traits
/// and register new factors, allowing for easy extension of the factor system.
///
/// # Type Parameters
///
/// * `P`: A type that implements both `FactorBase` and `TFactor` traits.
///
/// # Returns
///
/// * `Result<()>`: Ok if the registration is successful, Err if the factor already exists.
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

/// Registers both Polars and T factors.
///
/// This function is a convenience wrapper that calls both `register_pl_fac` and `register_t_fac`.
/// It's useful for factors that implement both `PlFactor` and `TFactor` traits.
///
/// This function can be used by other crates to implement the necessary traits
/// and register new factors of both types simultaneously, allowing for easy extension
/// of the factor system.
///
/// # Type Parameters
///
/// * `P`: A type that implements `FactorBase`, `PlFactor`, and `TFactor` traits.
///
/// # Returns
///
/// * `Result<()>`: Ok if both registrations are successful, Err if either registration fails.
#[inline]
pub fn register_fac<P: FactorBase + PlFactor + TFactor>() -> Result<()> {
    register_pl_fac::<P>()?;
    register_t_fac::<P>()?;
    Ok(())
}
