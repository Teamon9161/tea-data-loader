pub(super) mod export;
pub mod map;
use std::collections::HashMap;
use std::fmt::Debug;

use anyhow::{bail, Result};
use derive_more::From;
use once_cell::sync::Lazy;
use parking_lot::Mutex;
use polars::prelude::*;

pub trait FactorBase: Sized {
    fn fac_name() -> Arc<str>;

    fn new<P: Into<Param>>(param: P) -> Self;
}

pub trait GetFacName {
    fn name(&self) -> String;
}

pub trait PlFactor: GetFacName + Send + Sync + 'static {
    fn try_expr(&self) -> Result<Expr>;

    #[inline]
    fn expr(&self) -> Expr {
        self.try_expr().unwrap()
    }
}

pub trait TFactor: GetFacName + Send + Sync + 'static {
    fn eval(&self, df: &DataFrame) -> Result<Series>;
}

pub type PlFacInitFunc = Arc<dyn Fn(Param) -> Arc<dyn PlFactor> + Send + Sync>;
pub type TFacInitFunc = Arc<dyn Fn(Param) -> Arc<dyn TFactor> + Send + Sync>;

pub static POLARS_FAC_MAP: Lazy<Mutex<HashMap<Arc<str>, PlFacInitFunc>>> =
    Lazy::new(|| Mutex::new(HashMap::with_capacity(100)));

pub static TFAC_MAP: Lazy<Mutex<HashMap<Arc<str>, TFacInitFunc>>> =
    Lazy::new(|| Mutex::new(HashMap::with_capacity(100)));

#[inline]
pub fn register_pl_factor<P: FactorBase + PlFactor>() -> Result<()> {
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
pub fn register_tfactor<P: FactorBase + TFactor>() -> Result<()> {
    let exists_flag = TFAC_MAP.lock().contains_key(&P::fac_name());
    if exists_flag {
        bail!("Factor {} already exists", &P::fac_name());
    } else {
        TFAC_MAP
            .lock()
            .insert(P::fac_name(), Arc::new(|param| Arc::new(P::new(param))));
        Ok(())
    }
}

#[inline]
pub fn register_factor<P: FactorBase + PlFactor + TFactor>() -> Result<()> {
    register_pl_factor::<P>()?;
    register_tfactor::<P>()?;
    Ok(())
}

#[derive(Default, From)]
pub enum Param {
    I32(i32),
    F64(f64),
    #[default]
    None,
}

impl Debug for Param {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Param::I32(v) => write!(f, "{}", v),
            Param::F64(v) => write!(f, "{}", v),
            Param::None => write!(f, ""),
        }
    }
}

unsafe impl Send for Param {}
unsafe impl Sync for Param {}
