use std::any;
use std::fmt::Debug;
use std::str::FromStr;

use anyhow::{bail, Result};
use derive_more::From;

#[derive(Default, From)]
pub enum Param {
    I32(i32),
    F64(f64),
    #[default]
    None,
}

impl FromStr for Param {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Param> {
        if let Ok(v) = s.parse::<i32>() {
            Ok(Param::I32(v))
        } else if let Ok(v) = s.parse::<f64>() {
            Ok(Param::F64(v))
        } else if (s == "") || (s.to_lowercase().as_str() == "none") {
            Ok(Param::None)
        } else {
            bail!("Invalid param: {}", s)
        }
    }
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
