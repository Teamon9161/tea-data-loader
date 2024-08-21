use std::fmt::Debug;
use std::str::FromStr;

use anyhow::{bail, Result};
use derive_more::{Deref, DerefMut, From};
use polars::prelude::{RollingCovOptions, RollingOptionsFixedWindow};

#[derive(Default, From, Clone, Copy, PartialEq)]
pub enum Param {
    I32(i32),
    F64(f64),
    #[default]
    None,
}

impl FromStr for Param {
    type Err = anyhow::Error;
    #[inline]
    fn from_str(s: &str) -> Result<Param> {
        if let Ok(v) = s.parse::<i32>() {
            Ok(Param::I32(v))
        } else if let Ok(v) = s.parse::<f64>() {
            Ok(Param::F64(v))
        } else if s.is_empty() || (s.to_lowercase().as_str() == "none") {
            Ok(Param::None)
        } else {
            bail!("Invalid param: {}", s)
        }
    }
}

impl From<usize> for Param {
    #[inline]
    fn from(v: usize) -> Self {
        Param::I32(v as i32)
    }
}

impl From<Param> for usize {
    #[inline]
    fn from(p: Param) -> Self {
        p.as_usize()
    }
}

impl From<Param> for i64 {
    #[inline]
    fn from(p: Param) -> Self {
        p.as_i32() as i64
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

impl Param {
    #[inline]
    pub fn is_int(&self) -> bool {
        matches!(self, Param::I32(_))
    }

    #[inline]
    pub fn is_float(&self) -> bool {
        matches!(self, Param::F64(_))
    }

    #[inline]
    pub fn as_i32(&self) -> i32 {
        match self {
            Param::I32(v) => *v,
            Param::None => 1, // special case
            _ => panic!("param is not i32"),
        }
    }

    #[inline]
    pub fn as_f64(&self) -> f64 {
        if let Param::F64(v) = self {
            *v
        } else {
            panic!("param is not f64")
        }
    }

    #[inline]
    pub fn as_u32(&self) -> u32 {
        self.as_i32() as u32
    }

    #[inline]
    pub fn as_usize(&self) -> usize {
        self.as_i32() as usize
    }

    #[inline]
    pub fn rolling_opt(&self) -> RollingOptionsFixedWindow {
        let n = self.as_usize();
        let min_periods = n / 2;
        RollingOptionsFixedWindow {
            window_size: n,
            min_periods,
            ..Default::default()
        }
    }

    #[inline]
    pub fn rolling_cov_opt(&self) -> RollingCovOptions {
        let n = self.as_u32();
        let min_periods = n / 2;
        RollingCovOptions {
            window_size: self.as_u32(),
            min_periods,
            ddof: 1,
        }
    }

    #[inline]
    pub fn is_none(&self) -> bool {
        matches!(self, Param::None)
    }
}

impl From<Param> for RollingOptionsFixedWindow {
    #[inline]
    fn from(param: Param) -> Self {
        param.rolling_opt()
    }
}

impl From<Param> for RollingCovOptions {
    #[inline]
    fn from(param: Param) -> Self {
        param.rolling_cov_opt()
    }
}

#[derive(Default, Clone, From, PartialEq, Deref, DerefMut)]
#[repr(transparent)]
pub struct Params(pub Vec<Param>);

impl Debug for Params {
    #[inline]
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

impl FromStr for Params {
    type Err = anyhow::Error;
    #[inline]
    fn from_str(s: &str) -> Result<Self> {
        let params = if s.contains(['[']) || s.contains('(') {
            let nodes = s
                .trim_matches(['[', ']', '(', ')'])
                .trim_end_matches(',')
                .replace(" ", "");
            nodes
                .split(",")
                .map(|n| n.parse())
                .try_collect::<Vec<Param>>()?
        } else {
            vec![s.parse()?]
        };
        Ok(Params(params))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_parse_params() -> Result<()> {
        let params: Params = "(100,)".parse()?;
        assert_eq!(params.0, vec![Param::I32(100)]);
        let params: Params = "(100)".parse()?;
        assert_eq!(params.0, vec![Param::I32(100)]);
        let params: Params = "1.5".parse()?;
        assert_eq!(params.0, vec![Param::F64(1.5)]);
        let params: Params = "[100, 1.5]".parse()?;
        assert_eq!(params.0, vec![Param::I32(100), Param::F64(1.5)]);
        let params: Params = "[100, ,1.5]".parse()?;
        assert_eq!(
            params.0,
            vec![Param::I32(100), Param::None, Param::F64(1.5)]
        );
        Ok(())
    }
}
