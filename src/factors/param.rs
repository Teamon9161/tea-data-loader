use std::fmt::Debug;
use std::str::FromStr;

use anyhow::{bail, Result};
use derive_more::{Deref, DerefMut, From};

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
    pub fn as_i32(&self) -> i32 {
        if let Param::I32(v) = self {
            *v
        } else {
            panic!("param is not i32")
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
    pub fn is_none(&self) -> bool {
        matches!(self, Param::None)
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
