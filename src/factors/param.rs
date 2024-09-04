use std::fmt::Debug;
use std::str::FromStr;

use anyhow::{bail, Result};
use derive_more::{Deref, DerefMut, From};
use polars::prelude::{RollingCovOptions, RollingOptionsFixedWindow};

/// An enumeration type for factor parameters.
///
/// This enum represents different types of parameters that can be used in factor calculations.
/// It supports three variants: integer (`i32`), floating-point (`f64`), and `None` for cases
/// where no parameter is needed.
///
/// # Variants
///
/// * `I32(i32)` - Represents an integer parameter.
/// * `F64(f64)` - Represents a floating-point parameter.
/// * `None` - Represents the absence of a parameter.
///
/// # Examples
///
/// ```
/// use tea_data_loader::factors::Param;
///
/// // Creating an integer parameter
/// let int_param: Param = "100".parse().unwrap();
/// assert_eq!(int_param, Param::I32(100));
///
/// // Creating a floating-point parameter
/// let float_param: Param = "1.5".parse().unwrap();
/// assert_eq!(float_param, Param::F64(1.5));
///
/// // Creating a None parameter
/// let none_param: Param = "".parse().unwrap();
/// assert_eq!(none_param, Param::None);
///
/// // Alternative way to create a None parameter
/// let alt_none_param: Param = "none".parse().unwrap();
/// assert_eq!(alt_none_param, Param::None);
/// ```
#[derive(Default, From, Clone, Copy, PartialEq)]
pub enum Param {
    /// Represents an integer parameter.
    I32(i32),
    /// Represents a floating-point parameter.
    F64(f64),
    /// Represents the absence of a parameter. This is the default variant.
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
    /// Checks if the parameter is an integer.
    #[inline]
    pub fn is_int(&self) -> bool {
        matches!(self, Param::I32(_))
    }

    /// Checks if the parameter is a floating-point number.
    #[inline]
    pub fn is_float(&self) -> bool {
        matches!(self, Param::F64(_))
    }

    /// Converts the parameter to an i32.
    ///
    /// # Panics
    ///
    /// Panics if the parameter is not an i32 or None.
    #[inline]
    pub fn as_i32(&self) -> i32 {
        match self {
            Param::I32(v) => *v,
            Param::None => 1, // special case
            _ => panic!("param is not i32"),
        }
    }

    /// Converts the parameter to an f64.
    ///
    /// # Panics
    ///
    /// Panics if the parameter is not an f64.
    #[inline]
    pub fn as_f64(&self) -> f64 {
        if let Param::F64(v) = self {
            *v
        } else {
            panic!("param is not f64")
        }
    }

    /// Converts the parameter to a u32.
    #[inline]
    pub fn as_u32(&self) -> u32 {
        self.as_i32() as u32
    }

    /// Converts the parameter to a usize.
    #[inline]
    pub fn as_usize(&self) -> usize {
        self.as_i32() as usize
    }

    /// Creates a Polars RollingOptionsFixedWindow from the parameter.
    ///
    /// This method converts the parameter to a RollingOptionsFixedWindow, which is used
    /// for rolling window operations in Polars.
    ///
    /// # Details
    /// - The window_size is set to the parameter value converted to usize.
    /// - The min_periods is set to half of the window_size (rounded down).
    /// - Other options use their default values.
    ///
    /// # Returns
    /// A RollingOptionsFixedWindow with the specified window_size and min_periods.
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

    /// Creates a Polars RollingCovOptions from the parameter.
    ///
    /// This method converts the parameter to a RollingCovOptions, which is used
    /// for rolling covariance operations in Polars.
    ///
    /// # Details
    /// - The window_size is set to the parameter value converted to u32.
    /// - The min_periods is set to half of the window_size (rounded down).
    /// - The ddof (delta degrees of freedom) is set to 1.
    ///
    /// # Returns
    /// A RollingCovOptions with the specified window_size, min_periods, and ddof.
    #[inline]
    pub fn rolling_cov_opt(&self) -> RollingCovOptions {
        let n = self.as_u32();
        let min_periods = n / 2;
        RollingCovOptions {
            window_size: n,
            min_periods,
            ddof: 1,
        }
    }

    /// Checks if the parameter is None.
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
/// A collection of `Param` values, primarily used for strategy parameters.
///
/// `Params` is a wrapper around a `Vec<Param>` that provides a convenient way to
/// handle multiple strategy parameters. It's particularly useful because strategy
/// parameters are often numerous and can be of different types, which is why the
/// `Param` enum is used as the underlying type.
///
/// This struct implements `Default`, `Clone`, `From`, `PartialEq`, `Deref`, and
/// `DerefMut` traits for ease of use in various contexts.
///
/// The `#[repr(transparent)]` attribute ensures that `Params` has the same
/// memory layout as `Vec<Param>`, allowing for efficient conversions and
/// interoperability with functions expecting a `Vec<Param>`.
///
/// # Examples
///
/// ```
/// use tea_data_loader::factors::{Param, Params};
///
/// let params = Params(vec![Param::I32(100), Param::F64(0.5), Param::None]);
/// ```
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
