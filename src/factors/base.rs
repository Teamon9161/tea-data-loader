use std::sync::Arc;

use polars::prelude::{Literal, LiteralValue, Null as PlNull};

use super::export::*;

define_base_fac!(
    TradingDate: "交易日",
    Time: "时间"
);

/// Represents a null factor value.
///
/// This struct is used to represent null or missing values in factor calculations.
/// It implements various traits to allow seamless integration with the factor system.
#[derive(Clone, Copy)]
pub struct Null;

/// A constant instance of the `Null` struct.
///
/// This provides a convenient way to represent null values without needing to construct
/// a new `Null` instance each time.
pub const NONE: Null = Null {};

impl Literal for Null {
    #[inline]
    fn lit(self) -> Expr {
        Expr::Literal(LiteralValue::Null)
    }
}

impl FactorBase for Null {
    #[inline]
    fn fac_name() -> Arc<str> {
        "null".into()
    }

    #[inline]
    fn new(_param: impl Into<Param>) -> Self {
        NONE
    }
}

impl std::fmt::Debug for Null {
    #[inline]
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "null")
    }
}

impl PlFactor for Null {
    #[inline]
    fn try_expr(&self) -> Result<Expr> {
        Ok(PlNull {}.lit())
    }
}

impl TFactor for Null {
    #[inline]
    fn eval(&self, df: &DataFrame) -> Result<Series> {
        Ok(Series::new_null("__null__".into(), df.height()))
    }
}

impl Into<Expr> for Null {
    #[inline]
    fn into(self) -> Expr {
        PlNull {}.lit()
    }
}

impl From<Null> for PlNull {
    #[inline]
    fn from(_value: Null) -> Self {
        PlNull {}
    }
}

impl From<PlNull> for Null {
    #[inline]
    fn from(_value: PlNull) -> Self {
        NONE
    }
}

impl From<Null> for Param {
    #[inline]
    fn from(_value: Null) -> Self {
        Param::None
    }
}

/// Represents a direct factor that uses an existing column from a DataFrame without any calculation.
///
/// This struct wraps a column name as a String and treats that column directly as a factor.
/// It's useful when you want to use raw data from the DataFrame as a factor without any
/// additional computation or transformation.
///
/// The String parameter specifies the name of the column to be used as the factor.
#[derive(FromParam, Clone)]
pub struct Direct(pub String);

impl std::fmt::Debug for Direct {
    #[inline]
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl FactorBase for Direct {
    #[inline]
    fn fac_name() -> Arc<str> {
        "Direct".into()
    }
}

impl From<String> for Direct {
    #[inline]
    fn from(value: String) -> Self {
        Self(value)
    }
}

impl From<&str> for Direct {
    #[inline]
    fn from(value: &str) -> Self {
        Self(value.to_string())
    }
}

impl PlFactor for Direct {
    #[inline]
    fn try_expr(&self) -> Result<Expr> {
        Ok(col(self.0.as_str()))
    }
}
