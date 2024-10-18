use std::sync::Arc;

use polars::prelude::Literal;

use super::export::*;

impl FactorBase for bool {
    #[inline]
    fn fac_name() -> Arc<str> {
        "bool".into()
    }

    #[inline]
    fn new(param: impl Into<Param>) -> Self {
        param.into().as_bool()
    }
}

impl PlFactor for bool {
    fn try_expr(&self) -> Result<Expr> {
        Ok(self.lit())
    }
}

impl FactorBase for usize {
    #[inline]
    fn fac_name() -> Arc<str> {
        "usize".into()
    }

    #[inline]
    fn new(param: impl Into<Param>) -> Self {
        param.into().as_usize()
    }
}

impl PlFactor for usize {
    fn try_expr(&self) -> Result<Expr> {
        Ok((*self as i64).lit())
    }
}

impl FactorBase for i32 {
    #[inline]
    fn fac_name() -> Arc<str> {
        "i32".into()
    }

    #[inline]
    fn new(param: impl Into<Param>) -> Self {
        param.into().as_i32()
    }
}

impl PlFactor for i32 {
    fn try_expr(&self) -> Result<Expr> {
        Ok((*self as i64).lit())
    }
}

impl FactorBase for i64 {
    #[inline]
    fn fac_name() -> Arc<str> {
        "i64".into()
    }

    #[inline]
    fn new(param: impl Into<Param>) -> Self {
        param.into().as_i64()
    }
}

impl PlFactor for i64 {
    fn try_expr(&self) -> Result<Expr> {
        Ok(self.lit())
    }
}

impl FactorBase for f32 {
    #[inline]
    fn fac_name() -> Arc<str> {
        "f32".into()
    }

    #[inline]
    fn new(param: impl Into<Param>) -> Self {
        param.into().as_f64() as f32
    }
}

impl PlFactor for f32 {
    fn try_expr(&self) -> Result<Expr> {
        Ok((*self as f64).lit())
    }
}

impl FactorBase for f64 {
    #[inline]
    fn fac_name() -> Arc<str> {
        "f64".into()
    }

    #[inline]
    fn new(param: impl Into<Param>) -> Self {
        param.into().as_f64()
    }
}

impl PlFactor for f64 {
    fn try_expr(&self) -> Result<Expr> {
        Ok(self.lit())
    }
}

impl FactorBase for String {
    #[inline]
    fn fac_name() -> Arc<str> {
        "string".into()
    }

    #[inline]
    fn new(param: impl Into<Param>) -> Self {
        param.into().as_str().to_string()
    }
}

impl PlFactor for String {
    fn try_expr(&self) -> Result<Expr> {
        Ok(col(self))
    }
}

impl FactorBase for &str {
    #[inline]
    fn fac_name() -> Arc<str> {
        "str".into()
    }

    #[inline]
    fn new(_param: impl Into<Param>) -> Self {
        panic!("&str can not be created with FactorBase::new")
    }
}

impl PlFactor for &'static str {
    fn try_expr(&self) -> Result<Expr> {
        Ok(col(*self))
    }
}

impl FactorBase for Arc<str> {
    #[inline]
    fn fac_name() -> Arc<str> {
        "Arc<str>".into()
    }

    #[inline]
    fn new(param: impl Into<Param>) -> Self {
        param.into().as_str().into()
    }
}

impl PlFactor for Arc<str> {
    fn try_expr(&self) -> Result<Expr> {
        Ok(col(self.as_ref()))
    }
}

impl FactorBase for &String {
    #[inline]
    fn fac_name() -> Arc<str> {
        "string".into()
    }

    #[inline]
    fn new(_param: impl Into<Param>) -> Self {
        panic!("&String can not be created with FactorBase::new")
    }
}

impl PlFactor for &'static String {
    fn try_expr(&self) -> Result<Expr> {
        Ok(col(*self))
    }
}
