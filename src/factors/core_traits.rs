use anyhow::Result;
use polars::lazy::dsl::Expr;
use polars::prelude::*;

use super::param::Param;

pub trait FactorBase: Sized {
    fn fac_name() -> Arc<str>;

    fn new<P: Into<Param>>(param: P) -> Self;
}

pub trait GetName {
    fn name(&self) -> String;
}

pub trait PlFactor: GetName + Send + Sync + 'static {
    fn try_expr(&self) -> Result<Expr>;

    #[inline]
    fn expr(&self) -> Expr {
        self.try_expr().unwrap()
    }
}

pub trait TFactor: GetName + Send + Sync + 'static {
    fn eval(&self, df: &DataFrame) -> Result<Series>;
}
