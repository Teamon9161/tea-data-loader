pub(super) mod export;
pub mod map;
mod param;
mod parse;
mod register;

use anyhow::Result;
pub use param::Param;
pub use parse::parse_pl_fac;
use polars::prelude::*;
pub use register::*;

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
