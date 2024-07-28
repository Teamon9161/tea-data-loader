use anyhow::Result;
use polars::prelude::*;

pub trait Strategy {
    fn eval_to_fac(&self, fac: &Series, filters: Option<DataFrame>) -> Result<Series>;

    #[inline]
    fn eval(&self, fac: &str, df: DataFrame, filters: Option<[Expr; 4]>) -> Result<Series> {
        let fac = df.column(fac)?.clone();
        if let Some(filters) = filters {
            let filters = df.lazy().select(filters).collect()?;
            self.eval_to_fac(&fac, Some(filters))
        } else {
            self.eval_to_fac(&fac, None)
        }
    }
}
