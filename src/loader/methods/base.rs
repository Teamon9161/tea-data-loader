use polars::prelude::*;

use crate::prelude::*;

impl DataLoader {
    #[inline]
    pub fn try_apply<F>(mut self, mut f: F) -> Result<Self>
    where
        F: FnMut(Frame) -> Result<Frame>,
    {
        let mut dfs = Vec::with_capacity(self.len());
        for df in self.dfs {
            dfs.push(f(df)?);
        }
        self.dfs = dfs.into();
        Ok(self)
    }

    #[inline]
    pub fn par_apply<F, DF: Into<Frame>>(mut self, f: F) -> Self
    where
        F: Fn(Frame) -> DF + Send + Sync,
    {
        self.dfs = self.dfs.par_apply(f);
        self
    }

    #[inline]
    pub fn par_apply_with_symbol<F, DF: Into<Frame>>(self, f: F) -> Self
    where
        F: Fn((&str, Frame)) -> DF + Send + Sync,
    {
        use rayon::prelude::*;
        let mut out = self.empty_copy();
        out.dfs = crate::POOL.install(|| {
            self.into_par_iter()
                .map(|(symbol, df)| f((&*symbol, df)).into())
                .collect::<Vec<_>>()
                .into()
        });
        out
    }

    #[inline]
    pub fn with_column(self, expr: Expr) -> Result<Self> {
        self.try_apply(|df| df.with_column(expr.clone()))
    }

    #[inline]
    pub fn with_columns<E: AsRef<[Expr]>>(self, exprs: E) -> Result<Self> {
        let exprs = exprs.as_ref();
        self.try_apply(|df| df.with_columns(exprs))
    }

    #[inline]
    pub fn select<E: AsRef<[Expr]>>(self, exprs: E) -> Result<Self> {
        let exprs = exprs.as_ref();
        self.try_apply(|df| df.select(exprs))
    }

    #[inline]
    pub fn filter(self, expr: Expr) -> Result<Self> {
        self.try_apply(|df| df.filter(expr.clone()))
    }
}
