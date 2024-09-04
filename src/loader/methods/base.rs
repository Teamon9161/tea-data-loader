use polars::prelude::*;

use crate::prelude::*;

impl DataLoader {
    /// Applies a function to each DataFrame in the DataLoader, potentially modifying them.
    ///
    /// # Arguments
    ///
    /// * `f` - A mutable closure that takes a `Frame` and returns a `Result<Frame>`.
    ///
    /// # Returns
    ///
    /// A `Result` containing the modified `DataLoader` or an error.
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

    /// Applies a function to each DataFrame in the DataLoader in parallel.
    ///
    /// # Arguments
    ///
    /// * `f` - A closure that takes a `Frame` and returns something that can be converted into a `Frame`.
    ///
    /// # Returns
    ///
    /// The modified `DataLoader`.
    #[inline]
    pub fn par_apply<F, DF: Into<Frame>>(mut self, f: F) -> Self
    where
        F: Fn(Frame) -> DF + Send + Sync,
    {
        self.dfs = self.dfs.par_apply(f);
        self
    }

    /// Applies a function to each DataFrame in the DataLoader in parallel, providing the symbol along with the DataFrame.
    ///
    /// # Arguments
    ///
    /// * `f` - A closure that takes a tuple of `(&str, Frame)` and returns something that can be converted into a `Frame`.
    ///
    /// # Returns
    ///
    /// A new `DataLoader` with the modified DataFrames.
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

    /// Adds a new column to each DataFrame in the DataLoader.
    ///
    /// # Arguments
    ///
    /// * `expr` - An `Expr` representing the new column to add.
    ///
    /// # Returns
    ///
    /// A `Result` containing the modified `DataLoader` or an error.
    #[inline]
    pub fn with_column(self, expr: Expr) -> Result<Self> {
        self.try_apply(|df| df.with_column(expr.clone()))
    }

    /// Adds multiple new columns to each DataFrame in the DataLoader.
    ///
    /// # Arguments
    ///
    /// * `exprs` - A slice of `Expr` representing the new columns to add.
    ///
    /// # Returns
    ///
    /// A `Result` containing the modified `DataLoader` or an error.
    #[inline]
    pub fn with_columns<E: AsRef<[Expr]>>(self, exprs: E) -> Result<Self> {
        let exprs = exprs.as_ref();
        self.try_apply(|df| df.with_columns(exprs))
    }

    /// Selects specific columns from each DataFrame in the DataLoader.
    ///
    /// # Arguments
    ///
    /// * `exprs` - A slice of `Expr` representing the columns to select.
    ///
    /// # Returns
    ///
    /// A `Result` containing the modified `DataLoader` or an error.
    #[inline]
    pub fn select<E: AsRef<[Expr]>>(self, exprs: E) -> Result<Self> {
        let exprs = exprs.as_ref();
        self.try_apply(|df| df.select(exprs))
    }

    /// Filters rows in each DataFrame of the DataLoader based on a given expression.
    ///
    /// # Arguments
    ///
    /// * `expr` - An `Expr` representing the filter condition.
    ///
    /// # Returns
    ///
    /// A `Result` containing the modified `DataLoader` or an error.
    #[inline]
    pub fn filter(self, expr: Expr) -> Result<Self> {
        self.try_apply(|df| df.filter(expr.clone()))
    }
}
