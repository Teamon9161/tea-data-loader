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

    /// Sorts the DataFrame by the specified columns.
    ///
    /// # Arguments
    ///
    /// * `by` - A vector of column names to sort by.
    /// * `sort_options` - Options for sorting, including order and null handling.
    ///
    /// # Returns
    ///
    /// A `Result` containing the modified `DataLoader` or an error.
    #[inline]
    pub fn sort(
        self,
        by: impl IntoVec<PlSmallStr>,
        sort_options: SortMultipleOptions,
    ) -> Result<Self> {
        let by = by.into_vec();
        self.try_apply(|df| df.sort(by.clone(), sort_options.clone()))
    }

    /// Removes columns from each DataFrame in the DataLoader.
    /// Note that it's better to only select the columns you need
    /// and let the projection pushdown optimize away the unneeded columns.
    ///
    /// # Arguments
    ///
    /// * `columns` - An iterator of column names or selectors to remove
    ///
    /// # Notes
    ///
    /// If a column name does not exist in the schema, it will be silently ignored.
    #[inline]
    pub fn drop<I, T>(self, columns: I) -> Result<Self>
    where
        I: IntoIterator<Item = T> + Clone,
        T: Into<Selector>,
    {
        self.try_apply(|df| df.drop(columns.clone()))
    }

    /// Removes columns from each DataFrame in the DataLoader.
    /// Note that it's better to only select the columns you need
    /// and let the projection pushdown optimize away the unneeded columns.
    ///
    /// # Arguments
    ///
    /// * `columns` - An iterator of column names or selectors to remove
    ///
    /// # Errors
    ///
    /// Returns an error if any of the specified columns
    /// do not exist in the schema when materializing the DataFrames.
    #[inline]
    pub fn drop_strict<I, T>(self, columns: I) -> Result<Self>
    where
        I: IntoIterator<Item = T> + Clone,
        T: Into<Selector>,
    {
        self.try_apply(|df| df.drop_strict(columns.clone()))
    }

    /// Aligns multiple DataFrames based on specified columns and join type.
    ///
    /// This method aligns the DataFrames in the `DataLoader` by performing a series of joins
    /// on the specified columns. It creates a master alignment frame and then extracts
    /// individual aligned frames from it.
    ///
    /// # Arguments
    ///
    /// * `on` - An expression or slice of expressions specifying the columns to align on.
    /// * `how` - An optional `JoinType` specifying the type of join to perform. Defaults to `JoinType::Full` if not provided.
    ///
    /// # Returns
    ///
    /// Returns a `Result` containing the modified `DataLoader` with aligned frames, or an error if the alignment process fails.
    ///
    /// # Notes
    ///
    /// - If the `DataLoader` is empty, it returns the original instance.
    /// - For large numbers of frames (more than `POST_ALIGN_COLLECT_NUM`), it may need to collect eagerly to avoid stack overflow.
    /// - The method sorts the resulting frames based on the alignment columns.
    #[inline]
    pub fn align(mut self, on: impl AsRef<[Expr]>, how: Option<JoinType>) -> Result<Self> {
        self.dfs = self.dfs.align(on, how)?;
        Ok(self)
    }

    /// Finds the index of a given symbol in the DataLoader's symbols list.
    ///
    /// # Arguments
    ///
    /// * `symbol` - The symbol name to search for
    ///
    /// # Returns
    ///
    /// Returns `Some(index)` if the symbol is found, where `index` is the position
    /// of the symbol in the list. Returns `None` if either:
    /// - The symbol is not found
    /// - The DataLoader has no symbols list
    #[inline]
    pub fn find_index(&self, symbol: &str) -> Option<usize> {
        if let Some(symbols) = &self.symbols {
            symbols.iter().position(|s| &**s == symbol)
        } else {
            None
        }
    }

    /// Inserts a new DataFrame into the DataLoader with the given symbol name.
    ///
    /// # Arguments
    ///
    /// * `symbol` - The name/identifier for the DataFrame
    /// * `frame` - The DataFrame to insert
    ///
    /// # Returns
    ///
    /// A `Result` containing a mutable reference to the modified `DataLoader` or an error.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Attempting to insert a new symbol when the DataLoader has data but no symbol names
    pub fn insert(&mut self, symbol: &str, frame: impl Into<Frame>) -> Result<&mut Self> {
        if let Some(symbols) = &mut self.symbols {
            let arc_symbol = symbol.into();
            if symbols.contains(&arc_symbol) {
                self[symbol] = frame.into()
            } else {
                symbols.push(arc_symbol);
                self.dfs.push(frame.into());
            }
        } else if self.is_empty() {
            self.symbols = Some(vec![symbol.into()]);
            self.dfs.push(frame.into());
        } else {
            bail!(
                "DataLoader should have symbol names when insert a new symbol, cannot insert new symbol: {}, ",
                symbol
            );
        }
        Ok(self)
    }
}
