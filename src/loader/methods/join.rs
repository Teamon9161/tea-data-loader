use std::path::Path;

use polars::prelude::*;

use crate::prelude::*;
/// Options for joining data in a DataLoader.
///
/// This struct provides configuration options for joining data from an external source
/// with the data in a DataLoader.
///
/// # Type Parameters
///
/// * `P`: A type that can be converted to a Path, typically used for specifying the file path.
/// * `E`: A type that can be converted to a slice of Expressions, used for specifying join columns.
pub struct JoinOpt<P: AsRef<Path>, E: AsRef<[Expr]>> {
    /// The path to the file containing the data to join.
    path: P,
    /// The column(s) to join on from the left (existing) DataFrame.
    left_on: Option<E>,
    /// The column(s) to join on from the right (new) DataFrame.
    right_on: Option<E>,
    /// The type of join to perform (e.g., inner, left, right, outer).
    how: JoinType,
    /// Whether to coalesce columns with the same name after joining.
    coalesce: Option<bool>,
    /// A flag to determine whether the join operation should be performed.
    flag: bool,
    /// An optional suffix to append to the file name when reading the data.
    suffix: Option<&'static str>,
}

impl<P: AsRef<Path>, E: AsRef<[Expr]>> JoinOpt<P, E> {
    #[inline]
    pub fn new_on(path: P, on: &E, how: JoinType, flag: bool) -> JoinOpt<P, &[Expr]>
    where
        E: Clone,
    {
        JoinOpt {
            path,
            left_on: Some(on.as_ref()),
            right_on: Some(on.as_ref()),
            how,
            coalesce: None,
            flag,
            suffix: None,
        }
    }

    #[inline]
    pub fn new(path: P, left_on: E, right_on: E, how: JoinType, flag: bool) -> Self {
        JoinOpt {
            path,
            left_on: Some(left_on),
            right_on: Some(right_on),
            how,
            coalesce: None,
            flag,
            suffix: None,
        }
    }
}

impl DataLoader {
    /// Joins the current DataLoader with another dataset based on the provided options.
    ///
    /// This method performs a join operation between the current DataLoader and another dataset
    /// specified by the `JoinOpt` parameter. It supports various join types and options.
    ///
    /// # Arguments
    ///
    /// * `option` - A `JoinOpt` struct containing join options including path, join columns, join type, etc.
    ///
    /// # Returns
    ///
    /// A `Result` containing the joined `DataLoader` if successful, or an error if the join operation fails.
    pub fn join(self, option: JoinOpt<impl AsRef<Path>, impl AsRef<[Expr]>>) -> Result<Self> {
        if !option.flag {
            return Ok(self);
        }
        let suffix = option.suffix.unwrap_or(".feather");
        let mut out = self.empty_copy();
        let default_on = [col("time")];
        let coalesce = if let Some(coalesce) = option.coalesce {
            if coalesce {
                JoinCoalesce::CoalesceColumns
            } else {
                JoinCoalesce::KeepColumns
            }
        } else {
            JoinCoalesce::JoinSpecific
        };
        for (symbol, df) in self.into_iter() {
            let other_path = option.path.as_ref().join(symbol.to_string() + suffix);
            let other = LazyFrame::scan_ipc(&other_path, Default::default())?;
            let df = df.join(
                other.into(),
                option
                    .left_on
                    .as_ref()
                    .map(|e| e.as_ref())
                    .unwrap_or_else(|| default_on.as_ref()),
                option
                    .right_on
                    .as_ref()
                    .map(|e| e.as_ref())
                    .unwrap_or_else(|| default_on.as_ref()),
                JoinArgs::new(option.how.clone()).with_coalesce(coalesce),
            )?;
            out.dfs.push(df);
        }
        Ok(out)
    }

    /// Performs a left join between the current DataLoader and another dataset.
    ///
    /// This is a convenience method that calls `join` with `JoinType::Left`.
    ///
    /// # Arguments
    ///
    /// * `path` - The path to the other dataset.
    /// * `left_on` - The column(s) to join on from the left (current) DataFrame.
    /// * `right_on` - The column(s) to join on from the right (other) DataFrame.
    /// * `flag` - A boolean flag to determine whether the join operation should be performed.
    ///
    /// # Returns
    ///
    /// A `Result` containing the joined `DataLoader` if successful, or an error if the join operation fails.
    #[inline]
    pub fn left_join<E: AsRef<[Expr]>>(
        self,
        path: impl AsRef<Path>,
        left_on: E,
        right_on: E,
        flag: bool,
    ) -> Result<Self> {
        self.join(JoinOpt::new(path, left_on, right_on, JoinType::Left, flag))
    }
}
