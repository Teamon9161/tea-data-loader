use std::fmt::Debug;

use anyhow::Result;
use derive_more::{From, IsVariant};
use polars::prelude::*;
#[cfg(feature = "serde")]
use serde::{Deserialize, Deserializer, Serialize, Serializer};

/// Represents a frame that can be either an eager DataFrame or a lazy LazyFrame.
///
/// This enum allows for flexibility in handling data processing, enabling both
/// immediate (eager) and deferred (lazy) evaluation strategies.
#[derive(From, Clone, IsVariant)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub enum Frame {
    /// An eager DataFrame, which holds data in memory and allows for immediate operations.
    Eager(DataFrame),
    /// A lazy LazyFrame, which represents a set of operations to be performed when executed.
    #[cfg_attr(
        feature = "serde",
        serde(
            serialize_with = "Frame::serialize_lf",
            deserialize_with = "Frame::deserialize_lf"
        )
    )]
    Lazy(LazyFrame),
}

impl Frame {
    fn serialize_lf<S>(lf: &LazyFrame, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let dsl_plan = lf.logical_plan.clone();
        dsl_plan.serialize(serializer)
    }

    fn deserialize_lf<'de, D>(deserializer: D) -> Result<LazyFrame, D::Error>
    where
        D: Deserializer<'de>,
    {
        let dsl_plan = DslPlan::deserialize(deserializer)?;
        Ok(dsl_plan.into())
    }
}

impl Debug for Frame {
    #[inline]
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Frame::Eager(df) => df.fmt(f),
            Frame::Lazy(_) => write!(f, "LazyFrame"),
        }
    }
}

impl Frame {
    /// Unwraps the Frame into a DataFrame, panicking if it's not an eager DataFrame.
    ///
    /// # Panics
    ///
    /// Panics if the Frame is not an eager DataFrame.
    #[inline]
    pub fn unwrap_eager(self) -> DataFrame {
        if let Frame::Eager(df) = self {
            return df;
        }
        panic!("not a eager dataframe")
    }

    /// Unwraps the Frame into a LazyFrame, panicking if it's not a lazy LazyFrame.
    ///
    /// # Panics
    ///
    /// Panics if the Frame is not a lazy LazyFrame.
    #[inline]
    pub fn unwrap_lazy(self) -> LazyFrame {
        if let Frame::Lazy(df) = self {
            return df;
        }
        panic!("not a lazy dataframe")
    }

    /// Returns a reference to the inner DataFrame if the Frame is eager, or None otherwise.
    #[inline]
    pub fn as_eager(&self) -> Option<&DataFrame> {
        if let Frame::Eager(df) = self {
            return Some(df);
        }
        None
    }

    /// Returns a reference to the inner LazyFrame if the Frame is lazy, or None otherwise.
    #[inline]
    pub fn as_lazy(&self) -> Option<&LazyFrame> {
        if let Frame::Lazy(df) = self {
            return Some(df);
        }
        None
    }

    /// Returns the schema of the Frame.
    ///
    /// # Errors
    ///
    /// Returns an error if there's an issue retrieving the schema for a lazy Frame.
    #[inline]
    pub fn schema(&mut self) -> Result<SchemaRef> {
        match self {
            Frame::Eager(df) => Ok(df.schema().clone().into()),
            Frame::Lazy(df) => Ok(df.collect_schema()?),
        }
    }

    /// Applies a function to the Frame's lazy representation and returns the result as a new Frame.
    ///
    /// # Errors
    ///
    /// Returns an error if there's an issue collecting the result for an eager Frame.
    #[inline]
    pub(super) fn impl_by_lazy<F>(self, f: F) -> Result<Frame>
    where
        F: FnOnce(LazyFrame) -> LazyFrame,
    {
        match self {
            Frame::Eager(df) => Ok(f(df.lazy()).collect()?.into()),
            Frame::Lazy(df) => Ok(f(df).into()),
        }
    }

    /// Converts the Frame to a LazyFrame.
    #[inline]
    pub fn lazy(self) -> LazyFrame {
        match self {
            Frame::Eager(df) => df.lazy(),
            Frame::Lazy(df) => df,
        }
    }

    /// Collects the Frame into a DataFrame.
    ///
    /// # Errors
    ///
    /// Returns an error if there's an issue collecting a lazy Frame.
    #[inline]
    pub fn collect(self) -> Result<DataFrame> {
        match self {
            Frame::Eager(df) => Ok(df),
            Frame::Lazy(df) => Ok(df.collect()?),
        }
    }

    /// Renames columns in the Frame.
    ///
    /// `existing` and `new` are iterables of the same length containing the old and
    /// corresponding new column names. Renaming happens to all `existing` columns
    /// simultaneously, not iteratively. If `strict` is true, all columns in `existing`
    /// must be present in the `LazyFrame` when `rename` is called; otherwise, only
    /// those columns that are actually found will be renamed (others will be ignored).
    ///
    /// # Errors
    ///
    /// Returns an error if there's an issue renaming columns in an eager Frame.
    #[inline]
    pub fn rename<I, J, T, S>(self, existing: I, new: J, strict: bool) -> Result<Self>
    where
        I: IntoIterator<Item = T>,
        J: IntoIterator<Item = S>,
        T: AsRef<str>,
        S: AsRef<str>,
    {
        match self {
            Frame::Eager(mut df) => {
                for (e, n) in existing.into_iter().zip(new.into_iter()) {
                    df.rename(e.as_ref(), n.as_ref().into())?;
                }
                Ok(df.into())
            },
            Frame::Lazy(df) => Ok(df.rename(existing, new, strict).into()),
        }
    }

    /// Selects columns or expressions in the Frame.
    ///
    /// # Errors
    ///
    /// Returns an error if there's an issue applying the selection.
    #[inline]
    pub fn select<E: AsRef<[Expr]>>(self, exprs: E) -> Result<Self> {
        self.impl_by_lazy(|df| df.select(exprs))
    }

    /// Adds a new column to the Frame.
    ///
    /// # Errors
    ///
    /// Returns an error if there's an issue adding the column.
    #[inline]
    pub fn with_column(self, expr: Expr) -> Result<Self> {
        self.impl_by_lazy(|df| df.with_column(expr))
    }

    /// Adds multiple new columns to the Frame.
    ///
    /// # Errors
    ///
    /// Returns an error if there's an issue adding the columns.
    #[inline]
    pub fn with_columns<E: AsRef<[Expr]>>(self, exprs: E) -> Result<Self> {
        self.impl_by_lazy(|df| df.with_columns(exprs))
    }

    /// Filters the Frame based on a predicate.
    ///
    /// # Errors
    ///
    /// Returns an error if there's an issue applying the filter.
    #[inline]
    pub fn filter(self, predicate: Expr) -> Result<Self> {
        self.impl_by_lazy(|df| df.filter(predicate))
    }

    #[inline]
    pub fn sort(
        self,
        by: impl IntoVec<PlSmallStr>,
        sort_options: SortMultipleOptions,
    ) -> Result<Self> {
        self.impl_by_lazy(|df| df.sort(by, sort_options))
    }

    /// Removes columns from the Frame.
    /// Note that it's better to only select the columns you need
    /// and let the projection pushdown optimize away the unneeded columns.
    ///
    /// # Errors
    ///
    /// Returns a error if any of the specified columns
    /// do not exist in the schema when materializing the Frame.
    #[inline]
    pub fn drop_strict<I, T>(self, columns: I) -> Result<Self>
    where
        I: IntoIterator<Item = T>,
        T: Into<Selector>,
    {
        self.impl_by_lazy(|df| df.drop(columns))
    }

    /// Removes columns from the Frame.
    /// Note that it's better to only select the columns you need
    /// and let the projection pushdown optimize away the unneeded columns.
    ///
    /// # Notes
    ///
    /// If a column name does not exist in the schema, it will be silently ignored.
    #[inline]
    pub fn drop<I, T>(self, columns: I) -> Result<Self>
    where
        I: IntoIterator<Item = T>,
        T: Into<Selector>,
    {
        self.impl_by_lazy(|df| df.drop_no_validate(columns))
    }
}

/// A trait for types that can be converted into a `Frame`.
///
/// This trait provides a method to convert compatible types into a `Frame`,
/// which can represent either an eager `DataFrame` or a lazy `LazyFrame`.
pub trait IntoFrame {
    /// Converts the implementing type into a `Frame`.
    ///
    /// # Returns
    ///
    /// A `Frame` containing the data from the original type.
    fn into_frame(self) -> Frame;
}

impl IntoFrame for DataFrame {
    #[inline]
    fn into_frame(self) -> Frame {
        Frame::Eager(self)
    }
}

impl IntoFrame for LazyFrame {
    #[inline]
    fn into_frame(self) -> Frame {
        Frame::Lazy(self)
    }
}
