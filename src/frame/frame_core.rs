use std::fmt::Debug;

use anyhow::Result;
use derive_more::{From, IsVariant};
use polars::prelude::*;

#[derive(From, Clone, IsVariant)]
pub enum Frame {
    Eager(DataFrame),
    Lazy(LazyFrame),
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
    #[inline]
    pub fn unwrap_eager(self) -> DataFrame {
        if let Frame::Eager(df) = self {
            return df;
        }
        panic!("not a eager dataframe")
    }

    #[inline]
    pub fn unwrap_lazy(self) -> LazyFrame {
        if let Frame::Lazy(df) = self {
            return df;
        }
        panic!("not a lazy dataframe")
    }

    #[inline]
    pub fn as_eager(&self) -> Option<&DataFrame> {
        if let Frame::Eager(df) = self {
            return Some(df);
        }
        None
    }

    #[inline]
    pub fn as_lazy(&self) -> Option<&LazyFrame> {
        if let Frame::Lazy(df) = self {
            return Some(df);
        }
        None
    }

    #[inline]
    pub fn schema(&mut self) -> Result<SchemaRef> {
        match self {
            Frame::Eager(df) => Ok(df.schema().into()),
            Frame::Lazy(df) => Ok(df.schema()?),
        }
    }

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

    #[inline]
    pub fn lazy(self) -> LazyFrame {
        match self {
            Frame::Eager(df) => df.lazy(),
            Frame::Lazy(df) => df,
        }
    }

    #[inline]
    pub fn collect(self) -> Result<DataFrame> {
        match self {
            Frame::Eager(df) => Ok(df),
            Frame::Lazy(df) => Ok(df.collect()?),
        }
    }

    #[inline]
    pub fn rename<I, J, T, S>(self, existing: I, new: J) -> Result<Self>
    where
        I: IntoIterator<Item = T>,
        J: IntoIterator<Item = S>,
        T: AsRef<str>,
        S: AsRef<str>,
    {
        match self {
            Frame::Eager(mut df) => {
                for (e, n) in existing.into_iter().zip(new.into_iter()) {
                    df.rename(e.as_ref(), n.as_ref())?;
                }
                Ok(df.into())
            },
            Frame::Lazy(df) => Ok(df.rename(existing, new).into()),
        }
    }

    #[inline]
    pub fn select<E: AsRef<[Expr]>>(self, exprs: E) -> Result<Self> {
        self.impl_by_lazy(|df| df.select(exprs))
    }

    #[inline]
    pub fn with_column(self, expr: Expr) -> Result<Self> {
        self.impl_by_lazy(|df| df.with_column(expr))
    }

    #[inline]
    pub fn with_columns<E: AsRef<[Expr]>>(self, exprs: E) -> Result<Self> {
        self.impl_by_lazy(|df| df.with_columns(exprs))
    }

    #[inline]
    pub fn filter(self, predicate: Expr) -> Result<Self> {
        self.impl_by_lazy(|df| df.filter(predicate))
    }

    #[inline]
    pub fn drop<I, T>(mut self, columns: I) -> Result<Self>
    where
        I: IntoIterator<Item = T>,
        T: AsRef<str>,
    {
        // ignore exists columns
        let schema = self.schema()?;
        let columns = columns.into_iter().filter(|c| schema.contains(c.as_ref()));
        self.impl_by_lazy(|df| df.drop(columns))
    }
}

pub trait IntoFrame {
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
