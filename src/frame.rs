use std::fmt::Debug;
use std::ops::{Deref, DerefMut};

use anyhow::Result;
use derive_more::From;
use polars::prelude::*;
use tevec::prelude::{terr, CollectTrustedToVec, TryCollectTrustedToVec};

#[derive(From, Clone)]
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
    pub fn schema(&mut self) -> Result<SchemaRef> {
        match self {
            Frame::Eager(df) => Ok(df.schema().into()),
            Frame::Lazy(df) => Ok(df.schema()?),
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
    pub fn select<E: AsRef<[Expr]>>(self, exprs: E) -> Self {
        match self {
            Frame::Eager(df) => df.lazy().select(exprs).into(),
            Frame::Lazy(df) => df.select(exprs).into(),
        }
    }

    #[inline]
    pub fn with_column(self, expr: Expr) -> Self {
        match self {
            Frame::Eager(df) => df.lazy().with_column(expr).into(),
            Frame::Lazy(df) => df.with_column(expr).into(),
        }
    }

    #[inline]
    pub fn with_columns<E: AsRef<[Expr]>>(self, exprs: E) -> Self {
        match self {
            Frame::Eager(df) => df.lazy().with_columns(exprs).into(),
            Frame::Lazy(df) => df.with_columns(exprs).into(),
        }
    }

    #[inline]
    pub fn filter(self, predicate: Expr) -> Self {
        match self {
            Frame::Eager(df) => df.lazy().filter(predicate).into(),
            Frame::Lazy(df) => df.filter(predicate).into(),
        }
    }
}

#[derive(Debug, From, Default, Clone)]
pub struct Frames(pub Vec<Frame>);

impl Deref for Frames {
    type Target = [Frame];

    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for Frames {
    #[inline]
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl From<Vec<LazyFrame>> for Frames {
    #[inline]
    fn from(lazy_frames: Vec<LazyFrame>) -> Self {
        lazy_frames
            .into_iter()
            .map(Into::<Frame>::into)
            .collect_trusted_to_vec()
            .into()
    }
}

impl From<Vec<DataFrame>> for Frames {
    #[inline]
    fn from(eager_frames: Vec<DataFrame>) -> Self {
        eager_frames
            .into_iter()
            .map(Into::<Frame>::into)
            .collect_trusted_to_vec()
            .into()
    }
}

impl<F: Into<Frame>> FromIterator<F> for Frames {
    #[inline]
    fn from_iter<T: IntoIterator<Item = F>>(iter: T) -> Self {
        iter.into_iter().map(Into::into).collect::<Vec<_>>().into()
    }
}

impl Frames {
    #[inline]
    pub fn lazy(self) -> Self {
        self.apply(Frame::lazy)
    }

    #[inline]
    pub fn collect(self, par: bool) -> Result<Self> {
        if !par {
            self.try_apply(Frame::collect)
        } else {
            Ok(self.par_apply(|df| df.collect().unwrap()))
        }
    }

    #[inline]
    pub fn push(&mut self, frame: Frame) {
        self.0.push(frame);
    }

    #[inline]
    pub fn apply<F, DF: Into<Frame>>(self, mut f: F) -> Self
    where
        F: FnMut(Frame) -> DF,
    {
        self.0
            .into_iter()
            .map(|df| f(df).into())
            .collect_trusted_to_vec()
            .into()
    }

    #[inline]
    pub fn try_apply<F, DF: Into<Frame>>(self, mut f: F) -> Result<Self>
    where
        F: FnMut(Frame) -> Result<DF>,
    {
        Ok(self
            .0
            .into_iter()
            .map(|df| f(df).map(Into::into).map_err(|e| terr!("{:?}", e)))
            .try_collect_trusted_to_vec()?
            .into())
    }

    #[inline]
    pub fn par_apply<F, DF: Into<Frame>>(self, f: F) -> Self
    where
        F: Fn(Frame) -> DF + Send + Sync,
    {
        use rayon::prelude::*;
        self.0
            .into_par_iter()
            .map(|df| f(df).into())
            .collect::<Vec<_>>()
            .into()
    }
}
