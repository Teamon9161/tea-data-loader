use std::fmt::Debug;
use std::ops::{Deref, DerefMut};

use anyhow::Result;
use derive_more::From;
use polars::prelude::*;
use tea_strategy::tevec::prelude::{terr, CollectTrustedToVec, TryCollectTrustedToVec};

use super::frame_core::Frame;

// TODO: parallelize serialization & deserialization
#[derive(Debug, From, Default, Clone)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
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
        crate::POOL.install(|| {
            self.0
                .into_par_iter()
                .map(|df| f(df).into())
                .collect::<Vec<_>>()
                .into()
        })
    }
}
