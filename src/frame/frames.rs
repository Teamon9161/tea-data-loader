use std::fmt::Debug;
use std::ops::{Deref, DerefMut};

use anyhow::Result;
use derive_more::From;
use polars::prelude::*;
use tea_strategy::tevec::prelude::{terr, CollectTrustedToVec, TryCollectTrustedToVec};

use super::frame_core::Frame;

/// A collection of `Frame` objects.
///
/// This struct represents a collection of `Frame` objects, which can be either
/// eager `DataFrame`s or lazy `LazyFrame`s. It provides a convenient way to
/// handle multiple frames together.
///
/// # Features
///
/// - Implements `Deref` and `DerefMut` to `[Frame]`, allowing easy access to the underlying `Vec<Frame>`.
/// - Can be created from `Vec<LazyFrame>` or `Vec<DataFrame>`.
/// - Implements `FromIterator` for any type that can be converted into a `Frame`.
///
/// # Serialization
///
/// When the "serde" feature is enabled, this struct can be serialized and deserialized.
///
/// # TODO
///
/// - Parallelize serialization & deserialization for improved performance.
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
    /// Converts all frames to lazy frames.
    ///
    /// This method applies the `lazy` transformation to each frame in the collection.
    #[inline]
    pub fn lazy(self) -> Self {
        self.apply(Frame::lazy)
    }

    /// Collects all frames, optionally in parallel.
    ///
    /// # Arguments
    ///
    /// * `par` - If true, collection is performed in parallel.
    ///
    /// # Returns
    ///
    /// A `Result` containing the collected frames.
    #[inline]
    pub fn collect(self, par: bool) -> Result<Self> {
        if !par {
            self.try_apply(Frame::collect)
        } else {
            Ok(self.par_apply(|df| df.collect().unwrap()))
        }
    }

    /// Adds a new frame to the collection.
    ///
    /// # Arguments
    ///
    /// * `frame` - The frame to be added.
    #[inline]
    pub fn push(&mut self, frame: Frame) {
        self.0.push(frame);
    }

    /// Applies a function to each frame in the collection.
    ///
    /// # Arguments
    ///
    /// * `f` - The function to apply to each frame.
    ///
    /// # Returns
    ///
    /// A new `Frames` collection with the function applied to each frame.
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

    /// Attempts to apply a fallible function to each frame in the collection.
    ///
    /// # Arguments
    ///
    /// * `f` - The fallible function to apply to each frame.
    ///
    /// # Returns
    ///
    /// A `Result` containing a new `Frames` collection with the function applied to each frame,
    /// or an error if any application fails.
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

    /// Applies a function to each frame in parallel.
    ///
    /// # Arguments
    ///
    /// * `f` - The function to apply to each frame. Must be `Send` and `Sync`.
    ///
    /// # Returns
    ///
    /// A new `Frames` collection with the function applied to each frame in parallel.
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
