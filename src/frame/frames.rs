use std::fmt::Debug;
use std::ops::{Deref, DerefMut};

use anyhow::Result;
use derive_more::From;
use polars::prelude::*;
use rayon::prelude::*;
use tea_strategy::tevec::prelude::{terr, CollectTrustedToVec, TryCollectTrustedToVec};

use super::frame_core::Frame;
use crate::enums::AggMethod;

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

impl<F: Into<Frame> + Send> FromParallelIterator<F> for Frames {
    #[inline]
    fn from_par_iter<T: IntoParallelIterator<Item = F>>(iter: T) -> Self {
        iter.into_par_iter()
            .map(Into::into)
            .collect::<Vec<_>>()
            .into()
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

    /// Retrieves a column from each frame in the collection.
    ///
    /// # Arguments
    ///
    /// * `key` - The name of the column to retrieve.
    ///
    /// # Returns
    ///
    /// An iterator over references to the Series of the specified column from each frame.
    pub fn get_column<'a, S: AsRef<str> + 'a>(
        &'a self,
        key: S,
    ) -> impl Iterator<Item = &'a Column> + 'a {
        self.iter()
            .map(move |df| df.as_eager().unwrap().column(key.as_ref()).unwrap())
    }

    /// Performs horizontal aggregation on the frames collection.
    ///
    /// # Arguments
    ///
    /// * `keys` - An iterator of column names to aggregate.
    /// * `methods` - An iterator of aggregation methods corresponding to each key.
    ///
    /// # Returns
    ///
    /// A `Result` containing a `DataFrame` with the aggregated results, or an error if the aggregation fails.
    pub fn horizontal_agg<S: AsRef<str>>(
        self,
        keys: impl IntoIterator<Item = S>,
        methods: impl IntoIterator<Item = AggMethod>,
    ) -> Result<DataFrame> {
        use polars::lazy::dsl::*;

        use crate::utils::{column_into_expr, column_to_expr};
        let dfs = self.collect(true)?;
        let exprs = keys
            .into_iter()
            .zip(methods)
            .map(|(key, method)| {
                let expr = match method {
                    AggMethod::Mean => mean_horizontal(
                        dfs.get_column(key).map(column_to_expr).collect::<Vec<_>>(), true
                    )?,
                    AggMethod::WeightMean(weight) => {
                        let weight_sum = sum_horizontal(
                            dfs.get_column(&weight)
                                .map(column_to_expr)
                                .collect::<Vec<_>>(),
                                true
                        )?;
                        let all_sum = sum_horizontal(
                            dfs.iter()
                                .map(|df| {
                                    let df = df.as_eager().unwrap();
                                    let res = (df.column(key.as_ref()).unwrap()
                                        * df.column(weight.as_ref()).unwrap())
                                    .unwrap();
                                    column_into_expr(res)
                                })
                                .collect::<Vec<_>>(),
                                true
                        )?;
                        (all_sum / weight_sum).alias(key.as_ref())
                    },
                    AggMethod::Max => {
                        max_horizontal(dfs.get_column(key).map(column_to_expr).collect::<Vec<_>>())?
                    },
                    AggMethod::Min => {
                        min_horizontal(dfs.get_column(key).map(column_to_expr).collect::<Vec<_>>())?
                    },
                    AggMethod::Sum => {
                        sum_horizontal(dfs.get_column(key).map(column_to_expr).collect::<Vec<_>>(), true)?
                    },
                    AggMethod::First => {
                        let res = dfs[0].as_eager().unwrap().column(key.as_ref()).unwrap();
                        column_to_expr(res)
                    },
                    AggMethod::Last => {
                        let res = dfs[dfs.len() - 1]
                            .as_eager()
                            .unwrap()
                            .column(key.as_ref())
                            .unwrap();
                        column_to_expr(res)
                    },
                    AggMethod::ValidFirst => {
                        todo!()
                    },
                };
                Ok(expr)
            })
            .collect::<PolarsResult<Vec<_>>>()?;
        Ok(DataFrame::default().lazy().select(exprs).collect()?)
    }
}

#[cfg(test)]
mod tests {
    use polars::prelude::*;

    use crate::prelude::*;

    fn assert_series_equal(left: &Series, right: &Series) -> Result<()> {
        assert!(left.equal_missing(right).unwrap().all());
        Ok(())
    }

    #[test]
    fn test_horizontal_agg() -> Result<()> {
        // Create sample data
        let df1 = df! {
            "A" => [1, 2, 3],
            "B" => [4, 5, 6],
            "weight" => [0.5, 0.3, 0.2]
        }?;
        let df2 = df! {
            "A" => [10, 20, 30],
            "B" => [40, 50, 60],
            "weight" => [0.4, 0.4, 0.2]
        }?;

        let frames = Frames(vec![Frame::from(df1.lazy()), Frame::from(df2.lazy())]);

        // Test Mean
        let result_mean = frames
            .clone()
            .horizontal_agg(&["A", "B"], [AggMethod::Mean, AggMethod::Mean])?;
        let expected_mean_a = Series::new("A".into(), &[5.5, 11.0, 16.5]);
        let expected_mean_b = Series::new("B".into(), &[22.0, 27.5, 33.0]);
        assert_series_equal(
            result_mean.column("A")?.as_series().unwrap(),
            &expected_mean_a,
        )?;
        assert_series_equal(
            result_mean.column("B")?.as_series().unwrap(),
            &expected_mean_b,
        )?;

        // Test WeightMean
        let result_weight_mean = frames.clone().horizontal_agg(
            &["A", "B"],
            [
                AggMethod::WeightMean("weight".into()),
                AggMethod::WeightMean("weight".into()),
            ],
        )?;
        let expected_weight_mean_a = Series::new(
            "A".into(),
            &[
                (1.0 * 0.5 + 10.0 * 0.4) / 0.9,
                (2.0 * 0.3 + 20.0 * 0.4) / 0.7,
                (3.0 * 0.2 + 30.0 * 0.2) / 0.4,
            ],
        );
        let expected_weight_mean_b = Series::new(
            "B".into(),
            &[
                (4.0 * 0.5 + 40.0 * 0.4) / 0.9,
                (5.0 * 0.3 + 50.0 * 0.4) / 0.7,
                (6.0 * 0.2 + 60.0 * 0.2) / 0.4,
            ],
        );
        assert_series_equal(
            result_weight_mean.column("A")?.as_series().unwrap(),
            &expected_weight_mean_a,
        )?;
        assert_series_equal(
            result_weight_mean.column("B")?.as_series().unwrap(),
            &expected_weight_mean_b,
        )?;

        // Test Max
        let result_max = frames
            .clone()
            .horizontal_agg(&["A", "B"], [AggMethod::Max, AggMethod::Max])?;
        let expected_max_a = Series::new("A".into(), &[10.0, 20.0, 30.0]);
        let expected_max_b = Series::new("B".into(), &[40.0, 50.0, 60.0]);
        assert_series_equal(
            result_max.column("A")?.as_series().unwrap(),
            &expected_max_a,
        )?;
        assert_series_equal(
            result_max.column("B")?.as_series().unwrap(),
            &expected_max_b,
        )?;

        // Test Min
        let result_min = frames
            .clone()
            .horizontal_agg(&["A", "B"], [AggMethod::Min, AggMethod::Min])?;
        let expected_min_a = Series::new("A".into(), &[1.0, 2.0, 3.0]);
        let expected_min_b = Series::new("B".into(), &[4.0, 5.0, 6.0]);
        assert_series_equal(
            result_min.column("A")?.as_series().unwrap(),
            &expected_min_a,
        )?;
        assert_series_equal(
            result_min.column("B")?.as_series().unwrap(),
            &expected_min_b,
        )?;

        // Test Sum
        let result_sum = frames
            .clone()
            .horizontal_agg(&["A", "B"], [AggMethod::Sum, AggMethod::Sum])?;
        let expected_sum_a = Series::new("A".into(), &[11.0, 22.0, 33.0]);
        let expected_sum_b = Series::new("B".into(), &[44.0, 55.0, 66.0]);
        assert_series_equal(
            result_sum.column("A")?.as_series().unwrap(),
            &expected_sum_a,
        )?;
        assert_series_equal(
            result_sum.column("B")?.as_series().unwrap(),
            &expected_sum_b,
        )?;

        // Test First
        let result_first = frames
            .clone()
            .horizontal_agg(&["A", "B"], [AggMethod::First, AggMethod::First])?;
        let expected_first_a = Series::new("A".into(), &[1.0, 2.0, 3.0]);
        let expected_first_b = Series::new("B".into(), &[4.0, 5.0, 6.0]);
        assert_series_equal(
            result_first.column("A")?.as_series().unwrap(),
            &expected_first_a,
        )?;
        assert_series_equal(
            result_first.column("B")?.as_series().unwrap(),
            &expected_first_b,
        )?;

        // Test Last
        let result_last = frames
            .clone()
            .horizontal_agg(&["A", "B"], [AggMethod::Last, AggMethod::Last])?;
        let expected_last_a = Series::new("A".into(), &[10.0, 20.0, 30.0]);
        let expected_last_b = Series::new("B".into(), &[40.0, 50.0, 60.0]);
        assert_series_equal(
            result_last.column("A")?.as_series().unwrap(),
            &expected_last_a,
        )?;
        assert_series_equal(
            result_last.column("B")?.as_series().unwrap(),
            &expected_last_b,
        )?;

        Ok(())
    }
}
