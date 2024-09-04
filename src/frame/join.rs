use anyhow::Result;
use polars::prelude::*;

use super::Frame;

impl Frame {
    /// Performs a left join operation between two frames.
    ///
    /// # Arguments
    ///
    /// * `other` - The right frame to join with.
    /// * `left_on` - The column(s) to join on in the left frame.
    /// * `right_on` - The column(s) to join on in the right frame.
    ///
    /// # Returns
    ///
    /// A `Result` containing the joined `Frame`.
    #[inline]
    pub fn left_join<E: Into<Expr>>(self, other: Self, left_on: E, right_on: E) -> Result<Self> {
        let lazy_flag = self.is_lazy() || other.is_lazy();
        let lf = self.lazy().left_join(other.lazy(), left_on, right_on);
        if lazy_flag {
            Ok(lf.into())
        } else {
            Ok(lf.collect()?.into())
        }
    }

    /// Performs a full join operation between two frames.
    ///
    /// # Arguments
    ///
    /// * `other` - The right frame to join with.
    /// * `left_on` - The column(s) to join on in the left frame.
    /// * `right_on` - The column(s) to join on in the right frame.
    ///
    /// # Returns
    ///
    /// A `Result` containing the joined `Frame`.
    #[inline]
    pub fn full_join<E: Into<Expr>>(self, other: Self, left_on: E, right_on: E) -> Result<Self> {
        let lazy_flag = self.is_lazy() || other.is_lazy();
        let lf = self.lazy().full_join(other.lazy(), left_on, right_on);
        if lazy_flag {
            Ok(lf.into())
        } else {
            Ok(lf.collect()?.into())
        }
    }

    /// Performs an inner join operation between two frames.
    ///
    /// # Arguments
    ///
    /// * `other` - The right frame to join with.
    /// * `left_on` - The column(s) to join on in the left frame.
    /// * `right_on` - The column(s) to join on in the right frame.
    ///
    /// # Returns
    ///
    /// A `Result` containing the joined `Frame`.
    #[inline]
    pub fn inner_join<E: Into<Expr>>(self, other: Self, left_on: E, right_on: E) -> Result<Self> {
        let lazy_flag = self.is_lazy() || other.is_lazy();
        let lf = self.lazy().inner_join(other.lazy(), left_on, right_on);
        if lazy_flag {
            Ok(lf.into())
        } else {
            Ok(lf.collect()?.into())
        }
    }

    /// Performs a join operation between two frames with custom join arguments.
    ///
    /// # Arguments
    ///
    /// * `other` - The right frame to join with.
    /// * `left_on` - The column(s) to join on in the left frame.
    /// * `right_on` - The column(s) to join on in the right frame.
    /// * `args` - Custom join arguments.
    ///
    /// # Returns
    ///
    /// A `Result` containing the joined `Frame`.
    #[inline]
    pub fn join<E: AsRef<[Expr]>>(
        self,
        other: Self,
        left_on: E,
        right_on: E,
        args: JoinArgs,
    ) -> Result<Self> {
        let lazy_flag = self.is_lazy() || other.is_lazy();
        let lf = self.lazy().join(other.lazy(), left_on, right_on, args);
        if lazy_flag {
            Ok(lf.into())
        } else {
            Ok(lf.collect()?.into())
        }
    }
}
