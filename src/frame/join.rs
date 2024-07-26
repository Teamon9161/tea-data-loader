use anyhow::Result;
use polars::prelude::*;

use super::Frame;

impl Frame {
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
