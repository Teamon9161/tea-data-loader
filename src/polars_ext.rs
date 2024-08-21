use polars::lazy::dsl::{Expr, GetOutput};
use polars::prelude::{DataType, *};

use crate::export::tevec::prelude::*;

pub trait SeriesExt {
    fn cast_f64(&self) -> Result<Series>;
    fn cast_bool(&self) -> Result<Series>;
    fn cast_f32(&self) -> Result<Series>;
    fn ts_ewm(&self, window: usize, min_periods: Option<usize>) -> Self;
    fn ts_skew(&self, window: usize, min_periods: Option<usize>) -> Self;
    fn ts_kurt(&self, window: usize, min_periods: Option<usize>) -> Self;
    fn ts_rank(&self, window: usize, min_periods: Option<usize>, pct: bool, rev: bool) -> Self;
    fn ts_zscore(&self, window: usize, min_periods: Option<usize>) -> Self;
    fn ts_regx_beta(&self, x: &Series, window: usize, min_periods: Option<usize>) -> Self;
}

impl SeriesExt for Series {
    #[inline]
    fn cast_f64(&self) -> Result<Series> {
        if let DataType::Float64 = self.dtype() {
            Ok(self.clone())
        } else {
            Ok(Series::cast(self, &DataType::Float64)?)
        }
    }

    #[inline]
    fn cast_bool(&self) -> Result<Series> {
        if let DataType::Boolean = self.dtype() {
            Ok(self.clone())
        } else {
            Ok(Series::cast(self, &DataType::Boolean)?)
        }
    }

    #[inline]
    fn cast_f32(&self) -> Result<Series> {
        if let DataType::Float32 = self.dtype() {
            Ok(self.clone())
        } else {
            Ok(Series::cast(self, &DataType::Float32)?)
        }
    }

    fn ts_ewm(&self, window: usize, min_periods: Option<usize>) -> Self {
        let res: Series = match self.dtype() {
            DataType::Float64 => {
                let ca: Float64Chunked = self.f64().unwrap().ts_vewm(window, min_periods);
                ca.into_series()
            },
            DataType::Float32 => {
                let ca: Float32Chunked = self.f32().unwrap().ts_vewm(window, min_periods);
                ca.into_series()
            },
            DataType::Int64 => {
                let ca: Float64Chunked = self.i64().unwrap().ts_vewm(window, min_periods);
                ca.into_series()
            },
            DataType::Int32 => {
                let ca: Float64Chunked = self.i32().unwrap().ts_vewm(window, min_periods);
                ca.into_series()
            },
            _ => panic!("unsupported data type"),
        };
        res
    }

    fn ts_skew(&self, window: usize, min_periods: Option<usize>) -> Self {
        let res: Series = match self.dtype() {
            DataType::Float64 => {
                let ca: Float64Chunked = self.f64().unwrap().ts_vskew(window, min_periods);
                ca.into_series()
            },
            DataType::Float32 => {
                let ca: Float32Chunked = self.f32().unwrap().ts_vskew(window, min_periods);
                ca.into_series()
            },
            DataType::Int64 => {
                let ca: Float64Chunked = self.i64().unwrap().ts_vskew(window, min_periods);
                ca.into_series()
            },
            DataType::Int32 => {
                let ca: Float64Chunked = self.i32().unwrap().ts_vskew(window, min_periods);
                ca.into_series()
            },
            _ => panic!("unsupported data type"),
        };
        res
    }

    fn ts_kurt(&self, window: usize, min_periods: Option<usize>) -> Self {
        let res: Series = match self.dtype() {
            DataType::Float64 => {
                let ca: Float64Chunked = self.f64().unwrap().ts_vkurt(window, min_periods);
                ca.into_series()
            },
            DataType::Float32 => {
                let ca: Float32Chunked = self.f32().unwrap().ts_vkurt(window, min_periods);
                ca.into_series()
            },
            DataType::Int64 => {
                let ca: Float64Chunked = self.i64().unwrap().ts_vkurt(window, min_periods);
                ca.into_series()
            },
            DataType::Int32 => {
                let ca: Float64Chunked = self.i32().unwrap().ts_vkurt(window, min_periods);
                ca.into_series()
            },
            _ => panic!("unsupported data type"),
        };
        res
    }

    fn ts_rank(&self, window: usize, min_periods: Option<usize>, pct: bool, rev: bool) -> Self {
        let res: Series = match self.dtype() {
            DataType::Float64 => {
                let ca: Float64Chunked =
                    self.f64().unwrap().ts_vrank(window, min_periods, pct, rev);
                ca.into_series()
            },
            DataType::Float32 => {
                let ca: Float32Chunked =
                    self.f32().unwrap().ts_vrank(window, min_periods, pct, rev);
                ca.into_series()
            },
            DataType::Int64 => {
                let ca: Float64Chunked =
                    self.i64().unwrap().ts_vrank(window, min_periods, pct, rev);
                ca.into_series()
            },
            DataType::Int32 => {
                let ca: Float64Chunked =
                    self.i32().unwrap().ts_vrank(window, min_periods, pct, rev);
                ca.into_series()
            },
            _ => panic!("unsupported data type"),
        };
        res
    }

    fn ts_zscore(&self, window: usize, min_periods: Option<usize>) -> Self {
        let res: Series = match self.dtype() {
            DataType::Float64 => {
                let ca: Float64Chunked = self.f64().unwrap().ts_vzscore(window, min_periods);
                ca.into_series()
            },
            DataType::Float32 => {
                let ca: Float32Chunked = self.f32().unwrap().ts_vzscore(window, min_periods);
                ca.into_series()
            },
            DataType::Int64 => {
                let ca: Float64Chunked = self.i64().unwrap().ts_vzscore(window, min_periods);
                ca.into_series()
            },
            DataType::Int32 => {
                let ca: Float64Chunked = self.i32().unwrap().ts_vzscore(window, min_periods);
                ca.into_series()
            },
            _ => panic!("unsupported data type"),
        };
        res
    }

    fn ts_regx_beta(&self, x: &Series, window: usize, min_periods: Option<usize>) -> Self {
        let res: Series = match self.dtype() {
            DataType::Float64 => {
                let ca: Float64Chunked = self.f64().unwrap().ts_vregx_beta(
                    x.cast_f64().unwrap().f64().unwrap(),
                    window,
                    min_periods,
                );
                ca.into_series()
            },
            DataType::Float32 => {
                let ca: Float32Chunked = self.f32().unwrap().ts_vregx_beta(
                    x.cast_f32().unwrap().f32().unwrap(),
                    window,
                    min_periods,
                );
                ca.into_series()
            },
            DataType::Int64 => {
                let ca: Float64Chunked = self.i64().unwrap().ts_vregx_beta(
                    x.cast_f64().unwrap().f64().unwrap(),
                    window,
                    min_periods,
                );
                ca.into_series()
            },
            DataType::Int32 => {
                let ca: Float64Chunked = self.i32().unwrap().ts_vregx_beta(
                    x.cast_f64().unwrap().f64().unwrap(),
                    window,
                    min_periods,
                );
                ca.into_series()
            },
            _ => panic!("unsupported data type"),
        };
        res
    }
}

pub trait ExprExt {
    fn ts_ewm(self, window: usize, min_periods: Option<usize>) -> Self;
    fn ts_skew(self, window: usize, min_periods: Option<usize>) -> Self;
    fn ts_kurt(self, window: usize, min_periods: Option<usize>) -> Self;
    fn ts_rank(self, window: usize, min_periods: Option<usize>, pct: bool, rev: bool) -> Self;
    fn ts_zscore(self, window: usize, min_periods: Option<usize>) -> Self;
    fn ts_regx_beta(self, x: Expr, window: usize, min_periods: Option<usize>) -> Self;
}

impl ExprExt for Expr {
    fn ts_ewm(self, window: usize, min_periods: Option<usize>) -> Self {
        self.apply(
            move |s| Ok(Some(s.ts_ewm(window, min_periods))),
            GetOutput::float_type(),
        )
    }

    fn ts_skew(self, window: usize, min_periods: Option<usize>) -> Self {
        self.apply(
            move |s| Ok(Some(s.ts_skew(window, min_periods))),
            GetOutput::float_type(),
        )
    }

    fn ts_kurt(self, window: usize, min_periods: Option<usize>) -> Self {
        self.apply(
            move |s| Ok(Some(s.ts_kurt(window, min_periods))),
            GetOutput::float_type(),
        )
    }

    fn ts_rank(self, window: usize, min_periods: Option<usize>, pct: bool, rev: bool) -> Self {
        self.apply(
            move |s| Ok(Some(s.ts_rank(window, min_periods, pct, rev))),
            GetOutput::float_type(),
        )
    }

    fn ts_zscore(self, window: usize, min_periods: Option<usize>) -> Self {
        self.apply(
            move |s| Ok(Some(s.ts_zscore(window, min_periods))),
            GetOutput::float_type(),
        )
    }

    fn ts_regx_beta(self, x: Expr, window: usize, min_periods: Option<usize>) -> Self {
        self.apply_many(
            move |series_slice| {
                let y = &series_slice[0];
                let x = &series_slice[1];
                Ok(Some(y.ts_regx_beta(x, window, min_periods)))
            },
            &[x],
            GetOutput::map_dtypes(|dtypes| {
                Ok(match dtypes[0] {
                    DataType::Float32 => DataType::Float32,
                    _ => DataType::Float64,
                })
            }),
        )
    }
}
