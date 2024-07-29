macro_rules! impl_by_tea_strategy {
    ($strategy: ident $({$mark: tt})?) => {
        fn eval_to_fac(&self, fac: &::polars::prelude::Series, filters: Option<::polars::prelude::DataFrame>) -> ::anyhow::Result<::polars::prelude::Series> {
            use ::polars::prelude::{DataType, Float64Chunked};
            let out: Float64Chunked = match fac.dtype() {
                DataType::Int32 => {
                    ::tea_strategy::$strategy(fac.i32()?, filters.map(Into::into).as_ref(), &self.0)$($mark)?
                },
                DataType::Int64 => {
                    ::tea_strategy::$strategy(fac.i64()?, filters.map(Into::into).as_ref(), &self.0)$($mark)?
                },
                DataType::Float32 => {
                    ::tea_strategy::$strategy(fac.f32()?, filters.map(Into::into).as_ref(), &self.0)$($mark)?
                },
                DataType::Float64 => {
                    ::tea_strategy::$strategy(fac.f64()?, filters.map(Into::into).as_ref(), &self.0)$($mark)?
                },
                dtype => ::anyhow::bail!(
                    "dtype {} not supported for {}",
                    dtype,
                    stringify!($strategy)
                ),
            };
            Ok(out.into())
        }
    };
}

pub(super) use impl_by_tea_strategy;
