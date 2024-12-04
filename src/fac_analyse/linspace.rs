use polars::prelude::*;
use tea_strategy::tevec::prelude::Vec1Create;

pub fn linspace(start: Expr, end: Expr, num: Expr) -> Expr {
    use DataType::*;
    start.apply_many(
        |exprs| {
            let start = &exprs[0];
            let end = &exprs[1];
            let num = &exprs[2];
            let name = start.name();
            polars_ensure!(
                (start.len() == 1) && (end.len() == 1) && (num.len() == 1),
                ComputeError: "linspace expects all inputs to be scalars"
            );

            let start = Some(start.cast(&Float64)?.f64()?.get(0).unwrap());
            let end = end.cast(&Float64)?.f64()?.get(0).unwrap();
            let num = num.cast(&Int32)?.i32()?.get(0).unwrap() as usize;
            let arr: Float64Chunked = Vec1Create::linspace(start, end, num);
            Ok(Some(
                arr.with_name(name.clone()).into_series().into_column(),
            ))
        },
        &[end, num],
        GetOutput::map_dtypes(|_dtypes| Ok(Float64)),
    )
}
