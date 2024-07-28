use polars::lazy::dsl::{cols, when};
use polars::prelude::*;
use tea_strategy::tevec::prelude::{Cast, DateTime};

fn get_preprocess_exprs_impl(typ: &str) -> Vec<Expr> {
    match typ {
        "__base__" => {
            vec![when(cols(["close", "open", "high", "low"]).eq(0))
                .then(lit(NULL))
                .otherwise(cols(["close", "open", "high", "low"]))
                .forward_fill(None)
                .name()
                .keep()]
        },
        "future" => {
            let mut base_exprs = get_preprocess_exprs("__base__");
            base_exprs.extend([
                when(col("volume").lt(0))
                    .then(0)
                    .otherwise("volume")
                    .alias("volume"),
                col("dominant_id")
                    .neq(col("dominant_id").shift(lit(1)))
                    .fill_null(false)
                    .alias("contract_chg_signal"),
            ]);
            base_exprs
        },
        _ => unimplemented!("preprocess exprs is not implemented for type: {}", typ),
    }
}

fn get_filter_cond_impl(
    start: Option<DateTime>,
    end: Option<DateTime>,
    time: &str,
) -> Option<Expr> {
    match (start, end) {
        (Some(start), Some(end)) => {
            let start = start.to_cr().unwrap().naive_utc();
            let end = end.to_cr().unwrap().naive_utc();
            Some((col(time).gt_eq(lit(start))).and(col(time).lt_eq(lit(end))))
        },
        (Some(start), None) => {
            let start = start.to_cr().unwrap().naive_utc();
            Some(col(time).gt_eq(lit(start)))
        },
        (None, Some(end)) => {
            let end = end.to_cr().unwrap().naive_utc();
            Some(col(time).lt_eq(lit(end)))
        },
        (None, None) => None,
    }
}

#[inline]
pub fn get_filter_cond<A: Cast<DateTime>, B: Cast<DateTime>, T: AsRef<str>>(
    start: Option<A>,
    end: Option<B>,
    time: T,
) -> Option<Expr> {
    get_filter_cond_impl(start.map(Cast::cast), end.map(Cast::cast), time.as_ref())
}

#[inline]
pub fn get_preprocess_exprs<S: AsRef<str>>(typ: S) -> Vec<Expr> {
    get_preprocess_exprs_impl(typ.as_ref())
}
