use polars::lazy::dsl::{cols, when};
use polars::prelude::*;
use tea_strategy::tevec::prelude::{Cast, DateTime};
/// Utility functions for preprocessing and filtering data in the DataLoader.

/// Returns preprocessing expressions based on the given data type.
///
/// # Arguments
///
/// * `typ` - A string slice representing the data type.
///
/// # Returns
///
/// A vector of `Expr` objects containing the preprocessing expressions.
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
        "xbond" => vec![],
        "ddb-xbond" => vec![],
        _ => {
            eprintln!("preprocess exprs is not implemented for type: {}", typ);
            vec![]
        },
    }
}

/// Generates a filter condition based on start and end dates.
///
/// # Arguments
///
/// * `start` - An optional start DateTime.
/// * `end` - An optional end DateTime.
/// * `time` - A string slice representing the time column name.
///
/// # Returns
///
/// An optional `Expr` representing the filter condition.
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

/// Creates a time filter condition based on start and end dates.
///
/// # Arguments
///
/// * `start` - An optional start date that can be cast to DateTime.
/// * `end` - An optional end date that can be cast to DateTime.
/// * `time` - A string slice representing the time column name.
///
/// # Returns
///
/// An optional `Expr` representing the time filter condition.
#[inline]
pub fn get_time_filter_cond<A: Cast<DateTime>, B: Cast<DateTime>, T: AsRef<str>>(
    start: Option<A>,
    end: Option<B>,
    time: T,
) -> Option<Expr> {
    get_filter_cond_impl(start.map(Cast::cast), end.map(Cast::cast), time.as_ref())
}

/// Returns preprocessing expressions for a given data type.
///
/// # Arguments
///
/// * `typ` - A string slice representing the data type.
///
/// # Returns
///
/// A vector of `Expr` objects containing the preprocessing expressions.
#[inline]
pub fn get_preprocess_exprs<S: AsRef<str>>(typ: S) -> Vec<Expr> {
    get_preprocess_exprs_impl(typ.as_ref())
}
