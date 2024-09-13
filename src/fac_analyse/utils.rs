use std::sync::LazyLock;

use polars::prelude::*;
use tea_strategy::tevec::prelude::{CorrMethod, Vec1Create, EPS};

use super::linspace;
use crate::prelude::*;

pub(super) fn stable_corr(a: Expr, b: Expr, method: CorrMethod) -> Expr {
    let corr = match method {
        CorrMethod::Pearson => pearson_corr(a, b, 1),
        CorrMethod::Spearman => spearman_rank_corr(a, b, 1, true),
    };
    corr.clip(-0.2.lit(), 0.2.lit()).fill_nan(NULL.lit())
}

const GROUP_20_LABELS: [f64; 20] = [
    -1.0, -0.9, -0.8, -0.7, -0.6, -0.5, -0.4, -0.3, -0.2, -0.1, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7,
    0.8, 0.9, 1.0,
];

const GROUP_10_LABELS: [f64; 10] = [-1.0, -0.8, -0.6, -0.4, -0.2, 0.2, 0.4, 0.6, 0.8, 1.0];

const GROUP_20_LABEL_SERIES: LazyLock<Series> = LazyLock::new(|| {
    let labels: Vec<f64> = GROUP_20_LABELS.into();
    Series::from_vec("group", labels)
});

const GROUP_10_LABEL_SERIES: LazyLock<Series> = LazyLock::new(|| {
    let labels: Vec<f64> = GROUP_10_LABELS.into();
    Series::from_vec("group", labels)
});

pub(super) fn get_ts_group(fac: Expr, group: usize) -> Expr {
    let fac_max = fac.clone().max();
    let fac_min = fac.clone().min();
    let bins = linspace(
        fac_min - EPS.lit(),
        fac_max + EPS.lit(),
        ((group + 1) as i32).lit(),
    );
    match group {
        20 => fac.cut(
            bins,
            GROUP_20_LABEL_SERIES.clone().lit(),
            Some(true),
            Some(false),
        ),
        10 => fac.cut(
            bins,
            GROUP_10_LABEL_SERIES.clone().lit(),
            Some(true),
            Some(false),
        ),
        _ => {
            let labels: Vec<f64> = Vec1Create::linspace(Some(-1.), 1., group);
            let labels = Series::from_vec("group", labels);
            fac.cut(bins, labels.lit(), Some(true), Some(false))
        },
    }
}
