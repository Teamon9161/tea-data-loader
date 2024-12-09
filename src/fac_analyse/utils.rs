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
    corr.clip(-0.3.lit(), 0.3.lit()).fill_nan(NULL.lit())
}

#[allow(dead_code)]
/// 按照因子值的范围进行分组（每组的数量可能会有差异）
pub(super) fn get_ts_group_by_value(fac: Expr, group: usize) -> Expr {
    const GROUP_20_LABELS: [f64; 20] = [
        -1.0, -0.9, -0.8, -0.7, -0.6, -0.5, -0.4, -0.3, -0.2, -0.1, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6,
        0.7, 0.8, 0.9, 1.0,
    ];

    const GROUP_10_LABELS: [f64; 10] = [-1.0, -0.8, -0.6, -0.4, -0.2, 0.2, 0.4, 0.6, 0.8, 1.0];

    const GROUP_20_LABEL_SERIES: LazyLock<Series> = LazyLock::new(|| {
        let labels: Vec<f64> = GROUP_20_LABELS.into();
        Series::from_vec("group".into(), labels)
    });

    const GROUP_10_LABEL_SERIES: LazyLock<Series> = LazyLock::new(|| {
        let labels: Vec<f64> = GROUP_10_LABELS.into();
        Series::from_vec("group".into(), labels)
    });

    let fac_max = fac.clone().max();
    let fac_min = fac.clone().min();
    let bins = linspace(
        fac_min - EPS.lit(),
        fac_max + EPS.lit(),
        ((group + 1) as i32).lit(),
    );
    match group {
        20 => fac.tcut(
            bins,
            GROUP_20_LABEL_SERIES.clone().lit(),
            Some(true),
            Some(false),
        ),
        10 => fac.tcut(
            bins,
            GROUP_10_LABEL_SERIES.clone().lit(),
            Some(true),
            Some(false),
        ),
        _ => {
            let labels: Vec<f64> = Vec1Create::linspace(Some(-1.), 1., group);
            let labels = Series::from_vec("group".into(), labels);
            fac.tcut(bins, labels.lit(), Some(true), Some(false))
        },
    }
}

/// 根据因子值等量分组
pub(super) fn get_ts_group_by_count(fac: Expr, group: usize) -> Expr {
    let fac_rank = fac.clone().rank(
        RankOptions {
            method: RankMethod::Average,
            ..Default::default()
        },
        None,
    );
    let count = fac.count();
    // (fac_rank * (group as f64).lit()).protect_div(count)
    (fac_rank * (group as f64).lit()).protect_div(count).ceil()
}

pub(super) fn get_ts_group(fac: Expr, group: usize) -> Expr {
    get_ts_group_by_count(fac, group)
}

pub(super) fn infer_label_periods<S: AsRef<str>>(
    labels: impl IntoIterator<Item = S>,
) -> Vec<usize> {
    labels
        .into_iter()
        .map(|s| {
            let s = s.as_ref().split("_").last().unwrap();
            s.parse::<usize>().unwrap()
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use polars::prelude::*;

    use super::*;

    #[test]
    fn test_get_ts_group_by_count() {
        let df = df! [
            "fac" => [1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0],
        ]
        .unwrap();

        let fac = col("fac");
        let result = df
            .lazy()
            .select([get_ts_group_by_count(fac, 5)])
            .collect()
            .unwrap();

        let expected = Series::new("fac".into(), &[1, 1, 2, 2, 3, 3, 4, 4, 5, 5]);
        assert_eq!(
            result.column("fac").unwrap().as_materialized_series(),
            &expected
        );
    }
}
