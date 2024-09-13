import polars as pl
from polars.type_aliases import IntoExpr


def stable_corr(x: pl.Expr, y: pl.Expr, method="pearson"):
    return pl.corr(x, y, method=method).clip(-0.2, 0.2).fill_nan(None)


def get_ts_group(fac: IntoExpr, group: int = 10, *, drop_peak=False, peak_bound=0.025):
    import polars_qt as pq
    fac = pq.parse_into_expr(fac)
    if drop_peak:
        # 已经在预处理的时候去除了极端值
        fac_max = fac.quantile(1 - peak_bound, interpolation='nearest')
        fac_min = fac.quantile(peak_bound, interpolation='nearest')
        fac = fac.clip(fac_min, fac_max)
    else:
        fac_max = fac.max()
        fac_min = fac.min()
    eps = 1e-10
    # bins = pq.linspace(fac_min - fac_min.abs() * 1e-4, fac_max + fac_max.abs() * 1e-4, group + 1, eager=False)
    bins = pq.linspace(fac_min - eps, fac_max + eps, group + 1, eager=False)
    if group == 20:
        labels = [-1., -0.9, -0.8, -0.7, -0.6, -0.5, -0.4, -0.3, -0.2, -0.1,
                    0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.]
    elif group == 10:
        labels = [-1., -0.8, -0.6, -0.4, -0.2, 0.2, 0.4, 0.6, 0.8, 1.]
    else:
        labels = pq.linspace(-1, 1, group)
    return fac.qt.cut(bins, labels=labels, right=True, add_bounds=False)
