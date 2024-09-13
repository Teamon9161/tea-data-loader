from __future__ import annotations

import polars as pl
from polars import col


class Summary:
    def __init__(self, facs: list[str], labels: list[str]):
        self.facs = facs
        self.labels = labels

    def __getitem__(self, fac: str | int) -> FacSummary:
        if not isinstance(fac, (list, tuple)):
            return FacSummary(fac, self)
        else:
            return [FacSummary(f, self) for f in fac]

    def __getattr__(self, name):
        if name == 'ic':
            return self.concat('ts_ic', col(self.labels).mean())
        elif name == 'ic_std':
            return self.concat('ts_ic', col(self.labels).std())
        elif name == 'ir':
            return self.ic / self.ic_std
        elif name == 'ic_skew':
            return self.concat('ts_ic', col(self.labels).skew())
        elif name == 'ic_kurt':
            return self.concat('ts_ic', col(self.labels).kurtosis())
        elif name == 'ic_overall':
            return self.concat('_ic_overall', pl.all())
        elif name in self.facs:
            return FacSummary(name, self)
        else:
            return None
            # msg = f"Summary has no attribute {name}"
            # raise AttributeError(msg)

    def apply(self, attr: str, f: callable, *args, **kwargs):
        dfs = getattr(self, attr)
        return [
            f(df, *args, **kwargs)
            for df in dfs
        ]

    def _fac_attr_single(self, attr: str, fac: str | int):
        df = None
        values = getattr(self, attr)
        if isinstance(fac, str):
            if isinstance(values, (list, tuple)):
                for f, value in zip(self.facs, values):
                    if f != fac:
                        continue
                    df = value
                    break
            else:
                df = values[fac]
        elif isinstance(fac, int):
            if isinstance(values, (list, tuple)):
                df = values[fac]
            else:
                df = values.iloc[:, fac]
        else:
            raise TypeError("fac should be str or int")
        if df is None:
            msg = f"fac: {fac} not in result"
            raise ValueError(msg)
        return df

    def fac_attr(self, attr: str, facs: str | int | list[str | int]):
        if not isinstance(facs, (list, tuple)):
            return self._fac_attr_single(attr, facs)
        else:
            return [self._fac_attr_single(attr, fac) for fac in facs]

    def concat(self, attr, exprs=None, how='vertical'):
        if exprs is None:
            exprs = []
        res = self.apply(attr, lambda df: df.select(exprs))
        return (
            pl.concat(res, how=how)
            .with_columns(fac=pl.Series(self.facs))
            .to_pandas()
            .set_index('fac')
        )

class FacSummary:
    def __init__(self, fac: int | str, summary: Summary):
        self.summary = summary
        self.fac = fac

    def __getattr__(self, attr):
        return self.summary.fac_attr(attr, self.fac)

    def plot_group(self, period: str | int):
        import matplotlib.pyplot as plt
        label = f'label_{period}' if isinstance(period, int) else period
        df = self.summary.fac_attr('ts_group_rets', self.fac)
        for group, group_df in df.groupby('group'):
            group_df[label].rename(group).cumsum().plot()
        plt.legend(loc='upper left')
        plt.show()
        return self

    def plot_ic(self, period: str | int):
        import matplotlib.pyplot as plt
        label = f'label_{period}' if isinstance(period, int) else period
        df = self.summary.fac_attr('ts_ic', self.fac)[label]
        df.reset_index(drop=True).plot.bar(xticks=[])
        plt.show()
        df.expanding(12).mean().plot(color="r")
        plt.show()
        return self
