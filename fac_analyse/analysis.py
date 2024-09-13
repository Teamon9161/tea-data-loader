from __future__ import annotations

from typing import ClassVar

import pandas as pd
import polars as pl
from polars import col

from .summary import Summary
from .utils import get_ts_group, stable_corr


class Analysis:
    """
    因子分析框架
    1. 因子IC计算
    2. 因子分组收益
    3. 因子半衰期
    4. 因子衰减性分析
    """
    default_label_periods: ClassVar[list[str]] = [10, 30, 60, 120, 240, 480, 720, 1200]
    def __init__(self, dfs, facs: list[str], base_label: str, symbols=None, label_periods=None, type_=None, freq=None, *, drop_peak=True):
        if type(dfs).__qualname__ == "DataLoader":
            self.dl = dfs
        else:
            from data_loader import DataLoader
            self.dl = DataLoader.from_dfs(dfs, symbols=symbols, type=type_, freq=freq)
        self.symbols = self.dl.symbols
        self.dfs = self.dl.dfs
        self.facs = facs
        self.base_label = base_label
        self.label_periods = self.default_label_periods if label_periods is None else label_periods
        self.label_periods = [1, *self.label_periods]
        self.labels = [f'label_{n}' for n in self.label_periods]
        self.columns = [f'{fac}_{label}' for fac in self.facs for label in self.labels]
        self.dl: DataLoader = (
            self.dl
                .with_columns(col(facs).fill_nan(None).fac.winsorize(q=0.025, method='quantile'), flag=drop_peak)
                .with_columns([
                    ((1 + col(self.base_label)).log().rolling_sum(n, min_periods=n//2).exp() - 1).shift(-n+1).fill_null(0).alias(f'label_{n}')
                    for n in self.label_periods
                    if n != 1
                ], label_1=self.base_label)
                .collect("Calculating base labels")
                .lazy()
        )
        # result
        self.summary: Summary = Summary(facs, self.labels)

    @property
    def s(self) -> Summary:
        """short-hand of summary"""
        return self.summary

    def ic_overall(self, method: str = 'pearson'):
        ics = []
        for fac in self.facs:
            ic = self.dl.select([
                stable_corr(fac, label, method=method).alias(f'{label}')
                for label in self.labels
            ], count=pl.len()).collect(False)
            ics.append(ic)
        self.summary.symbol_ic = [ic.drop('count') for ic in ics]
        self.summary._ic_overall = [ic.agg_dfs(weight='count') for ic in ics]
        return self

    def ts_ic(self, rule: str = '1mo', method: str = "pearson", *, keep_symbol_ic=False):
        symbol_ts_ic = []
        for fac in self.facs:
            symbol_ts_ic.append(
                self.dl.group_by_time(rule, time=self.dl.daily_col)
                .agg([
                    stable_corr(fac, label, method=method).alias(label)
                    for label in self.labels
                ])
                .collect(False)
                .align(on=self.dl.daily_col)# 对齐时间
            )
        if keep_symbol_ic:
            self.summary.symbol_ts_ic = symbol_ts_ic
        ts_ic = [
            s_ic.agg_dfs({self.dl.daily_col: 'first'})
            .to_pandas()
            .set_index(self.dl.daily_col)
            for s_ic in symbol_ts_ic
        ]
        self.summary.ts_ic = ts_ic
        return self


    def ts_group_ret(self, facs: list[str] | None = None, group: int = 10):
        if facs is None:
            facs = self.facs
        if isinstance(facs, str):
            facs = [facs]
        # 日频的平均分组下期收益
        ts_group_rets = []
        for fac in facs:
            group_ret = (
                self.dl
                .apply(lambda df, fac=fac: (
                    # 按照日频聚合分组收益
                    df.group_by([self.dl.daily_col, get_ts_group(fac, group=group).alias('group')])
                    .agg([(col(f'label_{n}') / n).sum() for n in self.label_periods])
                ))
                .sort(['group', self.dl.daily_col])
                .collect(f"计算{fac}每日分组收益")
                .align(on=['group', self.dl.daily_col])
            )
            ts_group_rets.append(group_ret)
        self.summary.symbol_ts_group_rets = ts_group_rets
        self.summary.ts_group_rets = [
            tgr.agg_dfs({'group': 'first', self.dl.daily_col: 'first'})
            .to_pandas()
            .set_index(self.dl.daily_col)
            for tgr in ts_group_rets
        ]
        return self


    def group_ret(self, facs: list[str] | None = None, rule: str | None = None, group: int = 10):
        if facs is None:
            facs = self.facs
        if isinstance(facs, str):
            facs = [facs]

        # 按月份对齐平均分组收益, 计算全品种的分组收益
        group_rets = []
        if rule is None:
            for fac in facs:
                group_ret = (
                    self.dl
                    .lazy()
                    .apply(lambda df, fac=fac: (
                        # 按照日频聚合分组收益
                        df
                        .group_by(get_ts_group(fac, group=group).alias('group'))
                        .agg([
                            col(fac).min().alias('min'),
                            col(fac).max().alias('max'),
                            col(fac).count().alias('count'),
                        ] + [
                            col(f'label_{n}').mean()
                            for n in self.label_periods
                        ])
                    ))
                    .collect(f"计算并对齐{fac}历史分组收益")
                    .align(on='group')
                )
                group_rets.append(group_ret)
            self.summary.symbol_group_rets = group_rets
            self.summary.group_rets = [gr.agg_dfs({'group': 'first'}, weight='count').to_pandas() for gr in group_rets]
        else:
            for fac in facs:
                group_ret = (
                    self.dl
                    .lazy()
                    .with_columns(group=get_ts_group(fac, group=group))
                    .sort(['group', self.dl.daily_col])
                    .group_by_time(rule, time=self.dl.daily_col, group_by='group')
                    .agg([
                        col(fac).min().alias('min'),
                        col(fac).max().alias('max'),
                        col(fac).count().alias('count'),
                    ] + [
                        col(f'label_{n}').mean()
                        for n in self.label_periods
                    ])
                    .collect(f"计算并对齐{fac}历史分组收益")
                    .align(on=['group', self.dl.daily_col])
                )
                group_rets.append(group_ret)
            self.summary.symbol_group_rets = group_rets
            self.summary.group_rets = [
                gr.apply(lambda df:
                    df.group_by('group')
                        .agg(pl.all().exclude(self.dl.daily_col).mean())
                )
                .agg_dfs({'group': 'first'})
                .to_pandas()
                for gr in group_rets
            ]
        return self

    def half_life(self):
        if self.summary.half_life is None:
            half_life = (
                self.dl.select(
                    col(self.facs).qt.half_life()
                )
                .collect("计算因子半衰期")
            )
            self.summary.half_life = half_life.agg_dfs(default='mean').to_pandas()
        return self

    def group_analyse(self, fac: str | int, c_rate=3e-4):
        import numpy as np
        self.half_life()
        fac_name = self.facs[fac] if isinstance(fac, int) else fac
        df = self.summary.fac_attr('group_rets', fac)[self.labels]
        # 转为分钟平均收益
        df = df.apply(lambda s: s / int(s.name.replace('label_', '')))
        c_rate = c_rate / np.array(self.label_periods)  # 分钟平均手续费
        max_group_ret = df.iloc[-1, :]
        min_group_ret = df.iloc[0, :]
        df = max_group_ret - min_group_ret - c_rate * 4
        best_period = df.idxmax().replace('label_', '')
        best_ret = df.max()
        print(f'{fac_name} 最优的周期是', best_period, '因子半衰期: ', self.summary.fac_attr('half_life', fac)[0])
        return best_period, best_ret, df

