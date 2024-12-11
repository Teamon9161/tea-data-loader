from __future__ import annotations

from .rs_loader import _RS_DataLoaderGroupBy

from typing import TYPE_CHECKING
from polars._utils.parse import parse_into_list_of_expressions


if TYPE_CHECKING:
    from .py_loader import DataLoader
    from collections.abc import Iterable
    from polars._typing import IntoExpr

class DataLoaderGroupBy:
    def __init__(self, dlgb: _RS_DataLoaderGroupBy):
        self.dlgb = dlgb

    @property
    def dl(self) -> DataLoader:
        """The underlying DataLoader"""
        from .py_loader import DataLoader
        return DataLoader(self.dlgb.dl)

    @property
    def last_time(self) -> str | None:
        """The last time column name if present"""
        return self.dlgb.last_time

    @property
    def time(self) -> str | None:
        """The time column name if present"""
        return self.dlgb.time

    def agg(self, *aggs: IntoExpr | Iterable[IntoExpr], **named_aggs: IntoExpr) -> DataLoader:
        """
        Applies aggregation functions to the grouped data.

        This method performs aggregation on the grouped data using the provided aggregation expressions.
        It handles different scenarios based on the presence of a last time column and its relation
        to the time column.

        Args:
            aggs: A list of aggregation expressions to apply to the grouped data.

        Returns:
            A DataLoader instance containing the aggregated data.
        """
        from .py_loader import DataLoader
        aggs = parse_into_list_of_expressions(*aggs, **named_aggs)
        return DataLoader(self.dlgb.agg(aggs))
