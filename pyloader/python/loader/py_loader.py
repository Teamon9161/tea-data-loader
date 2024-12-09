from __future__ import annotations

import os
from typing import TYPE_CHECKING, Any, Callable

from polars import DataFrame, DataType, LazyFrame
from polars._utils.parse import parse_into_expression, parse_into_list_of_expressions
from typing_extensions import TypeAlias

from .rs_loader import DataLoaderGroupBy, _RS_Loader

if TYPE_CHECKING:
    from collections.abc import Iterable
    from datetime import datetime
    from pathlib import Path

    from polars._typing import ColumnNameOrSelector, IntoExpr

    # Type alias for DataFrame or LazyFrame
    PolarsFrame: TypeAlias = DataFrame | LazyFrame

structify = bool(int(os.environ.get("POLARS_AUTO_STRUCTIFY", 0)))


class DataLoader:
    """
    A class for loading and managing collections of DataFrames.

    DataLoader provides an interface for working with one or more polars DataFrames,
    optionally associating them with symbols. It inherits from _RS_Loader to provide
    the core loading functionality.
    """

    def __init__(
        self,
        typ: str | PolarsFrame | list[PolarsFrame] | _RS_Loader,
        symbols: list[str] | None = None,
    ) -> DataLoader:
        """
        Initialize a DataLoader instance.

        Parameters
        ----------
        typ : str | PolarsFrame | list[PolarsFrame] | _RS_Loader
            The type of data to load. Can be:
            - A string path to data files
            - A single Polars DataFrame/LazyFrame
            - A list of Polars DataFrames/LazyFrames
            - An existing _RS_Loader instance
        symbols : list[str] | None, default None
            Optional list of symbol names corresponding to the data frames.
            If None, no symbol names are assigned.

        Returns
        -------
        DataLoader
            The initialized DataLoader instance.
        """
        if isinstance(typ, _RS_Loader):
            self.dl: _RS_Loader = typ
            return
        if isinstance(typ, (LazyFrame, DataFrame)):
            typ = [typ]
        if isinstance(typ, (list, tuple)):
            dl = _RS_Loader("", symbols)
            dl.dfs = typ
            self.dl: _RS_Loader = dl
        else:
            self.dl: _RS_Loader = _RS_Loader(typ, symbols)

    def __repr__(self) -> str:
        return self.dl.__repr__()

    @property
    def dfs(self) -> list[PolarsFrame]:
        """Returns the list of DataFrames/LazyFrames."""
        return self.dl.dfs

    @property
    def symbols(self) -> list[str] | None:
        """Returns a list of symbols if present in the DataLoader."""
        return self.dl.symbols

    @property
    def type(self) -> str:
        """Returns the type of data."""
        return self.dl.type

    @property
    def start(self) -> datetime | None:
        """Returns the start date/time if set."""
        return self.dl.start

    @property
    def end(self) -> datetime | None:
        """Returns the end date/time if set."""
        return self.dl.end

    @property
    def freq(self) -> str | None:
        """Returns the frequency if set."""
        return self.dl.freq

    @property
    def schema(self) -> dict[str, DataType]:
        """Returns the schema of the first data frame in the DataLoader."""
        return self.dl.schema()

    @property
    def columns(self) -> list[str]:
        """Returns a list of column names from the first data frame in the DataLoader."""
        return self.dl.columns()

    def __iter__(self):
        """
        Iterate over the DataLoader's symbols and DataFrames.

        Returns:
            If symbols are present:
                An iterator of (symbol, DataFrame) tuples
            Otherwise:
                An iterator of just the DataFrames
        """
        if self.symbols is not None:
            return zip(self.symbols, self.dfs)
        else:
            return iter(self.dfs)

    def __getitem__(self, item: str | int) -> DataFrame | LazyFrame:
        return self.dl[item]

    def __setitem__(self, item: str | int, value: DataFrame | LazyFrame):
        self.dl[item] = value

    def __len__(self) -> int:
        """Returns the number of DataFrames/LazyFrames in the DataLoader."""
        return len(self.dl)

    def len(self) -> int:
        """Returns the number of DataFrames/LazyFrames in the DataLoader."""
        return len(self.dl)

    def is_empty(self) -> bool:
        """Returns `true` if the DataLoader contains no DataFrames/LazyFrames."""
        return self.dl.is_empty()

    def is_eager(self) -> bool:
        """Returns `true` if the DataLoader is eager. Note that only the first dataframe is checked."""
        return self.dl.is_eager()

    def is_lazy(self) -> bool:
        """Returns `true` if the DataLoader is lazy. Note that only the first dataframe is checked."""
        return self.dl.is_lazy()

    def find_index(self, symbol: str) -> int | None:
        """
        Finds the index of a given symbol in the DataLoader's symbols list.

        Args:
            symbol: The symbol name to search for

        Returns:
            The index position of the symbol if found, None if the symbol is not found
            or if the DataLoader has no symbols list.
        """
        return self.dl.find_index(symbol)

    def __setattr__(self, obj: str, value: Any):
        if obj in ["dfs", "symbols", "type", "start", "end", "freq"]:
            setattr(self.dl, obj, value)
        else:
            super().__setattr__(obj, value)

    def with_type(self, typ: str) -> DataLoader:
        """Sets the type for the DataLoader."""
        return DataLoader(self.dl.with_type(typ))

    def with_symbols(self, symbols: list[str]) -> DataLoader:
        """Sets the symbols for the DataLoader."""
        return DataLoader(self.dl.with_symbols(symbols))

    def with_start(self, start: str) -> DataLoader:
        """Sets the start date/time for the DataLoader."""
        return DataLoader(self.dl.with_start(start))

    def with_end(self, end: str) -> DataLoader:
        """Sets the end date/time for the DataLoader."""
        return DataLoader(self.dl.with_end(end))

    def with_freq(self, freq: str) -> DataLoader:
        """Sets the frequency for the DataLoader."""
        return DataLoader(self.dl.with_freq(freq))

    def with_dfs(self, dfs: list[PolarsFrame]) -> DataLoader:
        """Sets the data frames for the DataLoader."""
        return DataLoader(self.dl.with_dfs(dfs))

    def iter_dfs(self):
        """
        Iterate over just the DataFrames in the DataLoader.

        Returns:
            An iterator of the DataFrames
        """
        return iter(self.dfs)

    def collect(self, par: bool = True, inplace: bool = False) -> DataLoader:  # noqa: FBT001
        """
        Collects the data frames in the DataLoader.

        Args:
            par: A boolean indicating whether to use parallel processing.
            inplace: A boolean indicating whether to modify the DataLoader in place.
        """
        return DataLoader(self.dl.collect(par, inplace))

    def lazy(self) -> DataLoader:
        """
        Converts the data frames in the DataLoader to lazy frames.

        This method converts any eager DataFrames to LazyFrames while leaving already lazy frames unchanged.
        """
        return DataLoader(self.dl.lazy())

    def select(
        self, *exprs: IntoExpr | Iterable[IntoExpr], **named_exprs: IntoExpr
    ) -> DataLoader:
        """
        Select columns from all DataFrames/LazyFrames in this DataLoader.

        Parameters
        ----------
        *exprs
            Column(s) to select, specified as positional arguments.
            Accepts expression input. Strings are parsed as column names,
            other non-expression inputs are parsed as literals.
        **named_exprs
            Additional columns to select, specified as keyword arguments.
            The columns will be renamed to the keyword used.

        Returns
        -------
        DataLoader
            A new DataLoader containing the selected columns from all DataFrames/LazyFrames.
        """
        pyexprs = parse_into_list_of_expressions(
            *exprs, **named_exprs, __structify=structify
        )
        return DataLoader(self.dl.select(pyexprs))

    def with_columns(
        self,
        *exprs: IntoExpr | Iterable[IntoExpr],
        **named_exprs: IntoExpr,
    ) -> DataLoader:
        """
        Add columns to all DataFrames/LazyFrames in this DataLoader.

        Added columns will replace existing columns with the same name.

        Parameters
        ----------
        *exprs
            Column(s) to add, specified as positional arguments.
            Accepts expression input. Strings are parsed as column names, other
            non-expression inputs are parsed as literals.
        **named_exprs
            Additional columns to add, specified as keyword arguments.
            The columns will be renamed to the keyword used.

        Returns
        -------
        DataLoader
            A new DataLoader with the columns added to all contained DataFrames/LazyFrames.
        """
        pyexprs = parse_into_list_of_expressions(
            *exprs, **named_exprs, __structify=structify
        )
        return DataLoader(self.dl.with_columns(pyexprs))

    def drop(
        self,
        *columns: ColumnNameOrSelector | Iterable[ColumnNameOrSelector],
        strict: bool = False,
    ) -> DataLoader:
        """
        Remove columns from the DataFrame.

        Parameters
        ----------
        *columns
            Names of the columns that should be removed from the dataframe.
            Accepts column selector input.
        strict
            Validate that all column names exist in the current schema,
            and throw an exception if any do not.
        """
        pyexprs = parse_into_list_of_expressions(*columns)
        return DataLoader(self.dl.drop(pyexprs, strict=strict))

    def filter(self, expr: IntoExpr) -> DataLoader:
        """
        Filters rows in each DataFrame/LazyFrame based on a given expression.
        """
        pyexpr = parse_into_expression(expr)
        return DataLoader(self.dl.filter(pyexpr))

    def align(
        self, *on: IntoExpr | Iterable[IntoExpr], how: str | None = None
    ) -> DataLoader:
        """
        Aligns multiple DataFrames based on specified columns and join type.

        Args:
            on: A list of expressions specifying the columns to align on.
            how: inner | left | right | outer | cross, Defaults to "outer" if not provided.
        """
        on = parse_into_list_of_expressions(*on)
        return DataLoader(self.dl.align(on, how))

    def concat(self) -> LazyFrame:
        """
        Concatenates all DataFrames in the DataLoader into a single LazyFrame.

        This method performs the following operations:
        1. Iterates through all DataFrames in the DataLoader
        2. For each DataFrame, it checks if a 'symbol' column exists
        3. If 'symbol' column doesn't exist, it adds one using the symbol associated with the DataFrame
        4. Converts each DataFrame to a LazyFrame
        5. Concatenates all LazyFrames vertically
        """
        return self.dl.concat()

    def save(self, path: str | Path):
        """
        Saves the DataLoader data to a file or directory.

        Args:
            path: The path where the data should be saved. Can be a file path with extension
                 or a directory path.
        """
        self.dl.save(path)

    @classmethod
    def load(
        cls,
        path: str | Path,
        symbols: list[str] | None = None,
        lazy: bool = True,  # noqa: FBT001
    ):
        """
        Loads data from a DataLoader file or directory.

        Args:
            path: The path from where the data should be loaded
            symbols: Optional list of symbols to load
            lazy: Whether to load the data lazily
        """
        return DataLoader(_RS_Loader.load(path, symbols, lazy))

    def apply(
        self,
        func: Callable[[PolarsFrame], PolarsFrame],
        **kwargs: Any,
    ) -> DataLoader:
        """
        Applies a Python function to each DataFrame in the DataLoader.

        Args:
            func: A callable that takes a DataFrame as input and returns a DataFrame
            **kwargs: Optional keyword arguments to pass to the Python function
        """
        return DataLoader(self.dl.apply(func, **kwargs))

    def join(
        self,
        path: str | Path,
        on: IntoExpr | Iterable[IntoExpr] | None = None,
        left_on: IntoExpr | Iterable[IntoExpr] | None = None,
        right_on: IntoExpr | Iterable[IntoExpr] | None = None,
        how: str = "left",
        flag: bool = True,  # noqa: FBT001
    ) -> DataLoader:
        """
        Joins the current DataLoader with another dataset.

        Args:
            path: The path to the other dataset
            on: Optional columns to join on for both datasets
            left_on: Optional columns to join on from the left dataset
            right_on: Optional columns to join on from the right dataset
            how: The type of join to perform (left, right, inner, outer)
            flag: Whether to perform the join operation
        """
        if on is not None:
            on = parse_into_list_of_expressions(*on)
        if left_on is not None:
            left_on = parse_into_list_of_expressions(*left_on)
        if right_on is not None:
            right_on = parse_into_list_of_expressions(*right_on)
        return DataLoader(
            self.dl.join(
                path=path, on=on, left_on=left_on, right_on=right_on, how=how, flag=flag
            )
        )

    def group_by_time(
        self,
        rule: str,
        last_time: str | None = None,
        time: str = "time",
        group_by: IntoExpr | Iterable[IntoExpr] | None = None,
        daily_col: str = "trading_date",
        maintain_order: bool = True,  # noqa: FBT001
        label: str = "left",
    ) -> DataLoaderGroupBy:
        """
        Groups data by dynamic frequency.

        Args:
            rule: The grouping rule. Can be 'daily' or any rule supported by Polars.
            last_time: Optional time column name to call last method on
            time: Time column name to group by
            group_by: Optional additional columns to group by
            daily_col: Column name for daily grouping
            maintain_order: Whether to maintain original order within groups
            label: Which edge of the window to use for labels (left, right or datapoint)
        """
        if group_by is not None:
            group_by = parse_into_list_of_expressions(group_by)
        return DataLoader(
            self.dl.group_by_time(
                rule=rule,
                last_time=last_time,
                time=time,
                group_by=group_by,
                daily_col=daily_col,
                maintain_order=maintain_order,
                label=label,
            )
        )

    def group_by(
        self,
        *by: IntoExpr | Iterable[IntoExpr],
        maintain_order: bool = True,
    ) -> DataLoaderGroupBy:
        """
        Groups the data by the specified columns.

        Args:
            by: An expression or a list of expressions to group by
            maintain_order: Whether to maintain the original order within groups
        """
        by = parse_into_list_of_expressions(*by)
        return DataLoader(self.dl.group_by(by, maintain_order))

    def group_by_dynamic(
        self,
        index_column: IntoExpr,
        every: str,
        period: str | None = None,
        offset: str | None = None,
        group_by: IntoExpr | Iterable[IntoExpr] | None = None,
        label: str = "left",
        include_boundaries: bool = False,  # noqa: FBT001
        closed_window: str = "left",
        start_by: str = "window_bound",
        last_time: str | None = None,
    ) -> DataLoaderGroupBy:
        """
        Groups the data dynamically based on a time index and additional grouping expressions.

        Args:
            index_column: The expression representing the time index column
            every: The length of the window as a duration string (e.g. "1d", "2h", "30m")
            period: The period to advance the window as a duration string
            offset: The offset for the window boundaries as a duration string
            group_by: Additional expressions to group by alongside the time index
            label: Which edge of the window to use for labels
            include_boundaries: Whether to include the window boundaries in the output
            closed_window: How the window boundaries should be handled
            start_by: Strategy for determining the start of the first window
            last_time: Optional last time to consider for grouping
        """
        index_column = parse_into_expression(index_column)
        if group_by is not None:
            group_by = parse_into_list_of_expressions(group_by)
        return DataLoader(
            self.dl.group_by_dynamic(
                index_column=index_column,
                every=every,
                period=period,
                offset=offset,
                group_by=group_by,
                label=label,
                include_boundaries=include_boundaries,
                closed_window=closed_window,
                start_by=start_by,
                last_time=last_time,
            )
        )

    def kline(
        self,
        freq: str,
        tier: str | None = None,
        adjust: str | None = None,
    ) -> DataLoader:
        """
        Loads kline data based on the given options.

        Args:
            freq: The frequency of the kline data.
            tier: Optional tier level.
            adjust: Optional adjustment type.
            concat_tick_df: Whether to concatenate tick data frames.
        """
        return DataLoader(self.dl.kline(freq, tier, adjust))

    def calc_tick_future_ret(
        self,
        facs: list[str],
        c_rate: float = 0.0,
        c_rate_type: str = "absolute",
        is_signal: bool = True,  # noqa: FBT001
        init_cash: int = 10000000,
        bid: str = "bid1",
        ask: str = "ask1",
        contract_chg_signal: str | None = None,
        multiplier: float | None = None,
        signal_type: str = "absolute",
        blowup: bool = False,  # noqa: FBT001
        suffix: str = "",
    ) -> DataLoader:
        """
        Calculates tick-based future returns for the given factors.

        Args:
            facs: List of factor names.
            c_rate: Commission rate.
            c_rate_type: Commission rate type ("absolute" or "percent").
            is_signal: Whether the factors are signals.
            init_cash: Initial cash amount.
            bid: Bid price column name.
            ask: Ask price column name.
            contract_chg_signal: Optional contract change signal column.
            multiplier: Optional contract multiplier.
            signal_type: Signal type ("absolute" or "percent").
            blowup: Whether to allow blowup.
            suffix: Suffix for output columns.
        """
        return DataLoader(
            self.dl.calc_tick_future_ret(
                facs=facs,
                c_rate=c_rate,
                c_rate_type=c_rate_type,
                is_signal=is_signal,
                init_cash=init_cash,
                bid=bid,
                ask=ask,
                contract_chg_signal=contract_chg_signal,
                multiplier=multiplier,
                signal_type=signal_type,
                blowup=blowup,
                suffix=suffix,
            )
        )
