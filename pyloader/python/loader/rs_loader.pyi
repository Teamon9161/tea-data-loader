"""
A Python wrapper around the Rust `DataLoader` struct.

This struct provides a Python interface to the underlying Rust `DataLoader`,
allowing Python code to interact with the data loading and processing functionality.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Callable, overload

from polars import DataFrame, DataType, Expr, LazyFrame

from . import DataLoader

class _RS_Loader:
    """
    A class representing a data loader for financial time series data.

    This class holds various pieces of information related to the loaded data,
    including the data frames, symbols, time range, and other metadata.
    """

    @overload
    def __new__(cls, typ: str, symbols: list[str] | None = None) -> _RS_Loader:
        """
        Creates a new DataLoader instance with the specified type and symbols.

        Args:
            typ: A string that holds the type of data.
            symbols: An optional list of symbols.
        """

    @overload
    def __new__(
        cls, typ: list[DataFrame | LazyFrame], symbols: list[str] | None = None
    ) -> _RS_Loader:
        """
        Creates a new DataLoader instance from the provided data frames.

        Args:
            typ: A list of data frames.
            symbols: An optional list of symbols.
        """

    @staticmethod
    def from_dfs(dfs: list[DataFrame | LazyFrame]) -> _RS_Loader:
        """
        Creates a new DataLoader instance from the provided data frames.

        Args:
            dfs: A list of data frames.

        Returns:
            A new DataLoader instance with the provided data frames.
        """

    def len(self) -> int:
        """
        Returns the number of data frames in the DataLoader.
        """

    def is_empty(self) -> bool:
        """
        Returns `true` if the DataLoader contains no data frames.
        """

    def is_lazy(self) -> bool:
        """
        Checks if the DataLoader is lazy.

        This method determines if the data loading is lazy by checking the first data frame.
        If the DataLoader is empty, it is considered not lazy.

        Returns:
            `true` if the DataLoader is lazy, `false` otherwise.
        """

    def is_eager(self) -> bool:
        """
        Returns `true` if the DataLoader is eager.
        """

    def with_type(self, typ: str) -> _RS_Loader:
        """
        Sets the type for the DataLoader.

        Args:
            typ: A string representing the type.

        Returns:
            The modified DataLoader instance.
        """

    def with_start(self, start: str) -> _RS_Loader:
        """
        Sets the start date/time for the DataLoader.

        Args:
            start: A string representing the start date/time.

        Returns:
            The modified DataLoader instance.
        """

    def with_end(self, end: str) -> _RS_Loader:
        """
        Sets the end date/time for the DataLoader.

        Args:
            end: A string representing the end date/time.

        Returns:
            The modified DataLoader instance.
        """

    def with_symbols(self, symbols: list[str]) -> _RS_Loader:
        """
        Sets the symbols for the DataLoader.

        Args:
            symbols: A list of symbols.

        Returns:
            The modified DataLoader instance.
        """

    def with_freq(self, freq: str) -> _RS_Loader:
        """
        Sets the frequency for the DataLoader.

        Args:
            freq: A string representing the frequency.

        Returns:
            The modified DataLoader instance.
        """

    def with_dfs(self, dfs: list[DataFrame | LazyFrame]) -> _RS_Loader:
        """
        Sets the data frames for the DataLoader.

        Args:
            dfs: A list of data frames.

        Returns:
            The modified DataLoader instance.
        """

    @property
    def symbols(self) -> list[str] | None:
        """
        Returns a list of symbols if present in the DataLoader.
        """

    @property
    def dfs(self) -> list[DataFrame | LazyFrame]:
        """
        Returns the list of data frames.
        """

    @property
    def type(self) -> str:
        """
        Returns the type of data, such as future, bond, stock, etc.
        """

    @property
    def freq(self) -> str | None:
        """
        Returns the frequency of the data, such as "1d" for daily, "1h" for hourly, etc.
        """

    def __getitem__(self, idx: int | str) -> DataFrame | LazyFrame: ...
    def __setitem__(self, idx: int | str, df: DataFrame | LazyFrame) -> None: ...
    def __repr__(self) -> str: ...
    def collect(self, par: bool = True, inplace: bool = False) -> _RS_Loader:  # noqa: FBT001
        """
        Collects the data frames in the DataLoader.

        Args:
            par: A boolean indicating whether to use parallel processing.
            inplace: A boolean indicating whether to modify the DataLoader in place.

        Returns:
            The modified DataLoader instance.
        """

    def lazy(self) -> _RS_Loader:
        """
        Converts the data frames in the DataLoader to lazy frames.

        This method converts any eager DataFrames to LazyFrames while leaving already lazy frames unchanged.

        Returns:
            The modified DataLoader instance with all frames converted to LazyFrames.
        """

    def kline(
        self,
        freq: str,
        tier: str | None = None,
        adjust: str | None = None,
        concat_tick_df: bool = False,  # noqa: FBT001
    ) -> _RS_Loader:
        """
        Loads kline data based on the given options.

        Args:
            freq: The frequency of the kline data.
            tier: Optional tier level.
            adjust: Optional adjustment type.
            concat_tick_df: Whether to concatenate tick data frames.

        Returns:
            The modified DataLoader instance.
        """

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
    ) -> _RS_Loader:
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

        Returns:
            The modified DataLoader instance.
        """

    def with_column(self, expr: Expr) -> _RS_Loader:
        """
        Adds a new column to each DataFrame in the DataLoader.

        Args:
            expr: The expression defining the new column.

        Returns:
            The modified DataLoader instance.
        """

    def with_columns(self, exprs: list[Expr]) -> _RS_Loader:
        """
        Adds multiple new columns to each DataFrame in the DataLoader.

        Args:
            exprs: A list of expressions defining the new columns.

        Returns:
            The modified DataLoader instance.
        """

    def select(self, exprs: list[Expr]) -> _RS_Loader:
        """
        Selects specific columns from each DataFrame in the DataLoader.

        Args:
            exprs: A list of expressions defining the columns to select.

        Returns:
            The modified DataLoader instance.
        """

    def filter(self, expr: Expr) -> _RS_Loader:
        """
        Filters rows in each DataFrame of the DataLoader based on a given expression.

        Args:
            expr: The filter expression.

        Returns:
            The modified DataLoader instance.
        """

    def drop(self, columns: list[Expr], strict: bool = False) -> _RS_Loader:  # noqa: FBT001
        """
        Drops specified columns from each DataFrame in the DataLoader.

        Args:
            columns: A list of expressions specifying the columns to drop
            strict: If true, raises an error if any specified column doesn't exist.
                    If false, silently ignores non-existent columns.

        Returns:
            The modified DataLoader instance.
        """

    def align(self, on: list[Expr], how: str | None = None) -> _RS_Loader:
        """
        Aligns multiple DataFrames based on specified columns and join type. similar to `polars.align_frames`.

        This method aligns the DataFrames in the DataLoader by performing a series of joins
        on the specified columns. It creates a master alignment frame and then extracts
        individual aligned frames from it.

        Args:
            on: A list of expressions specifying the columns to align on.
            how: inner | left | right | outer | cross, Defaults to "outer" if not provided.

        Returns:
            The modified DataLoader instance with aligned DataFrames.

        Notes:
            - If the DataLoader is empty, it returns the original instance.
            - For large numbers of frames, it may need to collect eagerly to avoid stack overflow.
            - The method sorts the resulting frames based on the alignment columns.
        """

    def group_by_time(
        self,
        rule: str,
        last_time: str | None = None,
        time: str = "time",
        group_by: list[Expr] | None = None,
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

        Returns:
            A DataLoaderGroupBy containing the grouped data.
        """

    def group_by(
        self,
        by: list[Expr],
        maintain_order: bool = True,  # noqa: FBT001
    ) -> DataLoaderGroupBy:
        """
        Groups the data by the specified columns.

        Args:
            by: An expression or a list of expressions to group by
            maintain_order: Whether to maintain the original order within groups

        Returns:
            A DataLoaderGroupBy instance representing the grouped data.
        """

    def group_by_dynamic(
        self,
        index_column: Expr,
        every: str,
        period: str | None = None,
        offset: str | None = None,
        group_by: list[Expr] | None = None,
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
            period: The period to advance the window as a duration string. Defaults to every if not specified
            offset: The offset for the window boundaries as a duration string. Defaults to 0
            group_by: Additional expressions to group by alongside the time index
            label: Which edge of the window to use for labels (left, right or datapoint)
            include_boundaries: Whether to include the window boundaries in the output
            closed_window: How the window boundaries should be handled (left, right, both or none)
            start_by: Strategy for determining the start of the first window
            last_time: Optional last time to consider for grouping

        Returns:
            A DataLoaderGroupBy instance representing the dynamically grouped data.
        """

    def save(self, path: str | Path) -> None:
        """
        Saves the DataLoader data to a file or directory.

        Args:
            path: The path where the data should be saved. Can be a file path with extension
                 or a directory path. If a directory path is provided, data will be saved
                 in IPC (Arrow IPC) format.
        """

    @classmethod
    def load(
        cls,
        path: str | Path,
        symbols: list[str] | None = None,
        lazy: bool = True,  # noqa: FBT001
    ) -> _RS_Loader:
        """
        Loads data from a DataLoader file or directory.

        Args:
            path: The path from where the data should be loaded. Can be a file path or directory path.
            symbols: Optional list of symbols to load. If provided, only loads data for these symbols.
                    Only applicable when loading from a directory.
            lazy: Whether to load the data lazily. Defaults to true.

        Returns:
            A new DataLoader instance containing the loaded data.
        """

    def concat(self) -> LazyFrame:
        """
        Concatenates all DataFrames in the DataLoader into a single LazyFrame.

        This method performs the following operations:
        1. Iterates through all DataFrames in the DataLoader
        2. For each DataFrame, it checks if a 'symbol' column exists
        3. If 'symbol' column doesn't exist, it adds one using the symbol associated with the DataFrame
        4. Converts each DataFrame to a LazyFrame
        5. Concatenates all LazyFrames vertically

        Returns:
            A LazyFrame containing the concatenated data from all DataFrames in the DataLoader.
        """

    def join(
        self,
        path: str | Path,
        on: list[Expr] | None = None,
        left_on: list[Expr] | None = None,
        right_on: list[Expr] | None = None,
        how: str = "left",
        flag: bool = True,  # noqa: FBT001
    ) -> _RS_Loader:
        """
        Joins the current DataLoader with another dataset.

        This method performs a join operation between the current DataLoader and another dataset.
        It supports various join types and options.

        Args:
            path: The path to the other dataset.
            on: Optional columns to join on for both datasets. Cannot be used with left_on/right_on.
            left_on: Optional columns to join on from the left (current) dataset. Required if `on` not provided.
            right_on: Optional columns to join on from the right (other) dataset. Required if `on` not provided.
            how: The type of join to perform (left, right, inner, outer). Defaults to left join.
            flag: Whether to perform the join operation. Defaults to true.

        Returns:
            The DataLoader instance containing the joined data.
        """

    def apply(
        self,
        func: Callable[[DataFrame | LazyFrame], DataFrame | LazyFrame],
        **kwargs: Any,
    ) -> _RS_Loader:
        """
        Applies a Python function to each DataFrame in the DataLoader.

        Args:
            func: A callable that takes a DataFrame as input and returns a DataFrame
            **kwargs: Optional keyword arguments to pass to the Python function

        Returns:
            The transformed DataLoader instance.
        """

    def schema(self) -> dict[str, DataType]:
        """
        Returns the schema of the first data frame in the DataLoader.

        Returns:
            The schema of the first data frame. If the DataLoader is empty,
            returns an empty schema.

        Raises:
            PyValueError: If there is an error getting the schema.
        """

    def columns(self) -> list[str]:
        """
        Returns a list of column names from the first data frame in the DataLoader.

        Returns:
            A list of column names as strings.

        Raises:
            PyValueError: If there is an error getting the schema.
        """

    def find_index(self, symbol: str) -> int | None:
        """
        Finds the index of a given symbol in the DataLoader's symbols list.

        Args:
            symbol: The symbol name to search for

        Returns:
            The index position of the symbol if found, None if the symbol is not found
            or if the DataLoader has no symbols list.
        """

class DataLoaderGroupBy:
    """
    A class representing grouped data from a DataLoader.
    """

    @property
    def dl(self) -> DataLoader:
        """The underlying DataLoader"""

    @property
    def last_time(self) -> str | None:
        """The last time column name if present"""

    @property
    def time(self) -> str | None:
        """The time column name if present"""

    def agg(self, aggs: list[Expr]) -> DataLoader:
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
