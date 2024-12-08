from datetime import date

import polars as pl
from loader import DataLoader

test_df = pl.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})


def test_base():
    dl = DataLoader(pl.DataFrame(), ["a"])
    dl.type = "test"
    dl.start = "2024-01-01"
    assert dl.len() == len(dl) == 1
    dl2 = dl.with_type("test2").with_start("2024-01-02").with_end("2024-01-03")
    assert dl2.type == "test2"
    assert dl2.start.date() == date(2024, 1, 2)
    assert dl2.end.date() == date(2024, 1, 3)
    assert dl.type == "test"
    assert dl.start.date() == date(2024, 1, 1)
    assert dl.end is None
    assert dl[0].is_empty()
    assert dl["a"].is_empty()
    dl[0] = test_df
    assert dl[0].shape == (3, 2)
    # Setting a frame directly to a copy of the internal dataframe list doesn't work
    # TODO(Teamon): Can we add a warning message for this case? maybe hard because we
    # return a list of dataframes
    dl.dfs[0] = pl.DataFrame()
    assert dl[0].shape == (3, 2)
    dl["a"] = pl.LazyFrame()
    dl["b"] = pl.LazyFrame()
    dl["a"] = pl.LazyFrame()
    assert dl["a"].collect().is_empty()
    dl = dl.collect().lazy()
    assert dl.is_lazy()
    # test collect inplace
    dl.collect(inplace=True)
    assert dl.is_eager()
    # test drop
    dl = DataLoader(test_df).drop("a", "b", "c")
    assert len(dl.columns) == 0
