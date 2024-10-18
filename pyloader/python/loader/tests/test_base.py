import os

import polars as pl
from loader import DataLoader

os.environ["RUST_BACKTRACE"] = "1"


def test_base():
    dl = DataLoader("future").kline("min")
    dl[0]
