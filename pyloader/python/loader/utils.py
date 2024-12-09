from __future__ import annotations

from polars import when
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from polars import Expr
    from polars._typing import IntoExprColumn, IntoExpr

def iif(cond: IntoExprColumn, then: IntoExpr, otherwise: IntoExpr) -> Expr:
    return when(cond).then(then).otherwise(otherwise)
