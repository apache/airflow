# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import (
    TYPE_CHECKING,
    Annotated,
    Any,
    TypeVar,
)

from fastapi import Depends
from pydantic import NonNegativeInt
from sqlalchemy import String
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.sql.functions import FunctionElement

from airflow.api_fastapi.core_api.base import OrmClause
from airflow.configuration import conf
from airflow.typing_compat import Self

if TYPE_CHECKING:
    from sqlalchemy.orm.attributes import InstrumentedAttribute
    from sqlalchemy.sql import ColumnElement, Select
    from sqlalchemy.sql.compiler import SQLCompiler


T = TypeVar("T")


_FALLBACK_PAGE_LIMIT: int = conf.getint("api", "fallback_page_limit")


class _MySQLCollate(FunctionElement):
    """
    Wraps a SQL expression so that on MySQL it is emitted with an explicit ``COLLATE`` clause.

    On every other dialect the expression is passed through unchanged.

    This is needed when a computed expression (e.g. a ``CASE … END`` that mixes
    a stored ``VARCHAR`` column with a ``CAST(integer AS CHAR)``) ends up with
    MySQL coercibility ``NONE`` because the two branches carry different implicit
    collations.  Comparing such an expression with a bound parameter fails with
    "Illegal mix of collations".  Wrapping the expression in an explicit
    ``COLLATE`` gives it ``EXPLICIT`` coercibility, which MySQL accepts in all
    comparison operators.
    """

    type = String()
    inherit_cache = True

    def __init__(self, expr: ColumnElement[Any], collation: str) -> None:
        super().__init__(expr)
        self.collation = collation


@compiles(_MySQLCollate)
def _compile_mysql_collate_default(element: _MySQLCollate, compiler: SQLCompiler, **kw: Any) -> str:
    """Non-MySQL: render the inner expression without any COLLATE clause."""
    (expr,) = element.clauses
    return compiler.process(expr, **kw)


@compiles(_MySQLCollate, "mysql")
def _compile_mysql_collate_mysql(element: _MySQLCollate, compiler: SQLCompiler, **kw: Any) -> str:
    """MySQL: wrap the inner expression with the requested COLLATE clause."""
    (expr,) = element.clauses
    inner = compiler.process(expr, **kw)
    return f"({inner}) COLLATE {element.collation}"


class BaseParam(OrmClause[T], ABC):
    """Base class for path or query parameters with ORM transformation."""

    def __init__(self, value: T | None = None, skip_none: bool = True) -> None:
        super().__init__(value)
        self.attribute: ColumnElement | InstrumentedAttribute | None = None
        self.skip_none = skip_none

    def set_value(self, value: T | None) -> Self:
        self.value = value
        return self

    @classmethod
    @abstractmethod
    def depends(cls, *args: Any, **kwargs: Any) -> Self:
        pass


class LimitFilter(BaseParam[NonNegativeInt]):
    """Filter on the limit."""

    def to_orm(self, select: Select) -> Select:
        if self.value is None and self.skip_none:
            return select

        return select.limit(self.value)

    @classmethod
    def depends(cls, limit: NonNegativeInt = _FALLBACK_PAGE_LIMIT) -> LimitFilter:
        return cls().set_value(min(limit, conf.getint("api", "maximum_page_limit")))


class OffsetFilter(BaseParam[NonNegativeInt]):
    """Filter on offset."""

    def to_orm(self, select: Select) -> Select:
        if self.value is None and self.skip_none:
            return select
        return select.offset(self.value)

    @classmethod
    def depends(
        cls,
        offset: NonNegativeInt = 0,
    ) -> OffsetFilter:
        return cls().set_value(offset)


QueryLimit = Annotated[LimitFilter, Depends(LimitFilter.depends)]

QueryOffset = Annotated[OffsetFilter, Depends(OffsetFilter.depends)]
