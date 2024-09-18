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
from typing import Any, Generic, List, TypeVar, Union

from fastapi import Depends, HTTPException, Query as FastAPIQuery
from sqlalchemy import or_
from sqlalchemy.orm import Query
from sqlalchemy.sql import ColumnElement
from typing_extensions import Annotated, Self

from airflow.models.dag import DagModel, DagTag

T = TypeVar("T")


class BaseParam(Generic[T], ABC):
    """Base class for filters."""

    def __init__(self) -> None:
        self.value: T | None = None
        self.attribute: ColumnElement | None = None

    @abstractmethod
    def to_orm(self, query: Query) -> Query:
        pass

    @abstractmethod
    def __call__(self, *args: Any, **kwarg: Any) -> BaseParam:
        pass

    def set_value(self, value: T) -> Self:
        self.value = value
        return self


class _LimitFilter(BaseParam[int]):
    """Filter on the limit."""

    def to_orm(self, query: Query) -> Query:
        if self.value is None:
            return query

        return query.limit(self.value)

    def __call__(self, limit: int = 100) -> _LimitFilter:
        self.value = limit

        return self


class _OffsetFilter(BaseParam[int]):
    """Filter on offset."""

    def to_orm(self, query: Query) -> Query:
        if self.value is None:
            return query
        return query.offset(self.value)

    def __call__(self, offset: int = 0) -> _OffsetFilter:
        self.value = offset

        return self


class _PausedFilter(BaseParam[Union[bool, None]]):
    """Filter on is_paused."""

    def to_orm(self, query: Query) -> Query:
        if self.value is None:
            return query
        return query.where(DagModel.is_paused == self.value)

    def __call__(self, paused: bool | None = FastAPIQuery(default=None)) -> _PausedFilter:
        return self.set_value(paused)


class _OnlyActiveFilter(BaseParam[bool]):
    """Filter on is_active."""

    def to_orm(self, query: Query) -> Query:
        if self.value:
            return query.where(DagModel.is_active == self.value)
        return query

    def __call__(self, only_active: bool = FastAPIQuery(default=True)) -> _OnlyActiveFilter:
        return self.set_value(only_active)


class _DagIdPatternSearch(BaseParam[Union[str, None]]):
    """Search on dag_id."""

    def to_orm(self, query: Query) -> Query:
        if self.value is None:
            return query
        return query.where(DagModel.dag_id.ilike(f"%{self.value}"))

    def __call__(self, dag_id_pattern: str | None = FastAPIQuery(default=None)) -> _DagIdPatternSearch:
        return self.set_value(dag_id_pattern)


class SortParam(BaseParam[Union[str]]):
    """Order result by the attribute."""

    def __init__(self, allowed_attrs: list[str]) -> None:
        super().__init__()
        self.allowed_attrs = allowed_attrs

    def to_orm(self, query: Query) -> Query:
        if self.value is None:
            return query

        lstriped_orderby = self.value.lstrip("-")
        if self.allowed_attrs and lstriped_orderby not in self.allowed_attrs:
            raise HTTPException(
                400,
                f"Ordering with '{lstriped_orderby}' is disallowed or "
                f"the attribute does not exist on the model",
            )
        if self.value[0] == "-":
            return query.order_by(getattr(DagModel, lstriped_orderby).desc())
        else:
            return query.order_by(getattr(DagModel, lstriped_orderby).asc())
        return query

    def __call__(self, order_by: str = FastAPIQuery(default="dag_id")) -> SortParam:
        return self.set_value(order_by)


class _TagsFilter(BaseParam[List[str]]):
    """Filter on tags."""

    def to_orm(self, query: Query) -> Query:
        if self.value:
            conditions = [DagModel.tags.any(DagTag.name == tag) for tag in self.value]
            return query.where(or_(*conditions))

        return query

    def __call__(self, tags: list[str] = FastAPIQuery(default_factory=list)) -> _TagsFilter:
        return self.set_value(tags)


QueryLimit = Annotated[_LimitFilter, Depends(_LimitFilter())]
QueryOffset = Annotated[_OffsetFilter, Depends(_OffsetFilter())]
QueryPausedFilter = Annotated[_PausedFilter, Depends(_PausedFilter())]
QueryOnlyActiveFilter = Annotated[_OnlyActiveFilter, Depends(_OnlyActiveFilter())]
QueryDagIdPatternSearch = Annotated[_DagIdPatternSearch, Depends(_DagIdPatternSearch())]
QueryTagsFilter = Annotated[_TagsFilter, Depends(_TagsFilter())]
