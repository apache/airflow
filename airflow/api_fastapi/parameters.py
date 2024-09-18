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
from typing import TYPE_CHECKING, Any, Generic, List, TypeVar, Union

from fastapi import Depends, HTTPException, Query
from sqlalchemy import nullslast, or_
from typing_extensions import Annotated, Self

from airflow.models.dag import DagModel, DagTag

if TYPE_CHECKING:
    from sqlalchemy.sql import ColumnElement, Select

T = TypeVar("T")


class BaseParam(Generic[T], ABC):
    """Base class for filters."""

    def __init__(self) -> None:
        self.value: T | None = None
        self.attribute: ColumnElement | None = None

    @abstractmethod
    def to_orm(self, select: Select) -> Select:
        pass

    @abstractmethod
    def __call__(self, *args: Any, **kwarg: Any) -> BaseParam:
        pass

    def set_value(self, value: T) -> Self:
        self.value = value
        return self


class _LimitFilter(BaseParam[int]):
    """Filter on the limit."""

    def to_orm(self, select: Select) -> Select:
        if self.value is None:
            return select

        return select.limit(self.value)

    def __call__(self, limit: int = 100) -> _LimitFilter:
        return self.set_value(limit)


class _OffsetFilter(BaseParam[int]):
    """Filter on offset."""

    def to_orm(self, select: Select) -> Select:
        if self.value is None:
            return select
        return select.offset(self.value)

    def __call__(self, offset: int = 0) -> _OffsetFilter:
        return self.set_value(offset)


class _PausedFilter(BaseParam[Union[bool, None]]):
    """Filter on is_paused."""

    def to_orm(self, select: Select) -> Select:
        if self.value is None:
            return select
        return select.where(DagModel.is_paused == self.value)

    def __call__(self, paused: bool | None = Query(default=None)) -> _PausedFilter:
        return self.set_value(paused)


class _OnlyActiveFilter(BaseParam[bool]):
    """Filter on is_active."""

    def to_orm(self, select: Select) -> Select:
        if self.value:
            return select.where(DagModel.is_active == self.value)
        return select

    def __call__(self, only_active: bool = Query(default=True)) -> _OnlyActiveFilter:
        return self.set_value(only_active)


class _SearchParam(BaseParam[Union[str, None]]):
    """Search on attribute."""

    def __init__(self, attribute: ColumnElement) -> None:
        super().__init__()
        self.attribute: ColumnElement = attribute

    def to_orm(self, select: Select) -> Select:
        if self.value is None:
            return select
        return select.where(self.attribute.ilike(f"%{self.value}"))


class _DagIdPatternSearch(_SearchParam):
    """Search on dag_id."""

    def __init__(self) -> None:
        super().__init__(DagModel.dag_id)

    def __call__(self, dag_id_pattern: str | None = Query(default=None)) -> _DagIdPatternSearch:
        return self.set_value(dag_id_pattern)


class _DagDisplayNamePatternSearch(_SearchParam):
    """Search on dag_display_name."""

    def __init__(self) -> None:
        super().__init__(DagModel.dag_display_name)

    def __call__(
        self, dag_display_name_pattern: str | None = Query(default=None)
    ) -> _DagDisplayNamePatternSearch:
        return self.set_value(dag_display_name_pattern)


class SortParam(BaseParam[Union[str]]):
    """Order result by the attribute."""

    def __init__(self, allowed_attrs: list[str]) -> None:
        super().__init__()
        self.allowed_attrs = allowed_attrs

    def to_orm(self, select: Select) -> Select:
        if self.value is None:
            return select

        lstriped_orderby = self.value.lstrip("-")
        if self.allowed_attrs and lstriped_orderby not in self.allowed_attrs:
            raise HTTPException(
                400,
                f"Ordering with '{lstriped_orderby}' is disallowed or "
                f"the attribute does not exist on the model",
            )
        if self.value[0] == "-":
            return select.order_by(nullslast(getattr(DagModel, lstriped_orderby).desc()), DagModel.dag_id)
        else:
            return select.order_by(nullslast(getattr(DagModel, lstriped_orderby)), DagModel.dag_id.asc())

    def __call__(self, order_by: str = Query(default="dag_id")) -> SortParam:
        return self.set_value(order_by)


class _TagsFilter(BaseParam[List[str]]):
    """Filter on tags."""

    def to_orm(self, select: Select) -> Select:
        if not self.value:
            return select

        conditions = [DagModel.tags.any(DagTag.name == tag) for tag in self.value]
        return select.where(or_(*conditions))

    def __call__(self, tags: list[str] = Query(default_factory=list)) -> _TagsFilter:
        return self.set_value(tags)


class _OwnersFilter(BaseParam[List[str]]):
    """Filter on owners."""

    def to_orm(self, select: Select) -> Select:
        if not self.value:
            return select

        conditions = [DagModel.owners.ilike(f"%{owner}%") for owner in self.value]
        return select.where(or_(*conditions))

    def __call__(self, owners: list[str] = Query(default_factory=list)) -> _OwnersFilter:
        return self.set_value(owners)


QueryLimit = Annotated[_LimitFilter, Depends(_LimitFilter())]
QueryOffset = Annotated[_OffsetFilter, Depends(_OffsetFilter())]
QueryPausedFilter = Annotated[_PausedFilter, Depends(_PausedFilter())]
QueryOnlyActiveFilter = Annotated[_OnlyActiveFilter, Depends(_OnlyActiveFilter())]
QueryDagIdPatternSearch = Annotated[_DagIdPatternSearch, Depends(_DagIdPatternSearch())]
QueryDagDisplayNamePatternSearch = Annotated[
    _DagDisplayNamePatternSearch, Depends(_DagDisplayNamePatternSearch())
]
QueryTagsFilter = Annotated[_TagsFilter, Depends(_TagsFilter())]
QueryOwnersFilter = Annotated[_OwnersFilter, Depends(_OwnersFilter())]
