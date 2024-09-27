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
from typing import TYPE_CHECKING, Any, Generic, List, TypeVar

from fastapi import Depends, HTTPException, Query
from sqlalchemy import case, or_
from typing_extensions import Annotated, Self

from airflow.models.dag import DagModel, DagTag
from airflow.models.dagrun import DagRun
from airflow.utils.state import DagRunState

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

    def set_value(self, value: T | None) -> Self:
        self.value = value
        return self

    @abstractmethod
    def depends(self, *args: Any, **kwargs: Any) -> Self:
        pass


class _LimitFilter(BaseParam[int]):
    """Filter on the limit."""

    def to_orm(self, select: Select) -> Select:
        if self.value is None:
            return select

        return select.limit(self.value)

    def depends(self, limit: int = 100) -> _LimitFilter:
        return self.set_value(limit)


class _OffsetFilter(BaseParam[int]):
    """Filter on offset."""

    def to_orm(self, select: Select) -> Select:
        if self.value is None:
            return select
        return select.offset(self.value)

    def depends(self, offset: int = 0) -> _OffsetFilter:
        return self.set_value(offset)


class _PausedFilter(BaseParam[bool]):
    """Filter on is_paused."""

    def to_orm(self, select: Select) -> Select:
        if self.value is None:
            return select
        return select.where(DagModel.is_paused == self.value)

    def depends(self, paused: bool | None = None) -> _PausedFilter:
        return self.set_value(paused)


class _OnlyActiveFilter(BaseParam[bool]):
    """Filter on is_active."""

    def to_orm(self, select: Select) -> Select:
        if self.value:
            return select.where(DagModel.is_active == self.value)
        return select

    def depends(self, only_active: bool = True) -> _OnlyActiveFilter:
        return self.set_value(only_active)


class _SearchParam(BaseParam[str]):
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

    def depends(self, dag_id_pattern: str | None = None) -> _DagIdPatternSearch:
        return self.set_value(dag_id_pattern)


class _DagDisplayNamePatternSearch(_SearchParam):
    """Search on dag_display_name."""

    def __init__(self) -> None:
        super().__init__(DagModel.dag_display_name)

    def depends(self, dag_display_name_pattern: str | None = None) -> _DagDisplayNamePatternSearch:
        return self.set_value(dag_display_name_pattern)


class SortParam(BaseParam[str]):
    """Order result by the attribute."""

    attr_mapping = {
        "last_run_state": DagRun.state,
        "last_run_start_date": DagRun.start_date,
    }

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

        column = self.attr_mapping.get(lstriped_orderby, None) or getattr(DagModel, lstriped_orderby)

        # MySQL does not support `nullslast`, and True/False ordering depends on the
        # database implementation.
        nullscheck = case((column.isnot(None), 0), else_=1)
        if self.value[0] == "-":
            return select.order_by(nullscheck, column.desc(), DagModel.dag_id.desc())
        else:
            return select.order_by(nullscheck, column.asc(), DagModel.dag_id.asc())

    def depends(self, order_by: str = "dag_id") -> SortParam:
        return self.set_value(order_by)


class _TagsFilter(BaseParam[List[str]]):
    """Filter on tags."""

    def to_orm(self, select: Select) -> Select:
        if not self.value:
            return select

        conditions = [DagModel.tags.any(DagTag.name == tag) for tag in self.value]
        return select.where(or_(*conditions))

    def depends(self, tags: list[str] = Query(default_factory=list)) -> _TagsFilter:
        return self.set_value(tags)


class _OwnersFilter(BaseParam[List[str]]):
    """Filter on owners."""

    def to_orm(self, select: Select) -> Select:
        if not self.value:
            return select

        conditions = [DagModel.owners.ilike(f"%{owner}%") for owner in self.value]
        return select.where(or_(*conditions))

    def depends(self, owners: list[str] = Query(default_factory=list)) -> _OwnersFilter:
        return self.set_value(owners)


class _LastDagRunStateFilter(BaseParam[DagRunState]):
    """Filter on the state of the latest DagRun."""

    def to_orm(self, select: Select) -> Select:
        if self.value is None:
            return select
        return select.where(DagRun.state == self.value)

    def depends(self, last_dag_run_state: DagRunState | None = None) -> _LastDagRunStateFilter:
        return self.set_value(last_dag_run_state)


# DAG
QueryLimit = Annotated[_LimitFilter, Depends(_LimitFilter().depends)]
QueryOffset = Annotated[_OffsetFilter, Depends(_OffsetFilter().depends)]
QueryPausedFilter = Annotated[_PausedFilter, Depends(_PausedFilter().depends)]
QueryOnlyActiveFilter = Annotated[_OnlyActiveFilter, Depends(_OnlyActiveFilter().depends)]
QueryDagIdPatternSearch = Annotated[_DagIdPatternSearch, Depends(_DagIdPatternSearch().depends)]
QueryDagDisplayNamePatternSearch = Annotated[
    _DagDisplayNamePatternSearch, Depends(_DagDisplayNamePatternSearch().depends)
]
QueryTagsFilter = Annotated[_TagsFilter, Depends(_TagsFilter().depends)]
QueryOwnersFilter = Annotated[_OwnersFilter, Depends(_OwnersFilter().depends)]
# DagRun
QueryLastDagRunStateFilter = Annotated[_LastDagRunStateFilter, Depends(_LastDagRunStateFilter().depends)]
