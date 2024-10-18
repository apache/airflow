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
from datetime import datetime
from typing import TYPE_CHECKING, Any, Callable, Generic, List, Literal, TypeVar

from fastapi import Depends, HTTPException, Query
from pendulum.parsing.exceptions import ParserError
from pydantic import AfterValidator
from sqlalchemy import Column, case, or_
from sqlalchemy.inspection import inspect
from typing_extensions import Annotated, Self

from airflow.models import Base, Connection
from airflow.models.dag import DagModel, DagTag
from airflow.models.dagrun import DagRun
from airflow.utils import timezone
from airflow.utils.state import DagRunState

if TYPE_CHECKING:
    from sqlalchemy.sql import ColumnElement, Select

T = TypeVar("T")


class BaseParam(Generic[T], ABC):
    """Base class for filters."""

    def __init__(self, skip_none: bool = True) -> None:
        self.value: T | None = None
        self.attribute: ColumnElement | None = None
        self.skip_none = skip_none

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
        if self.value is None and self.skip_none:
            return select

        return select.limit(self.value)

    def depends(self, limit: int = 100) -> _LimitFilter:
        return self.set_value(limit)


class _OffsetFilter(BaseParam[int]):
    """Filter on offset."""

    def to_orm(self, select: Select) -> Select:
        if self.value is None and self.skip_none:
            return select
        return select.offset(self.value)

    def depends(self, offset: int = 0) -> _OffsetFilter:
        return self.set_value(offset)


class _PausedFilter(BaseParam[bool]):
    """Filter on is_paused."""

    def to_orm(self, select: Select) -> Select:
        if self.value is None and self.skip_none:
            return select
        return select.where(DagModel.is_paused == self.value)

    def depends(self, paused: bool | None = None) -> _PausedFilter:
        return self.set_value(paused)


class _OnlyActiveFilter(BaseParam[bool]):
    """Filter on is_active."""

    def to_orm(self, select: Select) -> Select:
        if self.value and self.skip_none:
            return select.where(DagModel.is_active == self.value)
        return select

    def depends(self, only_active: bool = True) -> _OnlyActiveFilter:
        return self.set_value(only_active)


class _SearchParam(BaseParam[str]):
    """Search on attribute."""

    def __init__(self, attribute: ColumnElement, skip_none: bool = True) -> None:
        super().__init__(skip_none)
        self.attribute: ColumnElement = attribute

    def to_orm(self, select: Select) -> Select:
        if self.value is None and self.skip_none:
            return select
        return select.where(self.attribute.ilike(f"%{self.value}"))

    def transform_aliases(self, value: str | None) -> str | None:
        if value == "~":
            value = "%"
        return value


class _OrderByParam(BaseParam[str]):
    """Order result by specified attribute ascending or descending."""

    def __init__(self, attribute: ColumnElement, skip_none: bool = True) -> None:
        super().__init__(skip_none)
        self.attribute: ColumnElement = attribute
        self.value: Literal["asc", "desc"] | None = None

    def to_orm(self, select: Select) -> Select:
        if self.value is None and self.skip_none:
            return select
        asc_stmt = select.order_by(self.attribute.asc())
        if self.value is None:
            return asc_stmt
        return asc_stmt if self.value == "asc" else select.order_by(self.attribute.desc())

    def depends(self, order_by: str = "asc") -> _OrderByParam:
        return self.set_value(order_by)


class _DagIdPatternSearch(_SearchParam):
    """Search on dag_id."""

    def __init__(self, skip_none: bool = True) -> None:
        super().__init__(DagModel.dag_id, skip_none)

    def depends(self, dag_id_pattern: str | None = None) -> _DagIdPatternSearch:
        dag_id_pattern = super().transform_aliases(dag_id_pattern)
        return self.set_value(dag_id_pattern)


class _DagDisplayNamePatternSearch(_SearchParam):
    """Search on dag_display_name."""

    def __init__(self, skip_none: bool = True) -> None:
        super().__init__(DagModel.dag_display_name, skip_none)

    def depends(self, dag_display_name_pattern: str | None = None) -> _DagDisplayNamePatternSearch:
        dag_display_name_pattern = super().transform_aliases(dag_display_name_pattern)
        return self.set_value(dag_display_name_pattern)


class SortParam(BaseParam[str]):
    """Order result by the attribute."""

    attr_mapping = {
        "last_run_state": DagRun.state,
        "last_run_start_date": DagRun.start_date,
        "connection_id": Connection.conn_id,
    }

    def __init__(
        self,
        allowed_attrs: list[str],
        model: Base,
    ) -> None:
        super().__init__()
        self.allowed_attrs = allowed_attrs
        self.model = model

    def to_orm(self, select: Select) -> Select:
        if self.skip_none is False:
            raise ValueError(f"Cannot set 'skip_none' to False on a {type(self)}")

        if self.value is None:
            return select

        lstriped_orderby = self.value.lstrip("-")
        if self.allowed_attrs and lstriped_orderby not in self.allowed_attrs:
            raise HTTPException(
                400,
                f"Ordering with '{lstriped_orderby}' is disallowed or "
                f"the attribute does not exist on the model",
            )

        column: Column = self.attr_mapping.get(lstriped_orderby, None) or getattr(
            self.model, lstriped_orderby
        )

        # MySQL does not support `nullslast`, and True/False ordering depends on the
        # database implementation.
        nullscheck = case((column.isnot(None), 0), else_=1)

        # Reset default sorting
        select = select.order_by(None)

        primary_key_column = self.get_primary_key_column()

        if self.value[0] == "-":
            return select.order_by(nullscheck, column.desc(), primary_key_column)
        else:
            return select.order_by(nullscheck, column.asc(), primary_key_column)

    def get_primary_key_column(self) -> Column:
        """Get the primary key column of the model of SortParam object."""
        return inspect(self.model).primary_key[0]

    def get_primary_key_string(self) -> str:
        """Get the primary key string of the model of SortParam object."""
        return self.get_primary_key_column().name

    def depends(self, *args: Any, **kwargs: Any) -> Self:
        raise NotImplementedError("Use dynamic_depends, depends not implemented.")

    def dynamic_depends(self) -> Callable:
        def inner(order_by: str = self.get_primary_key_string()) -> SortParam:
            return self.set_value(self.get_primary_key_string() if order_by == "" else order_by)

        return inner


class _TagsFilter(BaseParam[List[str]]):
    """Filter on tags."""

    def to_orm(self, select: Select) -> Select:
        if self.skip_none is False:
            raise ValueError(f"Cannot set 'skip_none' to False on a {type(self)}")

        if not self.value:
            return select

        conditions = [DagModel.tags.any(DagTag.name == tag) for tag in self.value]
        return select.where(or_(*conditions))

    def depends(self, tags: list[str] = Query(default_factory=list)) -> _TagsFilter:
        return self.set_value(tags)


class _OwnersFilter(BaseParam[List[str]]):
    """Filter on owners."""

    def to_orm(self, select: Select) -> Select:
        if self.skip_none is False:
            raise ValueError(f"Cannot set 'skip_none' to False on a {type(self)}")

        if not self.value:
            return select

        conditions = [DagModel.owners.ilike(f"%{owner}%") for owner in self.value]
        return select.where(or_(*conditions))

    def depends(self, owners: list[str] = Query(default_factory=list)) -> _OwnersFilter:
        return self.set_value(owners)


class _LastDagRunStateFilter(BaseParam[DagRunState]):
    """Filter on the state of the latest DagRun."""

    def to_orm(self, select: Select) -> Select:
        if self.value is None and self.skip_none:
            return select
        return select.where(DagRun.state == self.value)

    def depends(self, last_dag_run_state: DagRunState | None = None) -> _LastDagRunStateFilter:
        return self.set_value(last_dag_run_state)


class _DagTagNamePatternSearch(_SearchParam):
    """Search on dag_tag.name."""

    def __init__(self, skip_none: bool = True) -> None:
        super().__init__(DagTag.name, skip_none)

    def depends(self, tag_name_pattern: str | None = None) -> _DagTagNamePatternSearch:
        tag_name_pattern = super().transform_aliases(tag_name_pattern)
        return self.set_value(tag_name_pattern)


def _safe_parse_datetime(date_to_check: str) -> datetime:
    """
    Parse datetime and raise error for invalid dates.

    :param date_to_check: the string value to be parsed
    """
    if not date_to_check:
        raise ValueError(f"{date_to_check} cannot be None.")
    try:
        return timezone.parse(date_to_check, strict=True)
    except (TypeError, ParserError):
        raise HTTPException(
            400, f"Invalid datetime: {date_to_check!r}. Please check the date parameter have this value."
        )


# Common Safe DateTime
DateTimeQuery = Annotated[str, AfterValidator(_safe_parse_datetime)]
# DAG
QueryLimit = Annotated[_LimitFilter, Depends(_LimitFilter().depends)]
QueryOffset = Annotated[_OffsetFilter, Depends(_OffsetFilter().depends)]
QueryPausedFilter = Annotated[_PausedFilter, Depends(_PausedFilter().depends)]
QueryOnlyActiveFilter = Annotated[_OnlyActiveFilter, Depends(_OnlyActiveFilter().depends)]
QueryDagIdPatternSearch = Annotated[_DagIdPatternSearch, Depends(_DagIdPatternSearch().depends)]
QueryDagDisplayNamePatternSearch = Annotated[
    _DagDisplayNamePatternSearch, Depends(_DagDisplayNamePatternSearch().depends)
]
QueryDagIdPatternSearchWithNone = Annotated[
    _DagIdPatternSearch, Depends(_DagIdPatternSearch(skip_none=False).depends)
]
QueryTagsFilter = Annotated[_TagsFilter, Depends(_TagsFilter().depends)]
QueryOwnersFilter = Annotated[_OwnersFilter, Depends(_OwnersFilter().depends)]
# DagRun
QueryLastDagRunStateFilter = Annotated[_LastDagRunStateFilter, Depends(_LastDagRunStateFilter().depends)]
# DAGTags
QueryDagTagOrderBy = Annotated[_OrderByParam, Depends(_OrderByParam(DagTag.name, skip_none=False).depends)]
QueryDagTagPatternSearch = Annotated[_DagTagNamePatternSearch, Depends(_DagTagNamePatternSearch().depends)]
