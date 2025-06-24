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
from collections.abc import Iterable
from datetime import datetime
from enum import Enum
from typing import (
    TYPE_CHECKING,
    Annotated,
    Any,
    Callable,
    Generic,
    Literal,
    Optional,
    TypeVar,
    Union,
    overload,
)

from fastapi import Depends, HTTPException, Query, status
from pendulum.parsing.exceptions import ParserError
from pydantic import AfterValidator, BaseModel, NonNegativeInt
from sqlalchemy import Column, and_, case, func, or_
from sqlalchemy.inspection import inspect

from airflow.api_fastapi.core_api.base import OrmClause
from airflow.models import Base
from airflow.models.asset import (
    AssetAliasModel,
    AssetModel,
    DagScheduleAssetReference,
    TaskOutletAssetReference,
)
from airflow.models.connection import Connection
from airflow.models.dag import DagModel, DagTag
from airflow.models.dag_version import DagVersion
from airflow.models.dagrun import DagRun
from airflow.models.pool import Pool
from airflow.models.taskinstance import TaskInstance
from airflow.models.variable import Variable
from airflow.typing_compat import Self
from airflow.utils import timezone
from airflow.utils.state import DagRunState, TaskInstanceState
from airflow.utils.types import DagRunType

if TYPE_CHECKING:
    from sqlalchemy.sql import ColumnElement, Select

T = TypeVar("T")


class BaseParam(OrmClause[T], ABC):
    """Base class for path or query parameters with ORM transformation."""

    def __init__(self, value: T | None = None, skip_none: bool = True) -> None:
        super().__init__(value)
        self.attribute: ColumnElement | None = None
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
    def depends(cls, limit: NonNegativeInt = 50) -> LimitFilter:
        return cls().set_value(limit)


class OffsetFilter(BaseParam[NonNegativeInt]):
    """Filter on offset."""

    def to_orm(self, select: Select) -> Select:
        if self.value is None and self.skip_none:
            return select
        return select.offset(self.value)

    @classmethod
    def depends(cls, offset: NonNegativeInt = 0) -> OffsetFilter:
        return cls().set_value(offset)


class _ExcludeStaleFilter(BaseParam[bool]):
    """Filter on is_stale."""

    def to_orm(self, select: Select) -> Select:
        if self.value and self.skip_none:
            return select.where(DagModel.is_stale != self.value)
        return select

    @classmethod
    def depends(cls, exclude_stale: bool = True) -> _ExcludeStaleFilter:
        return cls().set_value(exclude_stale)


class _SearchParam(BaseParam[str]):
    """Search on attribute."""

    def __init__(self, attribute: ColumnElement, skip_none: bool = True) -> None:
        super().__init__(skip_none=skip_none)
        self.attribute: ColumnElement = attribute

    def to_orm(self, select: Select) -> Select:
        if self.value is None and self.skip_none:
            return select
        return select.where(self.attribute.ilike(f"%{self.value}%"))

    def transform_aliases(self, value: str | None) -> str | None:
        if value == "~":
            value = "%"
        return value

    @classmethod
    def depends(cls, *args: Any, **kwargs: Any) -> Self:
        raise NotImplementedError("Use search_param_factory instead , depends is not implemented.")


def search_param_factory(
    attribute: ColumnElement,
    pattern_name: str,
    skip_none: bool = True,
) -> Callable[[str | None], _SearchParam]:
    def depends_search(value: str | None = Query(alias=pattern_name, default=None)) -> _SearchParam:
        search_parm = _SearchParam(attribute, skip_none)
        value = search_parm.transform_aliases(value)
        return search_parm.set_value(value)

    return depends_search


class SortParam(BaseParam[str]):
    """Order result by the attribute."""

    def __init__(
        self, allowed_attrs: list[str], model: Base, to_replace: dict[str, str | Column] | None = None
    ) -> None:
        super().__init__()
        self.allowed_attrs = allowed_attrs
        self.model = model
        self.to_replace = to_replace

    def to_orm(self, select: Select) -> Select:
        if self.skip_none is False:
            raise ValueError(f"Cannot set 'skip_none' to False on a {type(self)}")

        if self.value is None:
            self.value = self.get_primary_key_string()

        lstriped_orderby = self.value.lstrip("-")
        column: Column | None = None
        if self.to_replace:
            replacement = self.to_replace.get(lstriped_orderby, lstriped_orderby)
            if isinstance(replacement, str):
                lstriped_orderby = replacement
            else:
                column = replacement

        if (self.allowed_attrs and lstriped_orderby not in self.allowed_attrs) and column is None:
            raise HTTPException(
                400,
                f"Ordering with '{lstriped_orderby}' is disallowed or "
                f"the attribute does not exist on the model",
            )
        if column is None:
            column = getattr(self.model, lstriped_orderby)

        # MySQL does not support `nullslast`, and True/False ordering depends on the
        # database implementation.
        nullscheck = case((column.isnot(None), 0), else_=1)

        # Reset default sorting
        select = select.order_by(None)

        primary_key_column = self.get_primary_key_column()

        if self.value[0] == "-":
            return select.order_by(nullscheck, column.desc(), primary_key_column.desc())
        return select.order_by(nullscheck, column.asc(), primary_key_column.asc())

    def get_primary_key_column(self) -> Column:
        """Get the primary key column of the model of SortParam object."""
        return inspect(self.model).primary_key[0]

    def get_primary_key_string(self) -> str:
        """Get the primary key string of the model of SortParam object."""
        return self.get_primary_key_column().name

    @classmethod
    def depends(cls, *args: Any, **kwargs: Any) -> Self:
        raise NotImplementedError("Use dynamic_depends, depends not implemented.")

    def dynamic_depends(self, default: str | None = None) -> Callable:
        def inner(order_by: str = default or self.get_primary_key_string()) -> SortParam:
            return self.set_value(self.get_primary_key_string() if order_by == "" else order_by)

        return inner


class FilterOptionEnum(Enum):
    """Filter options for FilterParam."""

    EQUAL = "eq"
    NOT_EQUAL = "ne"
    LESS_THAN = "lt"
    LESS_THAN_EQUAL = "le"
    GREATER_THAN = "gt"
    GREATER_THAN_EQUAL = "ge"
    IN = "in"
    NOT_IN = "not_in"
    ANY_EQUAL = "any_eq"
    ALL_EQUAL = "all_eq"
    IS_NONE = "is_none"


class FilterParam(BaseParam[T]):
    """Filter on attribute."""

    def __init__(
        self,
        attribute: ColumnElement,
        value: T | None = None,
        filter_option: FilterOptionEnum = FilterOptionEnum.EQUAL,
        skip_none: bool = True,
    ) -> None:
        super().__init__(value, skip_none)
        self.attribute: ColumnElement = attribute
        self.value: T | None = value
        self.filter_option: FilterOptionEnum = filter_option

    def to_orm(self, select: Select) -> Select:
        if isinstance(self.value, (list, str)) and not self.value and self.skip_none:
            return select
        if self.value is None and self.skip_none:
            return select

        if isinstance(self.value, list):
            if self.filter_option == FilterOptionEnum.IN:
                return select.where(self.attribute.in_(self.value))
            if self.filter_option == FilterOptionEnum.NOT_IN:
                return select.where(self.attribute.notin_(self.value))
            if self.filter_option == FilterOptionEnum.ANY_EQUAL:
                conditions = [self.attribute == val for val in self.value]
                return select.where(or_(*conditions))
            if self.filter_option == FilterOptionEnum.ALL_EQUAL:
                conditions = [self.attribute == val for val in self.value]
                return select.where(and_(*conditions))
            raise HTTPException(
                400, f"Invalid filter option {self.filter_option} for list value {self.value}"
            )

        if self.filter_option == FilterOptionEnum.EQUAL:
            return select.where(self.attribute == self.value)
        if self.filter_option == FilterOptionEnum.NOT_EQUAL:
            return select.where(self.attribute != self.value)
        if self.filter_option == FilterOptionEnum.LESS_THAN:
            return select.where(self.attribute < self.value)
        if self.filter_option == FilterOptionEnum.LESS_THAN_EQUAL:
            return select.where(self.attribute <= self.value)
        if self.filter_option == FilterOptionEnum.GREATER_THAN:
            return select.where(self.attribute > self.value)
        if self.filter_option == FilterOptionEnum.GREATER_THAN_EQUAL:
            return select.where(self.attribute >= self.value)
        if self.filter_option == FilterOptionEnum.IS_NONE:
            if self.value is None:
                return select
            if self.value is False:
                return select.where(self.attribute.is_not(None))
            if self.value is True:
                return select.where(self.attribute.is_(None))
        raise ValueError(f"Invalid filter option {self.filter_option} for value {self.value}")

    @classmethod
    def depends(cls, *args: Any, **kwargs: Any) -> Self:
        raise NotImplementedError("Use filter_param_factory instead , depends is not implemented.")


def filter_param_factory(
    attribute: ColumnElement,
    _type: type,
    filter_option: FilterOptionEnum = FilterOptionEnum.EQUAL,
    filter_name: str | None = None,
    default_value: T | None = None,
    default_factory: Callable[[], T | None] | None = None,
    skip_none: bool = True,
    transform_callable: Callable[[T | None], Any] | None = None,
) -> Callable[[T | None], FilterParam[T | None]]:
    # if filter_name is not provided, use the attribute name as the default
    filter_name = filter_name or attribute.name
    # can only set either default_value or default_factory
    query = (
        Query(alias=filter_name, default_factory=default_factory)
        if default_factory is not None
        else Query(alias=filter_name, default=default_value)
    )

    def depends_filter(value: T | None = query) -> FilterParam[T | None]:
        if transform_callable:
            value = transform_callable(value)
        return FilterParam(attribute, value, filter_option, skip_none)

    # add type hint to value at runtime
    depends_filter.__annotations__["value"] = _type

    return depends_filter


class _TagFilterModel(BaseModel):
    """Tag Filter Model with a match mode parameter."""

    tags: list[str]
    tags_match_mode: Literal["any", "all"] | None


class _TagsFilter(BaseParam[_TagFilterModel]):
    """Filter on tags."""

    def to_orm(self, select: Select) -> Select:
        if self.skip_none is False:
            raise ValueError(f"Cannot set 'skip_none' to False on a {type(self)}")

        if not self.value or not self.value.tags:
            return select

        conditions = [DagModel.tags.any(DagTag.name == tag) for tag in self.value.tags]
        operator = or_ if not self.value.tags_match_mode or self.value.tags_match_mode == "any" else and_
        return select.where(operator(*conditions))

    @classmethod
    def depends(
        cls,
        tags: list[str] = Query(default_factory=list),
        tags_match_mode: Literal["any", "all"] | None = None,
    ) -> _TagsFilter:
        return cls().set_value(_TagFilterModel(tags=tags, tags_match_mode=tags_match_mode))


class _OwnersFilter(BaseParam[list[str]]):
    """Filter on owners."""

    def to_orm(self, select: Select) -> Select:
        if self.skip_none is False:
            raise ValueError(f"Cannot set 'skip_none' to False on a {type(self)}")

        if not self.value:
            return select

        conditions = [DagModel.owners.ilike(f"%{owner}%") for owner in self.value]
        return select.where(or_(*conditions))

    @classmethod
    def depends(cls, owners: list[str] = Query(default_factory=list)) -> _OwnersFilter:
        return cls().set_value(owners)


def _safe_parse_datetime(date_to_check: str) -> datetime:
    """
    Parse datetime and raise error for invalid dates.

    :param date_to_check: the string value to be parsed
    """
    if not date_to_check:
        raise ValueError(f"{date_to_check} cannot be None.")
    return _safe_parse_datetime_optional(date_to_check)


@overload
def _safe_parse_datetime_optional(date_to_check: str) -> datetime: ...


@overload
def _safe_parse_datetime_optional(date_to_check: None) -> None: ...


def _safe_parse_datetime_optional(date_to_check: str | None) -> datetime | None:
    """
    Parse datetime and raise error for invalid dates.

    Allow None values.

    :param date_to_check: the string value to be parsed
    """
    if date_to_check is None:
        return None
    try:
        return timezone.parse(date_to_check, strict=True)
    except (TypeError, ParserError):
        raise HTTPException(
            400, f"Invalid datetime: {date_to_check!r}. Please check the date parameter have this value."
        )


class _DagIdAssetReferenceFilter(BaseParam[list[str]]):
    """Search on dag_id."""

    def __init__(self, skip_none: bool = True) -> None:
        super().__init__(AssetModel.consuming_dags, skip_none)

    @classmethod
    def depends(cls, dag_ids: list[str] = Query(None)) -> _DagIdAssetReferenceFilter:
        # needed to handle cases where dag_ids=a1,b1
        if dag_ids and len(dag_ids) == 1 and "," in dag_ids[0]:
            dag_ids = dag_ids[0].split(",")
        return cls().set_value(dag_ids)

    def to_orm(self, select: Select) -> Select:
        if self.value is None and self.skip_none:
            return select
        return select.where(
            (AssetModel.consuming_dags.any(DagScheduleAssetReference.dag_id.in_(self.value)))
            | (AssetModel.producing_tasks.any(TaskOutletAssetReference.dag_id.in_(self.value)))
        )


class Range(BaseModel, Generic[T]):
    """Range with a lower and upper bound."""

    lower_bound: T | None
    upper_bound: T | None


class RangeFilter(BaseParam[Range]):
    """Filter on range in between the lower and upper bound."""

    def __init__(self, value: Range | None, attribute: ColumnElement) -> None:
        super().__init__(value)
        self.attribute: ColumnElement = attribute

    def to_orm(self, select: Select) -> Select:
        if self.skip_none is False:
            raise ValueError(f"Cannot set 'skip_none' to False on a {type(self)}")

        if self.value and self.value.lower_bound:
            select = select.where(self.attribute >= self.value.lower_bound)
        if self.value and self.value.upper_bound:
            select = select.where(self.attribute <= self.value.upper_bound)
        return select

    @classmethod
    def depends(cls, *args: Any, **kwargs: Any) -> Self:
        raise NotImplementedError("Use the `range_filter_factory` function to create the dependency")

    def is_active(self) -> bool:
        """Check if the range filter has any active bounds."""
        return self.value is not None and (
            self.value.lower_bound is not None or self.value.upper_bound is not None
        )


def datetime_range_filter_factory(
    filter_name: str, model: Base, attribute_name: str | None = None
) -> Callable[[datetime | None, datetime | None], RangeFilter]:
    def depends_datetime(
        lower_bound: datetime | None = Query(alias=f"{filter_name}_gte", default=None),
        upper_bound: datetime | None = Query(alias=f"{filter_name}_lte", default=None),
    ) -> RangeFilter:
        attr = getattr(model, attribute_name or filter_name)
        if filter_name in ("start_date", "end_date"):
            attr = func.coalesce(attr, func.now())
        return RangeFilter(
            Range(lower_bound=lower_bound, upper_bound=upper_bound),
            attr,
        )

    return depends_datetime


def float_range_filter_factory(
    filter_name: str, model: Base
) -> Callable[[float | None, float | None], RangeFilter]:
    def depends_float(
        lower_bound: float | None = Query(alias=f"{filter_name}_gte", default=None),
        upper_bound: float | None = Query(alias=f"{filter_name}_lte", default=None),
    ) -> RangeFilter:
        return RangeFilter(
            Range(lower_bound=lower_bound, upper_bound=upper_bound), getattr(model, filter_name)
        )

    return depends_float


# Common Safe DateTime
DateTimeQuery = Annotated[str, AfterValidator(_safe_parse_datetime)]
OptionalDateTimeQuery = Annotated[Union[str, None], AfterValidator(_safe_parse_datetime_optional)]

# DAG
QueryLimit = Annotated[LimitFilter, Depends(LimitFilter.depends)]
QueryOffset = Annotated[OffsetFilter, Depends(OffsetFilter.depends)]
QueryPausedFilter = Annotated[
    FilterParam[Optional[bool]],
    Depends(filter_param_factory(DagModel.is_paused, Optional[bool], filter_name="paused")),
]
QueryExcludeStaleFilter = Annotated[_ExcludeStaleFilter, Depends(_ExcludeStaleFilter.depends)]
QueryDagIdPatternSearch = Annotated[
    _SearchParam, Depends(search_param_factory(DagModel.dag_id, "dag_id_pattern"))
]
QueryDagDisplayNamePatternSearch = Annotated[
    _SearchParam, Depends(search_param_factory(DagModel.dag_display_name, "dag_display_name_pattern"))
]
QueryDagIdPatternSearchWithNone = Annotated[
    _SearchParam, Depends(search_param_factory(DagModel.dag_id, "dag_id_pattern", False))
]
QueryTagsFilter = Annotated[_TagsFilter, Depends(_TagsFilter.depends)]
QueryOwnersFilter = Annotated[_OwnersFilter, Depends(_OwnersFilter.depends)]

# DagRun
QueryLastDagRunStateFilter = Annotated[
    FilterParam[Optional[DagRunState]],
    Depends(filter_param_factory(DagRun.state, Optional[DagRunState], filter_name="last_dag_run_state")),
]


def _transform_dag_run_states(states: Iterable[str] | None) -> list[DagRunState | None] | None:
    try:
        if not states:
            return None
        return [None if s in ("none", None) else DagRunState(s) for s in states]
    except ValueError:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=f"Invalid value for state. Valid values are {', '.join(DagRunState)}",
        )


QueryDagRunStateFilter = Annotated[
    FilterParam[list[str]],
    Depends(
        filter_param_factory(
            DagRun.state,
            list[str],
            FilterOptionEnum.ANY_EQUAL,
            default_factory=list,
            transform_callable=_transform_dag_run_states,
        )
    ),
]


def _transform_dag_run_types(types: list[str] | None) -> list[DagRunType | None] | None:
    try:
        if not types:
            return None
        return [None if run_type in ("none", None) else DagRunType(run_type) for run_type in types]
    except ValueError:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=f"Invalid value for run type. Valid values are {', '.join(DagRunType)}",
        )


QueryDagRunRunTypesFilter = Annotated[
    FilterParam[list[str]],
    Depends(
        filter_param_factory(
            attribute=DagRun.run_type,
            _type=list[str],
            filter_option=FilterOptionEnum.ANY_EQUAL,
            default_factory=list,
            transform_callable=_transform_dag_run_types,
        )
    ),
]

# DAGTags
QueryDagTagPatternSearch = Annotated[
    _SearchParam, Depends(search_param_factory(DagTag.name, "tag_name_pattern"))
]


# TI
def _transform_ti_states(states: list[str] | None) -> list[TaskInstanceState | None] | None:
    """Transform a list of state strings into a list of TaskInstanceState enums handling special 'None' cases."""
    if not states:
        return None

    try:
        return [None if s in ("no_status", "none", None) else TaskInstanceState(s) for s in states]
    except ValueError:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=f"Invalid value for state. Valid values are {', '.join(TaskInstanceState)}",
        )


QueryTIStateFilter = Annotated[
    FilterParam[list[str]],
    Depends(
        filter_param_factory(
            TaskInstance.state,
            list[str],
            FilterOptionEnum.ANY_EQUAL,
            default_factory=list,
            transform_callable=_transform_ti_states,
        )
    ),
]
QueryTIPoolFilter = Annotated[
    FilterParam[list[str]],
    Depends(
        filter_param_factory(TaskInstance.pool, list[str], FilterOptionEnum.ANY_EQUAL, default_factory=list)
    ),
]
QueryTIQueueFilter = Annotated[
    FilterParam[list[str]],
    Depends(
        filter_param_factory(TaskInstance.queue, list[str], FilterOptionEnum.ANY_EQUAL, default_factory=list)
    ),
]
QueryTIExecutorFilter = Annotated[
    FilterParam[list[str]],
    Depends(
        filter_param_factory(
            TaskInstance.executor, list[str], FilterOptionEnum.ANY_EQUAL, default_factory=list
        )
    ),
]
QueryTITaskDisplayNamePatternSearch = Annotated[
    _SearchParam, Depends(search_param_factory(TaskInstance.task_display_name, "task_display_name_pattern"))
]
QueryTIDagVersionFilter = Annotated[
    FilterParam[list[int]],
    Depends(
        filter_param_factory(
            DagVersion.version_number,
            list[int],
            FilterOptionEnum.ANY_EQUAL,
            default_factory=list,
        )
    ),
]

# Assets
QueryAssetNamePatternSearch = Annotated[
    _SearchParam, Depends(search_param_factory(AssetModel.name, "name_pattern"))
]
QueryUriPatternSearch = Annotated[_SearchParam, Depends(search_param_factory(AssetModel.uri, "uri_pattern"))]
QueryAssetAliasNamePatternSearch = Annotated[
    _SearchParam, Depends(search_param_factory(AssetAliasModel.name, "name_pattern"))
]
QueryAssetDagIdPatternSearch = Annotated[
    _DagIdAssetReferenceFilter, Depends(_DagIdAssetReferenceFilter.depends)
]

# Variables
QueryVariableKeyPatternSearch = Annotated[
    _SearchParam, Depends(search_param_factory(Variable.key, "variable_key_pattern"))
]

# Pools
QueryPoolNamePatternSearch = Annotated[
    _SearchParam, Depends(search_param_factory(Pool.pool, "pool_name_pattern"))
]


# UI Shared
def _optional_boolean(value: bool | None) -> bool | None:
    return value if value is not None else False


QueryIncludeUpstream = Annotated[Union[bool], AfterValidator(_optional_boolean)]
QueryIncludeDownstream = Annotated[Union[bool], AfterValidator(_optional_boolean)]

state_priority: list[None | TaskInstanceState] = [
    TaskInstanceState.FAILED,
    TaskInstanceState.UPSTREAM_FAILED,
    TaskInstanceState.UP_FOR_RETRY,
    TaskInstanceState.UP_FOR_RESCHEDULE,
    TaskInstanceState.QUEUED,
    TaskInstanceState.SCHEDULED,
    TaskInstanceState.DEFERRED,
    TaskInstanceState.RUNNING,
    TaskInstanceState.RESTARTING,
    None,
    TaskInstanceState.SUCCESS,
    TaskInstanceState.SKIPPED,
    TaskInstanceState.REMOVED,
]

# Connections
QueryConnectionIdPatternSearch = Annotated[
    _SearchParam, Depends(search_param_factory(Connection.conn_id, "connection_id_pattern"))
]
