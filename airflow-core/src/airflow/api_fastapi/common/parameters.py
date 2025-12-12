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
from collections.abc import Callable, Iterable
from datetime import datetime
from enum import Enum
from typing import (
    TYPE_CHECKING,
    Annotated,
    Any,
    Generic,
    Literal,
    TypeVar,
    cast,
    overload,
)

from fastapi import Depends, HTTPException, Query, status
from pendulum.parsing.exceptions import ParserError
from pydantic import AfterValidator, BaseModel, NonNegativeInt
from sqlalchemy import Column, and_, func, not_, or_, select as sql_select
from sqlalchemy.inspection import inspect

from airflow._shared.timezones import timezone
from airflow.api_fastapi.compat import HTTP_422_UNPROCESSABLE_CONTENT
from airflow.api_fastapi.core_api.base import OrmClause
from airflow.api_fastapi.core_api.security import GetUserDep
from airflow.models import Base
from airflow.models.asset import (
    AssetAliasModel,
    AssetModel,
    DagScheduleAssetReference,
    TaskInletAssetReference,
    TaskOutletAssetReference,
)
from airflow.models.connection import Connection
from airflow.models.dag import DagModel, DagTag
from airflow.models.dag_favorite import DagFavorite
from airflow.models.dag_version import DagVersion
from airflow.models.dagrun import DagRun
from airflow.models.errors import ParseImportError
from airflow.models.hitl import HITLDetail
from airflow.models.pool import Pool
from airflow.models.taskinstance import TaskInstance
from airflow.models.variable import Variable
from airflow.models.xcom import XComModel
from airflow.typing_compat import Self
from airflow.utils.state import DagRunState, TaskInstanceState
from airflow.utils.types import DagRunType

if TYPE_CHECKING:
    from sqlalchemy.orm.attributes import InstrumentedAttribute
    from sqlalchemy.sql import ColumnElement, Select

    from airflow.serialization.serialized_objects import SerializedDAG

T = TypeVar("T")


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


class _FavoriteFilter(BaseParam[bool]):
    """Filter Dags by favorite status."""

    def __init__(self, user_id: str, value: T | None = None, skip_none: bool = True) -> None:
        super().__init__(skip_none=skip_none)
        self.user_id = user_id

    def to_orm(self, select_stmt: Select) -> Select:
        if self.value is None and self.skip_none:
            return select_stmt

        if self.value:
            select_stmt = select_stmt.join(DagFavorite, DagFavorite.dag_id == DagModel.dag_id).where(
                DagFavorite.user_id == self.user_id
            )
        else:
            select_stmt = select_stmt.where(
                not_(
                    sql_select(DagFavorite)
                    .where(and_(DagFavorite.dag_id == DagModel.dag_id, DagFavorite.user_id == self.user_id))
                    .exists()
                )
            )

        return select_stmt

    @classmethod
    def depends(cls, user: GetUserDep, is_favorite: bool | None = Query(None)) -> _FavoriteFilter:
        instance = cls(user_id=str(user.get_id())).set_value(is_favorite)
        return instance


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


class QueryTaskInstanceTaskGroupFilter(BaseParam[str]):
    """Task group filter - returns all tasks in the specified group."""

    def __init__(self, dag=None, skip_none: bool = True):
        super().__init__(skip_none=skip_none)
        self._dag: None | SerializedDAG = dag

    @property
    def dag(self) -> None | SerializedDAG:
        return self._dag

    @dag.setter
    def dag(self, value: None | SerializedDAG) -> None:
        self._dag = value

    def to_orm(self, select: Select) -> Select:
        if self.value is None and self.skip_none:
            return select

        if not self.dag:
            raise ValueError("Dag must be set before calling to_orm")

        if not hasattr(self.dag, "task_group"):
            return select

        # Exact matching on group_id
        task_groups = self.dag.task_group.get_task_group_dict()
        task_group = task_groups.get(self.value)
        if not task_group:
            raise HTTPException(
                status.HTTP_404_NOT_FOUND,
                detail={
                    "reason": "not_found",
                    "message": f"Task group {self.value} not found",
                },
            )

        return select.where(TaskInstance.task_id.in_(task.task_id for task in task_group.iter_tasks()))

    @classmethod
    def depends(
        cls,
        value: str | None = Query(
            alias="task_group",
            default=None,
            description="Filter by exact task group ID. Returns all tasks within the specified task group.",
        ),
    ) -> QueryTaskInstanceTaskGroupFilter:
        return cls(dag=None).set_value(value)


def search_param_factory(
    attribute: ColumnElement,
    pattern_name: str,
    skip_none: bool = True,
) -> Callable[[str | None], _SearchParam]:
    DESCRIPTION = (
        "SQL LIKE expression — use `%` / `_` wildcards (e.g. `%customer_%`). "
        "Regular expressions are **not** supported."
    )

    def depends_search(
        value: str | None = Query(alias=pattern_name, default=None, description=DESCRIPTION),
    ) -> _SearchParam:
        search_parm = _SearchParam(attribute, skip_none)
        value = search_parm.transform_aliases(value)
        return search_parm.set_value(value)

    return depends_search


class SortParam(BaseParam[list[str]]):
    """Order result by the attribute."""

    MAX_SORT_PARAMS = 10

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
            self.value = [self.get_primary_key_string()]

        order_by_values = self.value
        if len(order_by_values) > self.MAX_SORT_PARAMS:
            raise HTTPException(
                400,
                f"Ordering with more than {self.MAX_SORT_PARAMS} parameters is not allowed. Provided: {order_by_values}",
            )

        columns: list[ColumnElement] = []
        for order_by_value in order_by_values:
            lstriped_orderby = order_by_value.lstrip("-")
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

            if order_by_value.startswith("-"):
                columns.append(column.desc())
            else:
                columns.append(column.asc())

        # Reset default sorting
        select = select.order_by(None)

        primary_key_column = self.get_primary_key_column()
        # Always add a final discriminator to enforce deterministic ordering.
        if order_by_values and order_by_values[0].startswith("-"):
            columns.append(primary_key_column.desc())
        else:
            columns.append(primary_key_column.asc())

        return select.order_by(*columns)

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
        to_replace_attrs = list(self.to_replace.keys()) if self.to_replace else []

        all_attrs = self.allowed_attrs + to_replace_attrs

        def inner(
            order_by: list[str] = Query(
                default=[default] if default is not None else [self.get_primary_key_string()],
                description=f"Attributes to order by, multi criteria sort is supported. Prefix with `-` for descending order. "
                f"Supported attributes: `{', '.join(all_attrs) if all_attrs else self.get_primary_key_string()}`",
            ),
        ) -> SortParam:
            return self.set_value(order_by)

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
    CONTAINS = "contains"


class FilterParam(BaseParam[T]):
    """Filter on attribute."""

    def __init__(
        self,
        attribute: InstrumentedAttribute,
        value: T | None = None,
        filter_option: FilterOptionEnum = FilterOptionEnum.EQUAL,
        skip_none: bool = True,
    ) -> None:
        super().__init__(value, skip_none)
        self.attribute: InstrumentedAttribute = attribute
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
        if self.filter_option == FilterOptionEnum.CONTAINS:
            # For JSON/JSONB columns, convert to text before applying LIKE
            from sqlalchemy import Text, cast

            if str(self.attribute.type).upper() in ("JSON", "JSONB"):
                return select.where(cast(self.attribute, Text).contains(self.value))
            return select.where(self.attribute.contains(self.value))
        raise ValueError(f"Invalid filter option {self.filter_option} for value {self.value}")

    @classmethod
    def depends(cls, *args: Any, **kwargs: Any) -> Self:
        raise NotImplementedError("Use filter_param_factory instead , depends is not implemented.")


def filter_param_factory(
    attribute: ColumnElement | InstrumentedAttribute,
    _type: type,
    filter_option: FilterOptionEnum = FilterOptionEnum.EQUAL,
    filter_name: str | None = None,
    default_value: T | None = None,
    default_factory: Callable[[], T | None] | None = None,
    skip_none: bool = True,
    transform_callable: Callable[[T | None], Any] | None = None,
    *,
    description: str | None = None,
) -> Callable[[T | None], FilterParam[T | None]]:
    # if filter_name is not provided, use the attribute name as the default
    filter_name = filter_name or getattr(attribute, "name", str(attribute))
    # can only set either default_value or default_factory
    query = (
        Query(alias=filter_name, default_factory=default_factory, description=description)
        if default_factory is not None
        else Query(alias=filter_name, default=default_value, description=description)
    )

    def depends_filter(value: T | None = query) -> FilterParam[T | None]:
        if transform_callable:
            value = transform_callable(value)
        # Cast to InstrumentedAttribute for type compatibility
        attr = cast("InstrumentedAttribute", attribute)
        return FilterParam(attr, value, filter_option, skip_none)

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
        super().__init__(skip_none=skip_none)

    @classmethod
    def depends(cls, dag_ids: list[str] = Query(None)) -> _DagIdAssetReferenceFilter:
        # needed to handle cases where dag_ids=a1,b1
        if dag_ids and len(dag_ids) == 1 and "," in dag_ids[0]:
            dag_ids = dag_ids[0].split(",")
        return cls().set_value(dag_ids)

    def to_orm(self, select: Select) -> Select:
        if self.value is None and self.skip_none:
            return select

        # At this point, self.value is either a list[str] or None -> coerce falsy None to an empty list
        dag_ids = self.value or []
        return select.where(
            (AssetModel.scheduled_dags.any(DagScheduleAssetReference.dag_id.in_(dag_ids)))
            | (AssetModel.producing_tasks.any(TaskOutletAssetReference.dag_id.in_(dag_ids)))
            | (AssetModel.consuming_tasks.any(TaskInletAssetReference.dag_id.in_(dag_ids)))
        )


class Range(BaseModel, Generic[T]):
    """Range with a lower and upper bound."""

    lower_bound_gte: T | None
    lower_bound_gt: T | None
    upper_bound_lte: T | None
    upper_bound_lt: T | None


class RangeFilter(BaseParam[Range]):
    """Filter on range in between the lower and upper bound."""

    def __init__(self, value: Range | None, attribute: InstrumentedAttribute) -> None:
        super().__init__(value)
        self.attribute: InstrumentedAttribute = attribute

    def to_orm(self, select: Select) -> Select:
        if self.skip_none is False:
            raise ValueError(f"Cannot set 'skip_none' to False on a {type(self)}")

        if self.value is None:
            return select

        if self.value.lower_bound_gte:
            select = select.where(self.attribute >= self.value.lower_bound_gte)
        if self.value.lower_bound_gt:
            select = select.where(self.attribute > self.value.lower_bound_gt)
        if self.value.upper_bound_lte:
            select = select.where(self.attribute <= self.value.upper_bound_lte)
        if self.value.upper_bound_lt:
            select = select.where(self.attribute < self.value.upper_bound_lt)

        return select

    @classmethod
    def depends(cls, *args: Any, **kwargs: Any) -> Self:
        raise NotImplementedError("Use the `range_filter_factory` function to create the dependency")

    def is_active(self) -> bool:
        """Check if the range filter has any active bounds."""
        return self.value is not None and (
            self.value.lower_bound_gte is not None
            or self.value.lower_bound_gt is not None
            or self.value.upper_bound_lte is not None
            or self.value.upper_bound_lt is not None
        )


def datetime_range_filter_factory(
    filter_name: str, model: Base, attribute_name: str | None = None
) -> Callable[[datetime | None, datetime | None, datetime | None, datetime | None], RangeFilter]:
    def depends_datetime(
        lower_bound_gte: datetime | None = Query(alias=f"{filter_name}_gte", default=None),
        lower_bound_gt: datetime | None = Query(alias=f"{filter_name}_gt", default=None),
        upper_bound_lte: datetime | None = Query(alias=f"{filter_name}_lte", default=None),
        upper_bound_lt: datetime | None = Query(alias=f"{filter_name}_lt", default=None),
    ) -> RangeFilter:
        attr = getattr(model, attribute_name or filter_name)
        if filter_name in ("start_date", "end_date"):
            attr = func.coalesce(attr, func.now())
        return RangeFilter(
            Range(
                lower_bound_gte=lower_bound_gte,
                lower_bound_gt=lower_bound_gt,
                upper_bound_lte=upper_bound_lte,
                upper_bound_lt=upper_bound_lt,
            ),
            attr,
        )

    return depends_datetime


def float_range_filter_factory(
    filter_name: str, model: Base
) -> Callable[[float | None, float | None, float | None, float | None], RangeFilter]:
    def depends_float(
        lower_bound_gte: float | None = Query(alias=f"{filter_name}_gte", default=None),
        lower_bound_gt: float | None = Query(alias=f"{filter_name}_gt", default=None),
        upper_bound_lte: float | None = Query(alias=f"{filter_name}_lte", default=None),
        upper_bound_lt: float | None = Query(alias=f"{filter_name}_lt", default=None),
    ) -> RangeFilter:
        return RangeFilter(
            Range(
                lower_bound_gte=lower_bound_gte,
                lower_bound_gt=lower_bound_gt,
                upper_bound_lte=upper_bound_lte,
                upper_bound_lt=upper_bound_lt,
            ),
            getattr(model, filter_name),
        )

    return depends_float


# Common Safe DateTime
DateTimeQuery = Annotated[str, AfterValidator(_safe_parse_datetime)]
OptionalDateTimeQuery = Annotated[str | None, AfterValidator(_safe_parse_datetime_optional)]

# Dag
QueryLimit = Annotated[LimitFilter, Depends(LimitFilter.depends)]
QueryOffset = Annotated[OffsetFilter, Depends(OffsetFilter.depends)]
QueryPausedFilter = Annotated[
    FilterParam[bool | None],
    Depends(filter_param_factory(DagModel.is_paused, bool | None, filter_name="paused")),
]
QueryHasImportErrorsFilter = Annotated[
    FilterParam[bool | None],
    Depends(
        filter_param_factory(
            DagModel.has_import_errors,
            bool | None,
            filter_name="has_import_errors",
            description="Filter Dags by having import errors. Only Dags that have been successfully loaded before will be returned.",
        )
    ),
]
QueryFavoriteFilter = Annotated[_FavoriteFilter, Depends(_FavoriteFilter.depends)]
QueryExcludeStaleFilter = Annotated[_ExcludeStaleFilter, Depends(_ExcludeStaleFilter.depends)]
QueryDagIdPatternSearch = Annotated[
    _SearchParam, Depends(search_param_factory(DagModel.dag_id, "dag_id_pattern"))
]
QueryDagDisplayNamePatternSearch = Annotated[
    _SearchParam, Depends(search_param_factory(DagModel.dag_display_name, "dag_display_name_pattern"))
]
QueryBundleNameFilter = Annotated[
    FilterParam[str | None],
    Depends(filter_param_factory(DagModel.bundle_name, str | None, filter_name="bundle_name")),
]
QueryBundleVersionFilter = Annotated[
    FilterParam[str | None],
    Depends(filter_param_factory(DagModel.bundle_version, str | None, filter_name="bundle_version")),
]
QueryDagIdPatternSearchWithNone = Annotated[
    _SearchParam, Depends(search_param_factory(DagModel.dag_id, "dag_id_pattern", False))
]
QueryTagsFilter = Annotated[_TagsFilter, Depends(_TagsFilter.depends)]
QueryOwnersFilter = Annotated[_OwnersFilter, Depends(_OwnersFilter.depends)]


class _HasAssetScheduleFilter(BaseParam[bool]):
    """Filter Dags that have asset-based scheduling."""

    def to_orm(self, select: Select) -> Select:
        if self.value is None and self.skip_none:
            return select

        asset_ref_subquery = sql_select(DagScheduleAssetReference.dag_id).distinct()

        if self.value:
            # Filter Dags that have asset-based scheduling
            return select.where(DagModel.dag_id.in_(asset_ref_subquery))

        # Filter Dags that do NOT have asset-based scheduling
        return select.where(DagModel.dag_id.notin_(asset_ref_subquery))

    @classmethod
    def depends(
        cls,
        has_asset_schedule: bool | None = Query(None, description="Filter Dags with asset-based scheduling"),
    ) -> _HasAssetScheduleFilter:
        return cls().set_value(has_asset_schedule)


class _AssetDependencyFilter(BaseParam[str]):
    """Filter Dags by specific asset dependencies."""

    def to_orm(self, select: Select) -> Select:
        if self.value is None and self.skip_none:
            return select

        asset_dag_subquery = (
            sql_select(DagScheduleAssetReference.dag_id)
            .join(AssetModel, DagScheduleAssetReference.asset_id == AssetModel.id)
            .where(or_(AssetModel.name.ilike(f"%{self.value}%"), AssetModel.uri.ilike(f"%{self.value}%")))
            .distinct()
        )

        return select.where(DagModel.dag_id.in_(asset_dag_subquery))

    @classmethod
    def depends(
        cls,
        asset_dependency: str | None = Query(
            None, description="Filter Dags by asset dependency (name or URI)"
        ),
    ) -> _AssetDependencyFilter:
        return cls().set_value(asset_dependency)


QueryHasAssetScheduleFilter = Annotated[_HasAssetScheduleFilter, Depends(_HasAssetScheduleFilter.depends)]
QueryAssetDependencyFilter = Annotated[_AssetDependencyFilter, Depends(_AssetDependencyFilter.depends)]


class _PendingActionsFilter(BaseParam[bool]):
    """Filter Dags by having pending HITL actions (more than 1)."""

    def to_orm(self, select: Select) -> Select:
        if self.value is None and self.skip_none:
            return select

        from airflow.models.hitl import HITLDetail
        from airflow.models.taskinstance import TaskInstance

        # Join with HITLDetail and TaskInstance to find Dags
        pending_actions_count_subquery = (
            sql_select(func.count(HITLDetail.ti_id))
            .join(TaskInstance, HITLDetail.ti_id == TaskInstance.id)
            .where(
                HITLDetail.responded_at.is_(None),
                TaskInstance.state == TaskInstanceState.DEFERRED,
            )
            .where(TaskInstance.dag_id == DagModel.dag_id)
            .scalar_subquery()
        )

        if self.value is True:
            # Filter to show only Dags with pending actions
            where_clause = pending_actions_count_subquery >= 1
        else:
            # Filter to show only Dags without pending actions
            where_clause = pending_actions_count_subquery == 0

        return select.where(where_clause)

    @classmethod
    def depends(cls, has_pending_actions: bool | None = Query(None)) -> _PendingActionsFilter:
        return cls().set_value(has_pending_actions)


QueryPendingActionsFilter = Annotated[_PendingActionsFilter, Depends(_PendingActionsFilter.depends)]

# DagRun
QueryLastDagRunStateFilter = Annotated[
    FilterParam[DagRunState | None],
    Depends(filter_param_factory(DagRun.state, DagRunState | None, filter_name="last_dag_run_state")),
]


def _transform_dag_run_states(states: Iterable[str] | None) -> list[DagRunState | None] | None:
    try:
        if not states:
            return None
        return [None if s in ("none", None) else DagRunState(s) for s in states]
    except ValueError:
        raise HTTPException(
            status_code=HTTP_422_UNPROCESSABLE_CONTENT,
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
            status_code=HTTP_422_UNPROCESSABLE_CONTENT,
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

QueryDagRunTriggeringUserSearch = Annotated[
    _SearchParam, Depends(search_param_factory(DagRun.triggering_user_name, "triggering_user"))
]

# DagTags
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
            status_code=HTTP_422_UNPROCESSABLE_CONTENT,
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
QueryTIPoolNamePatternSearch = Annotated[
    _SearchParam,
    Depends(search_param_factory(TaskInstance.pool, "pool_name_pattern")),
]

QueryTIQueueNamePatternSearch = Annotated[
    _SearchParam,
    Depends(search_param_factory(TaskInstance.queue, "queue_name_pattern")),
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
QueryTITaskGroupFilter = Annotated[
    QueryTaskInstanceTaskGroupFilter, Depends(QueryTaskInstanceTaskGroupFilter.depends)
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
QueryDagRunVersionFilter = Annotated[
    FilterParam[list[int]],
    Depends(
        filter_param_factory(
            DagVersion.version_number,
            list[int],
            FilterOptionEnum.ANY_EQUAL,
            default_factory=list,
            filter_name="dag_version",
        )
    ),
]
QueryTITryNumberFilter = Annotated[
    FilterParam[list[int]],
    Depends(
        filter_param_factory(
            TaskInstance.try_number, list[int], FilterOptionEnum.ANY_EQUAL, default_factory=list
        )
    ),
]

QueryTIOperatorFilter = Annotated[
    FilterParam[list[str]],
    Depends(
        filter_param_factory(
            TaskInstance.operator, list[str], FilterOptionEnum.ANY_EQUAL, default_factory=list
        )
    ),
]
QueryTIOperatorNamePatternSearch = Annotated[
    _SearchParam,
    Depends(
        search_param_factory(
            TaskInstance.custom_operator_name,
            "operator_name_pattern",
        )
    ),
]

QueryTIMapIndexFilter = Annotated[
    FilterParam[list[int]],
    Depends(
        filter_param_factory(
            TaskInstance.map_index, list[int], FilterOptionEnum.ANY_EQUAL, default_factory=list
        )
    ),
]

# XCom
QueryXComKeyPatternSearch = Annotated[
    _SearchParam, Depends(search_param_factory(XComModel.key, "xcom_key_pattern"))
]

QueryXComDagDisplayNamePatternSearch = Annotated[
    _SearchParam, Depends(search_param_factory(DagModel.dag_display_name, "dag_display_name_pattern"))
]
QueryXComRunIdPatternSearch = Annotated[
    _SearchParam, Depends(search_param_factory(XComModel.run_id, "run_id_pattern"))
]
QueryXComTaskIdPatternSearch = Annotated[
    _SearchParam, Depends(search_param_factory(XComModel.task_id, "task_id_pattern"))
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


QueryIncludeUpstream = Annotated[bool, AfterValidator(_optional_boolean)]
QueryIncludeDownstream = Annotated[bool, AfterValidator(_optional_boolean)]

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

# Human in the loop
QueryHITLDetailDagIdPatternSearch = Annotated[
    _SearchParam,
    Depends(
        search_param_factory(
            TaskInstance.dag_id,
            "dag_id_pattern",
        )
    ),
]
QueryHITLDetailTaskIdPatternSearch = Annotated[
    _SearchParam,
    Depends(
        search_param_factory(
            TaskInstance.task_id,
            "task_id_pattern",
        )
    ),
]
QueryHITLDetailTaskIdFilter = Annotated[
    FilterParam[str | None],
    Depends(
        filter_param_factory(
            TaskInstance.task_id,
            str | None,
            filter_name="task_id",
        )
    ),
]
QueryHITLDetailMapIndexFilter = Annotated[
    FilterParam[int | None],
    Depends(
        filter_param_factory(
            TaskInstance.map_index,
            int | None,
            filter_name="map_index",
        )
    ),
]
QueryHITLDetailSubjectSearch = Annotated[
    _SearchParam,
    Depends(
        search_param_factory(
            HITLDetail.subject,
            "subject_search",
        )
    ),
]
QueryHITLDetailBodySearch = Annotated[
    _SearchParam,
    Depends(
        search_param_factory(
            HITLDetail.body,
            "body_search",
        )
    ),
]
QueryHITLDetailResponseReceivedFilter = Annotated[
    FilterParam[bool | None],
    Depends(
        filter_param_factory(
            HITLDetail.response_received,
            bool | None,
            filter_name="response_received",
        )
    ),
]
QueryHITLDetailRespondedUserIdFilter = Annotated[
    FilterParam[list[str]],
    Depends(
        filter_param_factory(
            HITLDetail.responded_by_user_id,
            list[str],
            FilterOptionEnum.ANY_EQUAL,
            default_factory=list,
            filter_name="responded_by_user_id",
        )
    ),
]
QueryHITLDetailRespondedUserNameFilter = Annotated[
    FilterParam[list[str]],
    Depends(
        filter_param_factory(
            HITLDetail.responded_by_user_name,
            list[str],
            FilterOptionEnum.ANY_EQUAL,
            default_factory=list,
            filter_name="responded_by_user_name",
        )
    ),
]

# Parse Import Errors
QueryParseImportErrorFilenamePatternSearch = Annotated[
    _SearchParam, Depends(search_param_factory(ParseImportError.filename, "filename_pattern"))
]
