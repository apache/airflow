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

from collections.abc import Callable
from enum import Enum
from typing import (
    TYPE_CHECKING,
    Any,
    cast,
)

from fastapi import HTTPException, Query
from sqlalchemy import and_, or_

from airflow.api_fastapi.common.parameters.base import BaseParam, T
from airflow.api_fastapi.compat import HTTP_422_UNPROCESSABLE_CONTENT
from airflow.typing_compat import Self
from airflow.utils.sqlalchemy import JsonContains

if TYPE_CHECKING:
    from sqlalchemy.orm.attributes import InstrumentedAttribute
    from sqlalchemy.sql import ColumnElement, Select


class _JsonKVFilter(BaseParam[dict[str, str]]):
    """
    Filter on a JSON column by multiple key-value pairs (AND logic).

    Uses dialect-aware SQL: ``@>`` (JSONB containment, GIN-indexable) on
    PostgreSQL, ``JSON_CONTAINS`` on MySQL, and ``JSON_EXTRACT`` on SQLite.
    """

    def __init__(
        self,
        attribute: ColumnElement,
        value: dict[str, str] | None = None,
        skip_none: bool = True,
    ) -> None:
        super().__init__(skip_none=skip_none)
        self.attribute: ColumnElement = attribute
        self.value = value

    def to_orm(self, select: Select) -> Select:
        if not self.value:
            return select
        return select.where(JsonContains(self.attribute, self.value))

    @classmethod
    def depends(cls, *args: Any, **kwargs: Any) -> Self:
        raise NotImplementedError("Use json_kv_filter_factory instead.")


def json_kv_filter_factory(
    attribute: ColumnElement,
    param_name: str = "extra",
) -> Callable[[list[str]], _JsonKVFilter]:
    DESCRIPTION = (
        "Filter by JSON key-value pairs. Repeat for multiple conditions (AND logic). "
        "Format: key=value (e.g. extra=region=us&extra=env=prod)."
    )

    def depends_json_kv(
        values: list[str] = Query(alias=param_name, default_factory=list, description=DESCRIPTION),
    ) -> _JsonKVFilter:
        kv_dict: dict[str, str] = {}
        for item in values:
            if "=" not in item:
                raise HTTPException(
                    status_code=HTTP_422_UNPROCESSABLE_CONTENT,
                    detail=f"Invalid {param_name} parameter format: {item!r}. Expected 'key=value'.",
                )
            k, v = item.split("=", 1)
            kv_dict[k] = v
        return _JsonKVFilter(attribute, kv_dict or None)

    return depends_json_kv


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
