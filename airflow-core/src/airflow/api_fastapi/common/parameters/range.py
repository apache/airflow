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
from datetime import datetime
from typing import (
    TYPE_CHECKING,
    Annotated,
    Any,
    Generic,
    overload,
)

from fastapi import HTTPException, Query
from pendulum.parsing.exceptions import ParserError
from pydantic import AfterValidator, BaseModel
from sqlalchemy import and_, func, or_

from airflow._shared.timezones import timezone
from airflow.api_fastapi.common.parameters.base import BaseParam, T
from airflow.models import Base
from airflow.typing_compat import Self

if TYPE_CHECKING:
    from sqlalchemy.orm.attributes import InstrumentedAttribute
    from sqlalchemy.sql import Select


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


class NullableDatetimeRangeFilter(RangeFilter):
    """
    RangeFilter for nullable datetime columns (``start_date``, ``end_date``), rewritten for index use.

    ``COALESCE(column, now())`` wraps the column in a function call that prevents PostgreSQL from
    using btree indexes, forcing sequential scans on large tables. This class emits equivalent
    ``OR`` predicates so each branch can be satisfied by an independent index scan.

    NULL semantics: ``start_date=NULL`` means the task has not started yet; ``end_date=NULL`` means
    the task is still running. For lower bounds the NULL branch passes unconditionally — a not-yet-
    started/ended task will eventually satisfy any past lower bound. For upper bounds the NULL branch
    is ``col IS NULL AND now() <= x``, preserving the COALESCE(col, now()) semantics without the
    function-wrap index penalty.
    """

    def to_orm(self, select: Select) -> Select:
        if self.skip_none is False:
            raise ValueError(f"Cannot set 'skip_none' to False on a {type(self)}")

        if self.value is None:
            return select

        if self.value.lower_bound_gte:
            x = self.value.lower_bound_gte
            select = select.where(or_(self.attribute >= x, self.attribute.is_(None)))
        if self.value.lower_bound_gt:
            x = self.value.lower_bound_gt
            select = select.where(or_(self.attribute > x, self.attribute.is_(None)))
        if self.value.upper_bound_lte:
            x = self.value.upper_bound_lte
            select = select.where(or_(self.attribute <= x, and_(self.attribute.is_(None), func.now() <= x)))
        if self.value.upper_bound_lt:
            x = self.value.upper_bound_lt
            select = select.where(or_(self.attribute < x, and_(self.attribute.is_(None), func.now() < x)))

        return select


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
        range_val = Range(
            lower_bound_gte=lower_bound_gte,
            lower_bound_gt=lower_bound_gt,
            upper_bound_lte=upper_bound_lte,
            upper_bound_lt=upper_bound_lt,
        )
        if filter_name in ("start_date", "end_date"):
            return NullableDatetimeRangeFilter(range_val, attr)
        return RangeFilter(range_val, attr)

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


def int_range_filter_factory(
    filter_name: str, model: Base
) -> Callable[[int | None, int | None, int | None, int | None], RangeFilter]:
    def depends_int(
        lower_bound_gte: int | None = Query(alias=f"{filter_name}_gte", default=None),
        lower_bound_gt: int | None = Query(alias=f"{filter_name}_gt", default=None),
        upper_bound_lte: int | None = Query(alias=f"{filter_name}_lte", default=None),
        upper_bound_lt: int | None = Query(alias=f"{filter_name}_lt", default=None),
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

    return depends_int


# Common Safe DateTime
DateTimeQuery = Annotated[str, AfterValidator(_safe_parse_datetime)]

OptionalDateTimeQuery = Annotated[str | None, AfterValidator(_safe_parse_datetime_optional)]
