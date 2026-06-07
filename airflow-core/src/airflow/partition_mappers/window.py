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
from datetime import datetime, timedelta
from typing import TYPE_CHECKING, Any, ClassVar

if TYPE_CHECKING:
    from collections.abc import Iterable


def _require_day_one(dt: datetime, window_cls: type) -> None:
    """
    Raise ``ValueError`` if *dt* is not on day 1 of its month.

    Month-aligned windows expand by month-step arithmetic, which is only
    safe when the period starts on day 1 (otherwise e.g.
    ``replace(month=feb)`` on Jan 31 raises ``ValueError: day is out of
    range for month``). Built-in temporal upstream mappers normalise to
    day 1, but a custom ``PartitionMapper.decode_downstream`` could return
    any day — checking here turns a confusing scheduler-tick crash into
    an explicit signal about the upstream-mapper contract.
    """
    if dt.day != 1:
        raise ValueError(
            f"{window_cls.__name__} expects a period start on day 1 of the month, "
            f"got {dt.isoformat()}. The paired upstream mapper's decode_downstream "
            "must normalise to day 1."
        )


def _shift_months(dt: datetime, months: int) -> datetime:
    """
    Return *dt* shifted forward by *months*, wrapping the year as needed.

    Caller is responsible for ensuring *dt* is on day 1 (see
    :func:`_require_day_one`) so that ``replace(month=...)`` is always valid.
    """
    total = dt.month - 1 + months
    return dt.replace(year=dt.year + total // 12, month=total % 12 + 1)


class Window(ABC):
    """
    Describes a rollup window: which decoded upstream items make up one decoded downstream period.

    Paired with a upstream mapper inside a :class:`RollupMapper`. The window
    operates purely in the upstream mapper's *decoded* form (``datetime`` for
    temporal mappers, domain-specific types for future segment / runtime
    mappers). It does not touch key strings, timezones, or formats — those
    belong to the upstream mapper. ``RollupMapper`` orchestrates the three:
    decode the downstream key, expand via the window, encode each upstream.

    The shipped temporal windows describe contiguous, non-overlapping periods
    in which each upstream key feeds exactly one downstream key. Sliding /
    overlapping semantics — one upstream key contributing to multiple adjacent
    downstream periods, e.g. a rolling 7-day window or the
    ``modifies-past-2-hours`` example from the AIP-76 spec — cannot be
    expressed by subclassing ``Window`` alone: ``Window.to_upstream`` only
    enumerates which upstream keys one downstream needs. The complementary
    direction (which downstreams an upstream key feeds) lives on the paired
    :meth:`PartitionMapper.to_downstream`, which already supports returning an
    iterable to fan out one source key across multiple target APDRs. Sliding
    therefore requires customizing **both** ``Window.to_upstream`` and the
    paired ``PartitionMapper.to_downstream`` consistently, so the schema
    invariant ``upstream_key in window.to_upstream(D) ⇔ D in
    mapper.to_downstream(upstream_key)`` holds.
    """

    #: Type that ``to_upstream`` expects as its ``decoded_downstream`` argument.
    #: ``RollupMapper.__init__`` uses this to reject pairings where the upstream
    #: mapper's ``decode_downstream`` leaves the value as ``str`` (base identity)
    #: but the window needs a different type. Temporal windows declare ``datetime``.
    expected_decoded_type: ClassVar[type] = str

    @abstractmethod
    def to_upstream(self, decoded_downstream: Any) -> Iterable[Any]:
        """Yield each decoded upstream item composing *decoded_downstream*."""

    def serialize(self) -> dict[str, Any]:
        return {}

    @classmethod
    def deserialize(cls, data: dict[str, Any]) -> Window:
        return cls()


class HourWindow(Window):
    """Sixty consecutive minute period-starts making up one hour."""

    expected_decoded_type: ClassVar[type] = datetime

    def to_upstream(self, period_start: datetime) -> Iterable[datetime]:
        return (period_start + timedelta(minutes=i) for i in range(60))


class DayWindow(Window):
    """
    Twenty-four consecutive hourly period-starts making up one day.

    Arithmetic is done on naive datetime steps so the 24-hour stride is
    unambiguous across DST transitions; the upstream mapper handles timezone
    awareness when it encodes each upstream member back to a key string.

    .. warning:: **DST edge cases with local-timezone upstream mappers**

        ``DayWindow`` always yields exactly 24 steps regardless of the local
        calendar date. When the upstream mapper uses a local timezone
        (e.g. ``StartOfDayMapper(timezone="America/New_York")``), DST gaps
        and folds can cause a mismatch:

        - **Spring-forward (clock skips ahead)**: the local day has fewer than
          24 real hours. One naive step falls in the gap (e.g. 02:00 ET on
          spring-forward day does not exist), so the upstream mapper encodes it
          to the *next* local hour. That key (e.g. ``"2024-03-10T03"``) does
          not match any upstream event — the rollup window can never be fully
          satisfied.
        - **Fall-back (clock repeats)**: the local day has 25 real hours, but
          ``DayWindow`` only enumerates 24 steps. The extra hour's upstream
          events are never included in the expected set, so those events do not
          contribute to any rollup.

        **Mitigation**: use UTC ``input_format`` (e.g. ``%Y-%m-%dT%H%z``) and
        ensure upstream producers emit UTC partition keys so local-clock
        ambiguity never arises.
    """

    expected_decoded_type: ClassVar[type] = datetime

    def to_upstream(self, period_start: datetime) -> Iterable[datetime]:
        return (period_start + timedelta(hours=i) for i in range(24))


class WeekWindow(Window):
    """Seven consecutive daily period-starts making up one week."""

    expected_decoded_type: ClassVar[type] = datetime

    def to_upstream(self, period_start: datetime) -> Iterable[datetime]:
        return (period_start + timedelta(days=i) for i in range(7))


class MonthWindow(Window):
    """
    All daily period-starts making up one calendar month.

    Requires *period_start* to be on day 1 of the month (built-in temporal
    upstream mappers normalise to day 1). Iterates from day 1 to the last
    day of that calendar month, so the yielded count varies between 28 and
    31 depending on the month. A non-day-1 input raises ``ValueError``.
    """

    expected_decoded_type: ClassVar[type] = datetime

    def to_upstream(self, period_start: datetime) -> Iterable[datetime]:
        _require_day_one(period_start, type(self))
        next_month = period_start.month % 12 + 1
        next_year = period_start.year + (1 if period_start.month == 12 else 0)
        next_start = period_start.replace(year=next_year, month=next_month)
        days = (next_start - period_start).days
        return (period_start + timedelta(days=i) for i in range(days))


class QuarterWindow(Window):
    """Three monthly period-starts making up one calendar quarter (e.g. Jan/Feb/Mar for Q1)."""

    expected_decoded_type: ClassVar[type] = datetime

    def to_upstream(self, period_start: datetime) -> Iterable[datetime]:
        _require_day_one(period_start, type(self))
        return (_shift_months(period_start, i) for i in range(3))


class YearWindow(Window):
    """Twelve consecutive monthly period-starts making up one calendar year."""

    expected_decoded_type: ClassVar[type] = datetime

    def to_upstream(self, period_start: datetime) -> Iterable[datetime]:
        _require_day_one(period_start, type(self))
        return (_shift_months(period_start, i) for i in range(12))
