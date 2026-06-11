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
from enum import Enum
from typing import TYPE_CHECKING, Any, ClassVar

import attrs

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable


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


def _build_directional_steps(
    period_start: datetime,
    count: int,
    step: Callable[[datetime, int], datetime],
    direction: Window.Direction,
) -> Iterable[datetime]:
    """
    Enumerate *count* period-starts beginning at or ending at *period_start*.

    *step* maps ``(base, i) -> base`` advanced by ``i`` units (e.g. ``i`` minutes
    for an hour window, ``i`` months for a year window). For ``FORWARD`` the
    sequence starts at *period_start*; for ``BACKWARD`` it is the trailing
    sequence whose last member is *period_start* (the mirror of ``FORWARD``),
    computed by stepping the base back ``count - 1`` units first. Callers that
    need a day-1 precondition must enforce it before calling this — it is not
    checked here.
    """
    base = step(period_start, -(count - 1)) if direction is Window.Direction.BACKWARD else period_start
    return (step(base, i) for i in range(count))


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

    class Direction(str, Enum):
        """Direction of a :class:`Window` fan-out relative to the upstream key."""

        BACKWARD = "backward"
        """Yield the trailing period ending at the upstream key (the mirror of FORWARD)."""

        FORWARD = "forward"
        """Default; yield the period starting at the upstream key (forward in time)."""

    #: Type that ``to_upstream`` expects as its ``decoded_downstream`` argument.
    #: ``RollupMapper.__init__`` uses this to reject pairings where the upstream
    #: mapper's ``decode_downstream`` leaves the value as ``str`` (base identity)
    #: but the window needs a different type. Temporal windows declare ``datetime``.
    expected_decoded_type: ClassVar[type] = str

    def __init__(self, *, direction: Window.Direction = Direction.FORWARD) -> None:
        self.direction = self.Direction(direction)

    @abstractmethod
    def to_upstream(self, decoded_downstream: Any) -> Iterable[Any]:
        """Yield each decoded upstream item composing *decoded_downstream*."""

    def serialize(self) -> dict[str, Any]:
        return {"direction": self.direction.value}

    @classmethod
    def deserialize(cls, data: dict[str, Any]) -> Window:
        return cls(direction=cls.Direction(data["direction"]))


class HourWindow(Window):
    """Sixty consecutive minute period-starts making up one hour."""

    expected_decoded_type: ClassVar[type] = datetime

    def to_upstream(self, period_start: datetime) -> Iterable[datetime]:
        return _build_directional_steps(
            period_start, 60, lambda s, i: s + timedelta(minutes=i), self.direction
        )


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

        The same 24-hour-stride assumption applies to ``DayWindow(direction=Window.Direction.BACKWARD)``:
        the 24 members are enumerated as naive hourly steps ending at the anchor, not as
        a step back to the "previous calendar day" in local time.
    """

    expected_decoded_type: ClassVar[type] = datetime

    def to_upstream(self, period_start: datetime) -> Iterable[datetime]:
        return _build_directional_steps(period_start, 24, lambda s, i: s + timedelta(hours=i), self.direction)


class WeekWindow(Window):
    """Seven consecutive daily period-starts making up one week."""

    expected_decoded_type: ClassVar[type] = datetime

    def to_upstream(self, period_start: datetime) -> Iterable[datetime]:
        return _build_directional_steps(period_start, 7, lambda s, i: s + timedelta(days=i), self.direction)


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
        # Not expressible via _build_directional_steps: the member count is not fixed (28-31)
        # and BACKWARD is an open-closed (prev_month_start, anchor] generator, not a
        # shift-then-forward mirror of FORWARD.
        _require_day_one(period_start, type(self))
        if self.direction is Window.Direction.BACKWARD:
            # Backward yields the trailing period ending at the anchor (period_start),
            # analogous to WeekWindow BACKWARD which yields the 7 days ending at the
            # anchor rather than a calendar week.  The members are the open-closed
            # interval (prev_month_start, anchor] — every day from the day after the
            # previous month's 1st up to and including anchor itself.  This does NOT
            # align to a calendar month: anchor=Mar 1 yields Feb 2…Mar 1 (29 days in
            # 2024), not the full calendar February.
            prev = _shift_months(period_start, -1)
            days = (period_start - prev).days
            return (prev + timedelta(days=i + 1) for i in range(days))
        next_start = _shift_months(period_start, 1)
        days = (next_start - period_start).days
        return (period_start + timedelta(days=i) for i in range(days))


class QuarterWindow(Window):
    """Three monthly period-starts making up one calendar quarter (e.g. Jan/Feb/Mar for Q1)."""

    expected_decoded_type: ClassVar[type] = datetime

    def to_upstream(self, period_start: datetime) -> Iterable[datetime]:
        _require_day_one(period_start, type(self))
        return _build_directional_steps(period_start, 3, _shift_months, self.direction)


class YearWindow(Window):
    """Twelve consecutive monthly period-starts making up one calendar year."""

    expected_decoded_type: ClassVar[type] = datetime

    def to_upstream(self, period_start: datetime) -> Iterable[datetime]:
        _require_day_one(period_start, type(self))
        return _build_directional_steps(period_start, 12, _shift_months, self.direction)


def _convert_segments(segments: Iterable[str]) -> frozenset[str]:
    """
    Validate and convert *segments* to a ``frozenset[str]``.

    Validates each element for type and non-emptiness (with index reporting)
    before collapsing into a frozenset, then checks the result is non-empty.
    """
    validated: list[str] = []
    for i, item in enumerate(segments):
        if not isinstance(item, str):
            raise ValueError(
                f"SegmentWindow segment keys must be str; got {type(item).__name__!r} at index {i}: {item!r}"
            )
        if not item:
            raise ValueError(
                f"SegmentWindow segment keys must be non-empty; got an empty string at index {i}."
            )
        validated.append(item)
    result = frozenset(validated)
    if not result:
        raise ValueError("SegmentWindow requires at least one segment key; got an empty iterable.")
    return result


@attrs.define
class SegmentWindow(Window):
    """
    A fixed categorical set of string keys that constitute one downstream period.

    Paired with :class:`~airflow.partition_mappers.fixed_key.FixedKeyMapper` inside a
    :class:`~airflow.partition_mappers.base.RollupMapper` to express a categorical
    rollup: the scheduler holds the downstream run until every declared segment key
    has arrived from the upstream producer, then fires once.

    ``to_upstream`` returns the complete segment set regardless of the downstream
    anchor value — the anchor is intentionally ignored because all segments map onto
    a single downstream partition key, not a time-based period.

    :param segments: Non-empty iterable of non-empty string segment keys. Duplicates
        are silently de-duplicated.
    :raises ValueError: if *segments* is empty, contains a non-``str`` element, or
        contains an empty-string element.
    """

    expected_decoded_type: ClassVar[type] = str

    _segments: frozenset[str] = attrs.field(converter=_convert_segments)

    def to_upstream(self, decoded_downstream: Any) -> frozenset[str]:
        """Return the full declared segment set, ignoring the downstream anchor."""
        return self._segments

    def serialize(self) -> dict[str, Any]:
        return {"segments": sorted(self._segments)}

    @classmethod
    def deserialize(cls, data: dict[str, Any]) -> SegmentWindow:
        return cls(data["segments"])
