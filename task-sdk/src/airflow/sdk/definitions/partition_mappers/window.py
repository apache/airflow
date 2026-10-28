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

# The SDK and core class hierarchies are independent (the SDK cannot import core),
# so both carry the same author-facing definitions; runtime logic lives in the core
# copy. The ``check-window-in-sync`` prek hook enforces that the two stay in sync.
from __future__ import annotations

from datetime import datetime, timedelta
from enum import Enum
from typing import TYPE_CHECKING, Any, ClassVar

import attrs

if TYPE_CHECKING:
    from collections.abc import Iterable


def _validate_step(window_cls: type[Window], step: timedelta | None) -> timedelta | None:
    """
    Return *step* if *window_cls* can enumerate its period in increments of *step*.

    Mirrors the core ``_validate_step`` so an invalid step fails at Dag parse time.
    """
    if step is None:
        return None
    if window_cls.period is None:
        raise TypeError(f"{window_cls.__name__} does not support 'step'; its period has no fixed length.")
    if not isinstance(step, timedelta):
        raise TypeError(f"{window_cls.__name__} 'step' must be a timedelta, got {type(step).__name__!r}.")
    if step <= timedelta(0) or step % timedelta(seconds=1):
        raise ValueError(
            f"{window_cls.__name__} 'step' must be a positive whole number of seconds, got {step}."
        )
    if window_cls.period % step:
        raise ValueError(
            f"{window_cls.__name__} 'step' must divide its period ({window_cls.period}) evenly, got {step}."
        )
    return step


class Window:
    """
    Describes a rollup window: which upstream keys make up one downstream key.

    Paired with a ``upstream_mapper`` :class:`PartitionMapper` inside a
    :class:`RollupMapper`. The upstream_mapper normalizes upstream keys to the
    downstream granularity; the window enumerates the complete set of
    upstream keys that roll up into one downstream key. Runtime logic
    lives in ``airflow.partition_mappers.window`` on the scheduler side.

    The shipped temporal windows describe contiguous, non-overlapping periods
    in which each upstream key feeds exactly one downstream key. Sliding /
    overlapping semantics — e.g. a rolling 7-day window, or the
    ``modifies-past-2-hours`` example from the AIP-76 spec — cannot be
    expressed by subclassing ``Window`` alone, because ``Window.to_upstream``
    only enumerates the upstream keys one downstream needs. The complementary
    direction (which downstreams an upstream key feeds) lives on the paired
    :meth:`PartitionMapper.to_downstream`, which supports returning an iterable
    to fan out one source key across multiple target partitions. Sliding
    therefore requires customizing **both** sides consistently so the
    invariant ``upstream_key in window.to_upstream(D) ⇔ D in
    mapper.to_downstream(upstream_key)`` holds.

    :param direction: ``Window.Direction.FORWARD`` (default) fans out the period
        starting at the upstream key (forward in time);
        ``Window.Direction.BACKWARD`` fans out the trailing period ending at the
        upstream key (the mirror of FORWARD).
    :param step: Spacing between upstream keys for fixed-length windows
        (``HourWindow``, ``DayWindow``, ``WeekWindow``); must divide the period
        evenly. ``None`` (default) keeps the window's built-in unit, e.g. one
        minute for ``HourWindow``. Calendar windows reject it.
    """

    class Direction(str, Enum):
        """Direction of a :class:`Window` fan-out relative to the upstream key."""

        BACKWARD = "backward"
        """Yield the trailing period ending at the upstream key (the mirror of FORWARD)."""

        FORWARD = "forward"
        """Default; yield the period starting at the upstream key (forward in time)."""

    #: Decoded type the window iterates in; ``RollupMapper.__init__`` uses this
    #: to reject pairings where the upstream mapper decodes to a different type.
    #: Default ``str`` matches the identity mapper; temporal windows declare
    #: ``datetime``. Mirrors the same attribute on the core ``Window``.
    expected_decoded_type: ClassVar[type] = str

    #: Length of one downstream period for windows that accept ``step``;
    #: ``None`` for windows whose period length varies (month, quarter, year).
    period: ClassVar[timedelta | None] = None

    def __init__(
        self, *, direction: Window.Direction = Direction.FORWARD, step: timedelta | None = None
    ) -> None:
        self.direction = self.Direction(direction)
        self.step = _validate_step(type(self), step)

    def serialize(self) -> dict[str, Any]:
        data: dict[str, Any] = {"direction": self.direction.value}
        # Omitted when unset so Dags without a step keep their existing rollup fingerprint.
        if self.step is not None:
            data["step"] = int(self.step.total_seconds())
        return data

    @classmethod
    def deserialize(cls, data: dict[str, Any]) -> Window:
        step = data.get("step")
        return cls(
            direction=cls.Direction(data["direction"]),
            step=None if step is None else timedelta(seconds=step),
        )


class HourWindow(Window):
    """Consecutive keys making up one hour: sixty minute keys, or ``step`` apart when set."""

    expected_decoded_type: ClassVar[type] = datetime
    period: ClassVar[timedelta | None] = timedelta(hours=1)


class DayWindow(Window):
    """Consecutive keys making up one day: twenty-four hourly keys, or ``step`` apart when set."""

    expected_decoded_type: ClassVar[type] = datetime
    period: ClassVar[timedelta | None] = timedelta(days=1)


class WeekWindow(Window):
    """Consecutive keys making up one week: seven daily keys, or ``step`` apart when set."""

    expected_decoded_type: ClassVar[type] = datetime
    period: ClassVar[timedelta | None] = timedelta(weeks=1)


class MonthWindow(Window):
    """All daily keys making up one calendar month."""

    expected_decoded_type: ClassVar[type] = datetime


class QuarterWindow(Window):
    """Three consecutive monthly keys making up one calendar quarter."""

    expected_decoded_type: ClassVar[type] = datetime


class YearWindow(Window):
    """Twelve consecutive monthly keys making up one calendar year."""

    expected_decoded_type: ClassVar[type] = datetime


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

    Authoring marker for the scheduler-side
    :class:`airflow.partition_mappers.window.SegmentWindow`. Paired with
    :class:`~airflow.sdk.definitions.partition_mappers.fixed_key.FixedKeyMapper` inside a
    :class:`~airflow.sdk.definitions.partition_mappers.base.RollupMapper` to express a
    categorical rollup.

    Construction validates the segment list so Dag parse errors surface
    immediately rather than deferring to scheduler deserialization.

    :param segments: Non-empty iterable of non-empty string segment keys. Duplicates
        are silently de-duplicated.
    :raises ValueError: if *segments* is empty, contains a non-``str`` element, or
        contains an empty-string element.
    """

    expected_decoded_type: ClassVar[type] = str

    _segments: frozenset[str] = attrs.field(converter=_convert_segments)
