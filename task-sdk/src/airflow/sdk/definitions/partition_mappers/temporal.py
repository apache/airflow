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

from datetime import datetime
from typing import TYPE_CHECKING, ClassVar

import attrs

from airflow.sdk._shared.timezones.timezone import parse_timezone
from airflow.sdk.definitions.partition_mappers.base import PartitionMapper

if TYPE_CHECKING:
    from pendulum import FixedTimezone, Timezone

    from airflow.sdk.definitions.partition_mappers.window import Window


def _timezone_converter(value: str | Timezone | FixedTimezone) -> Timezone | FixedTimezone:
    if isinstance(value, str):
        return parse_timezone(value)
    return value


@attrs.define
class _BaseTemporalMapper(PartitionMapper):
    """Base class for Temporal Partition Mappers."""

    default_output_format: ClassVar[str]
    expected_decoded_type: ClassVar[type] = datetime

    _timezone: str | Timezone | FixedTimezone = attrs.field(
        alias="timezone",
        default="UTC",
        kw_only=True,
        converter=_timezone_converter,
    )
    input_format: str = attrs.field(default="%Y-%m-%dT%H:%M:%S", kw_only=True)
    output_format: str | None = attrs.field(default=None, kw_only=True)

    def __attrs_post_init__(self) -> None:
        if not self.output_format:
            self.output_format = self.default_output_format


class StartOfHourMapper(_BaseTemporalMapper):
    """
    Map a partition key to the start of the hour that contains the key.

    Example: ``2024-03-13T10:42:15`` → ``2024-03-13T10``.
    """

    default_output_format = "%Y-%m-%dT%H"


class StartOfDayMapper(_BaseTemporalMapper):
    """
    Map a partition key to the start of the day that contains the key.

    Example: ``2024-03-13T10:42:15`` → ``2024-03-13``.
    """

    default_output_format = "%Y-%m-%d"


class StartOfWeekMapper(_BaseTemporalMapper):
    """
    Map a partition key to the Monday of the ISO week that contains the key.

    Example: ``2024-03-13T10:42:15`` → ``2024-03-11 (W11)``.
    """

    default_output_format = "%Y-%m-%d (W%V)"


class StartOfMonthMapper(_BaseTemporalMapper):
    """
    Map a partition key to day 1 of the month that contains the key.

    Example: ``2024-03-13T10:42:15`` → ``2024-03``.
    """

    default_output_format = "%Y-%m"


class StartOfQuarterMapper(_BaseTemporalMapper):
    """
    Map a partition key to the first day of the calendar quarter that contains the key.

    Example: ``2024-03-13T10:42:15`` → ``2024-Q1``.
    """

    default_output_format = "%Y-Q{quarter}"


class StartOfYearMapper(_BaseTemporalMapper):
    """
    Map a partition key to January 1 of the year that contains the key.

    Example: ``2024-03-13T10:42:15`` → ``2024``.
    """

    default_output_format = "%Y"


@attrs.define(init=False)
class FanOutMapper(PartitionMapper):
    """
    Partition mapper that fans one upstream key out into multiple downstream keys.

    Compose an ``upstream_mapper`` (which parses the coarse upstream key and
    normalizes it to a period start) with a ``window`` that enumerates the
    members of that period. ``downstream_mapper`` formats each member into a
    downstream key string; if omitted, a default is chosen from the window
    class (e.g. ``WeekWindow`` → ``StartOfDayMapper``).

    ``downstream_mapper`` must be passed explicitly for any window without an
    entry in the default table — currently ``HourWindow`` and any custom
    ``Window`` subclass. Constructing a ``FanOutMapper`` for those windows
    without a ``downstream_mapper`` raises ``ValueError`` at Dag-parse time.

    Symmetric to :class:`~airflow.sdk.RollupMapper`: rollup is N→1 (downstream
    waits for all members), fan-out is 1→N (one upstream event creates many
    downstream Dag runs).

    For forward fan-out (emit the *next* period's members instead of the current
    one), pass ``direction=Window.Direction.FORWARD`` to the window:

    .. code-block:: python

        from airflow.sdk import WeekWindow, Window
        from airflow.sdk.definitions.partition_mappers.temporal import FanOutMapper, StartOfWeekMapper

        # Weekly upstream → 7 daily downstream Dag runs (current week)
        FanOutMapper(upstream_mapper=StartOfWeekMapper(), window=WeekWindow())

        # Weekly upstream → 7 daily keys for the *following* week
        forward_window = WeekWindow(direction=Window.Direction.FORWARD)
        FanOutMapper(upstream_mapper=StartOfWeekMapper(), window=forward_window)
    """

    # Keep ``FanOutMapper.default_downstream_mapper_by_window_name`` in sync with
    # the core copy in
    # ``airflow-core/src/airflow/partition_mappers/temporal.py`` —
    # the SDK and core class hierarchies are independent (the SDK cannot import
    # core), so both sides carry the same defaults and the lookup is by class
    # name. When adding a new ``Window`` subclass, extend the table on both
    # sides; a missing entry raises ``ValueError`` at ``FanOutMapper.__init__``
    # (see ``FanOutMapper._resolve_default_downstream_mapper``). The
    # ``check-partition-mapper-defaults-in-sync`` prek hook enforces that the
    # two tables stay identical.
    default_downstream_mapper_by_window_name: ClassVar[dict[str, type[_BaseTemporalMapper]]] = {
        "DayWindow": StartOfHourMapper,
        "WeekWindow": StartOfDayMapper,
        "MonthWindow": StartOfDayMapper,
        "QuarterWindow": StartOfMonthMapper,
        "YearWindow": StartOfMonthMapper,
    }

    upstream_mapper: PartitionMapper = attrs.field(kw_only=True)
    window: Window = attrs.field(kw_only=True)
    downstream_mapper: PartitionMapper = attrs.field(kw_only=True)

    def __init__(
        self,
        *,
        upstream_mapper: PartitionMapper,
        window: Window,
        downstream_mapper: PartitionMapper | None = None,
        max_downstream_keys: int | None = None,
    ) -> None:
        self.__attrs_init__(
            upstream_mapper=upstream_mapper,
            window=window,
            downstream_mapper=downstream_mapper or type(self)._resolve_default_downstream_mapper(window),
            max_downstream_keys=max_downstream_keys,
        )

    @classmethod
    def _resolve_default_downstream_mapper(cls, window: Window) -> PartitionMapper:
        """
        Return the conventional downstream mapper for *window*.

        Looked up by the window's class **name** rather than identity so that
        the SDK ``Window`` classes (used in Dag-author code) and the core
        ``Window`` classes (used after deserialization) both resolve to the
        same default. Subclasses can extend or override the defaults by
        setting :attr:`default_downstream_mapper_by_window_name` on the subclass.
        """
        mapper_cls = cls.default_downstream_mapper_by_window_name.get(type(window).__name__)
        if mapper_cls is None:
            raise ValueError(
                f"{cls.__name__} has no default downstream_mapper for window type "
                f"{type(window).__name__}; pass downstream_mapper explicitly."
            )
        return mapper_cls()
