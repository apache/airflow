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

from airflow.sdk._shared.timezones.timezone import parse_timezone
from airflow.sdk.definitions.partition_mappers.base import PartitionMapper

if TYPE_CHECKING:
    from pendulum import FixedTimezone, Timezone


class _BaseTemporalMapper(PartitionMapper):
    """Base class for Temporal Partition Mappers."""

    default_output_format: str
    expected_decoded_type: ClassVar[type] = datetime

    def __init__(
        self,
        *,
        timezone: str | Timezone | FixedTimezone = "UTC",
        input_format: str = "%Y-%m-%dT%H:%M:%S",
        output_format: str | None = None,
    ) -> None:
        self.input_format = input_format
        self.output_format = output_format or self.default_output_format
        if isinstance(timezone, str):
            timezone = parse_timezone(timezone)
        self._timezone = timezone


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
