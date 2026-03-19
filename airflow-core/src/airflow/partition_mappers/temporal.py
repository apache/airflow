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
from typing import TYPE_CHECKING, Any

from airflow._shared.timezones.timezone import parse_timezone
from airflow.partition_mappers.base import PartitionMapper

if TYPE_CHECKING:
    from pendulum import FixedTimezone, Timezone


class _BaseTemporalMapper(PartitionMapper, ABC):
    """Base class for Temporal Partition Mappers."""

    default_output_format: str

    def __init__(
        self,
        *,
        timezone: str | Timezone | FixedTimezone,
        input_format: str = "%Y-%m-%dT%H:%M:%S%z",
        output_format: str | None = None,
    ):
        self.input_format = input_format
        self.output_format = output_format or self.default_output_format
        if isinstance(timezone, str):
            timezone = parse_timezone(timezone)
        self._timezone: Timezone | FixedTimezone = timezone

    def to_downstream(self, key: str) -> str:
        dt = datetime.strptime(key, self.input_format)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=self._timezone)
        else:
            dt = dt.astimezone(self._timezone)

        normalized = self.normalize(dt)
        return self.format(normalized)

    @abstractmethod
    def normalize(self, dt: datetime) -> datetime:
        """Return canonical start datetime for the partition."""

    def format(self, dt: datetime) -> str:
        """Format the normalized datetime."""
        return dt.strftime(self.output_format)

    def serialize(self) -> dict[str, Any]:
        from airflow.serialization.encoders import encode_timezone

        return {
            "timezone": encode_timezone(self._timezone),
            "input_format": self.input_format,
            "output_format": self.output_format,
        }

    @classmethod
    def deserialize(cls, data: dict[str, Any]) -> PartitionMapper:
        return cls(
            timezone=parse_timezone(data["timezone"]),
            input_format=data["input_format"],
            output_format=data["output_format"],
        )


class HourlyMapper(_BaseTemporalMapper):
    """Map a time-based partition key to hour."""

    default_output_format = "%Y-%m-%dT%H%z"

    def normalize(self, dt: datetime) -> datetime:
        return dt.replace(minute=0, second=0, microsecond=0)


class DailyMapper(_BaseTemporalMapper):
    """Map a time-based partition key to day."""

    default_output_format = "%Y-%m-%d%z"

    def normalize(self, dt: datetime) -> datetime:
        return dt.replace(hour=0, minute=0, second=0, microsecond=0)


class WeeklyMapper(_BaseTemporalMapper):
    """Map a time-based partition key to week."""

    default_output_format = "%Y-%m-%d (W%V)%z"

    def normalize(self, dt: datetime) -> datetime:
        start = dt - timedelta(days=dt.weekday())
        return start.replace(hour=0, minute=0, second=0, microsecond=0)


class MonthlyMapper(_BaseTemporalMapper):
    """Map a time-based partition key to month."""

    default_output_format = "%Y-%m%z"

    def normalize(self, dt: datetime) -> datetime:
        return dt.replace(
            day=1,
            hour=0,
            minute=0,
            second=0,
            microsecond=0,
        )


class QuarterlyMapper(_BaseTemporalMapper):
    """Map a time-based partition key to quarter."""

    default_output_format = "%Y-Q{quarter}%z"

    def normalize(self, dt: datetime) -> datetime:
        quarter = (dt.month - 1) // 3
        month = quarter * 3 + 1
        return dt.replace(
            month=month,
            day=1,
            hour=0,
            minute=0,
            second=0,
            microsecond=0,
        )

    def format(self, dt: datetime) -> str:
        quarter = (dt.month - 1) // 3 + 1
        return dt.strftime(self.output_format).format(quarter=quarter)


class YearlyMapper(_BaseTemporalMapper):
    """Map a time-based partition key to year."""

    default_output_format = "%Y%z"

    def normalize(self, dt: datetime) -> datetime:
        return dt.replace(
            month=1,
            day=1,
            hour=0,
            minute=0,
            second=0,
            microsecond=0,
        )
