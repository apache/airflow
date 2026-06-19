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

import datetime
from typing import TYPE_CHECKING

from airflow._shared.timezones.timezone import convert_to_utc
from airflow.exceptions import AirflowTimetableInvalid

if TYPE_CHECKING:
    from dateutil.relativedelta import relativedelta
    from pendulum import DateTime


class DeltaMixin:
    """Mixin to provide interface to work with timedelta and relativedelta."""

    def __init__(self, delta: datetime.timedelta | relativedelta) -> None:
        self._delta = delta

    def __eq__(self, other: object) -> bool:
        """
        Return if the offsets match.

        This is only for testing purposes and should not be relied on otherwise.
        """
        from airflow.serialization.encoders import coerce_to_core_timetable

        if not isinstance(other := coerce_to_core_timetable(other), type(self)):
            return NotImplemented
        return self._delta == other._delta

    def __hash__(self):
        return hash(self._delta)

    @property
    def summary(self) -> str:
        return str(self._delta)

    def validate(self) -> None:
        now = datetime.datetime.now()
        if (now + self._delta) <= now:
            raise AirflowTimetableInvalid(f"schedule interval must be positive, not {self._delta!r}")

    def _get_next(self, current: DateTime) -> DateTime:
        return convert_to_utc(current + self._delta)

    def _get_prev(self, current: DateTime) -> DateTime:
        return convert_to_utc(current - self._delta)

    def _align_to_next(self, current: DateTime) -> DateTime:
        return current

    def _align_to_prev(self, current: DateTime) -> DateTime:
        return current

    def count_skipped_intervals_between(
        self,
        prev_interval_end: DateTime,
        new_interval_start: DateTime,
    ) -> int:
        if new_interval_start <= prev_interval_end:
            return 0
        period_seconds = self._schedule_period_seconds()
        if period_seconds <= 0:
            return 0
        gap_seconds = (new_interval_start - prev_interval_end).total_seconds()
        return int(gap_seconds // period_seconds)

    def _schedule_period_seconds(self) -> float:
        if isinstance(self._delta, datetime.timedelta):
            return self._delta.total_seconds()
        return float(self._relativedelta_in_seconds(self._delta))

    @staticmethod
    def _relativedelta_in_seconds(delta: relativedelta) -> int:
        return (
            delta.years * 365 * 24 * 60 * 60
            + delta.months * 30 * 24 * 60 * 60
            + delta.days * 24 * 60 * 60
            + delta.hours * 60 * 60
            + delta.minutes * 60
            + delta.seconds
        )
