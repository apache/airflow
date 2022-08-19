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
#
from __future__ import annotations

import datetime
from typing import Any

from dateutil.relativedelta import relativedelta
from pendulum import DateTime
from pendulum.tz.timezone import Timezone

from airflow.timetables._cron import CronMixin
from airflow.timetables.base import DagRunInfo, DataInterval, TimeRestriction, Timetable


class CronTriggerTimetable(CronMixin, Timetable):
    """Timetable that triggers DAG runs according to a cron expression.

    This is different from ``CronDataIntervalTimetable``, where the cron
    expression specifies the *data interval* of a DAG run. With this timetable,
    the data intervals are specified independently from the cron expression.
    Also for the same reason, this timetable kicks off a DAG run immediately at
    the start of the period (similar to POSIX cron), instead of needing to wait
    for one data interval to pass.

    Don't pass ``@once`` in here; use ``OnceTimetable`` instead.
    """

    def __init__(
        self,
        cron: str,
        *,
        timezone: str | Timezone,
        interval: datetime.timedelta | relativedelta = datetime.timedelta(),
    ) -> None:
        super().__init__(cron, timezone)
        self._interval = interval

    @classmethod
    def deserialize(cls, data: dict[str, Any]) -> Timetable:
        from airflow.serialization.serialized_objects import decode_relativedelta, decode_timezone

        interval: datetime.timedelta | relativedelta
        if isinstance(data["interval"], dict):
            interval = decode_relativedelta(data["interval"])
        else:
            interval = datetime.timedelta(seconds=data["interval"])
        return cls(data["expression"], timezone=decode_timezone(data["timezone"]), interval=interval)

    def serialize(self) -> dict[str, Any]:
        from airflow.serialization.serialized_objects import encode_relativedelta, encode_timezone

        interval: float | dict[str, Any]
        if isinstance(self._interval, datetime.timedelta):
            interval = self._interval.total_seconds()
        else:
            interval = encode_relativedelta(self._interval)
        timezone = encode_timezone(self._timezone)
        return {"expression": self._expression, "timezone": timezone, "interval": interval}

    def infer_manual_data_interval(self, *, run_after: DateTime) -> DataInterval:
        return DataInterval(run_after - self._interval, run_after)

    def next_dagrun_info(
        self,
        *,
        last_automated_data_interval: DataInterval | None,
        restriction: TimeRestriction,
    ) -> DagRunInfo | None:
        if restriction.catchup:
            if last_automated_data_interval is None:
                if restriction.earliest is None:
                    return None
                next_start_time = self._align_to_next(restriction.earliest)
            else:
                next_start_time = self._get_next(last_automated_data_interval.end)
        else:
            current_time = DateTime.utcnow()
            if restriction.earliest is not None and current_time < restriction.earliest:
                next_start_time = self._align_to_next(restriction.earliest)
            else:
                next_start_time = self._align_to_next(current_time)
        if restriction.latest is not None and restriction.latest < next_start_time:
            return None
        return DagRunInfo.interval(next_start_time - self._interval, next_start_time)
