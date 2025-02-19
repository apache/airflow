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
import math
import operator
from typing import TYPE_CHECKING, Any

from airflow.timetables._cron import CronMixin
from airflow.timetables.base import DagRunInfo, DataInterval, Timetable
from airflow.utils.timezone import coerce_datetime, utcnow

if TYPE_CHECKING:
    from dateutil.relativedelta import relativedelta
    from pendulum import DateTime
    from pendulum.tz.timezone import FixedTimezone, Timezone

    from airflow.timetables.base import TimeRestriction


def _serialize_interval(interval: datetime.timedelta | relativedelta) -> float | dict:
    from airflow.serialization.serialized_objects import encode_relativedelta

    if isinstance(interval, datetime.timedelta):
        return interval.total_seconds()
    return encode_relativedelta(interval)


def _deserialize_interval(value: int | dict) -> datetime.timedelta | relativedelta:
    from airflow.serialization.serialized_objects import decode_relativedelta

    if isinstance(value, dict):
        return decode_relativedelta(value)
    return datetime.timedelta(seconds=value)


def _serialize_run_immediately(value: bool | datetime.timedelta) -> bool | float:
    if isinstance(value, datetime.timedelta):
        return value.total_seconds()
    return value


def _deserialize_run_immediately(value: bool | float) -> bool | datetime.timedelta:
    if isinstance(value, float):
        return datetime.timedelta(seconds=value)
    return value


class CronTriggerTimetable(CronMixin, Timetable):
    """
    Timetable that triggers DAG runs according to a cron expression.

    This is different from ``CronDataIntervalTimetable``, where the cron
    expression specifies the *data interval* of a DAG run. With this timetable,
    the data intervals are specified independently from the cron expression.
    Also for the same reason, this timetable kicks off a DAG run immediately at
    the start of the period (similar to POSIX cron), instead of needing to wait
    for one data interval to pass.

    Don't pass ``@once`` in here; use ``OnceTimetable`` instead.

    :param cron: cron string that defines when to run
    :param timezone: Which timezone to use to interpret the cron string
    :param interval: timedelta that defines the data interval start. Default 0.

    *run_immediately* controls, if no *start_time* is given to the DAG, when
    the first run of the DAG should be scheduled. It has no effect if there
    already exist runs for this DAG.

    * If *True*, always run immediately the most recent possible DAG run.
    * If *False*, wait to run until the next scheduled time in the future.
    * If passed a ``timedelta``, will run the most recent possible DAG run
      if that run's ``data_interval_end`` is within timedelta of now.
    * If *None*, the timedelta is calculated as 10% of the time between the
      most recent past scheduled time and the next scheduled time. E.g. if
      running every hour, this would run the previous time if less than 6
      minutes had past since the previous run time, otherwise it would wait
      until the next hour.
    """

    def __init__(
        self,
        cron: str,
        *,
        timezone: str | Timezone | FixedTimezone,
        interval: datetime.timedelta | relativedelta = datetime.timedelta(),
        run_immediately: bool | datetime.timedelta = False,
    ) -> None:
        super().__init__(cron, timezone)
        self._interval = interval
        self.run_immediately = run_immediately

    @classmethod
    def deserialize(cls, data: dict[str, Any]) -> Timetable:
        from airflow.serialization.serialized_objects import decode_timezone

        return cls(
            data["expression"],
            timezone=decode_timezone(data["timezone"]),
            interval=_deserialize_interval(data["interval"]),
            run_immediately=_deserialize_run_immediately(data.get("run_immediately", False)),
        )

    def serialize(self) -> dict[str, Any]:
        from airflow.serialization.serialized_objects import encode_timezone

        return {
            "expression": self._expression,
            "timezone": encode_timezone(self._timezone),
            "interval": _serialize_interval(self._interval),
            "run_immediately": _serialize_run_immediately(self.run_immediately),
        }

    def infer_manual_data_interval(self, *, run_after: DateTime) -> DataInterval:
        return DataInterval(
            # pendulum.Datetime ± timedelta should return pendulum.Datetime
            # however mypy decide that output would be datetime.datetime
            run_after - self._interval,  # type: ignore[arg-type]
            run_after,
        )

    def next_dagrun_info(
        self,
        *,
        last_automated_data_interval: DataInterval | None,
        restriction: TimeRestriction,
    ) -> DagRunInfo | None:
        if restriction.catchup:
            if last_automated_data_interval is not None:
                next_start_time = self._get_next(last_automated_data_interval.end)
            elif restriction.earliest is None:
                next_start_time = self._calc_first_run()
            else:
                next_start_time = self._align_to_next(restriction.earliest)
        else:
            start_time_candidates = [self._align_to_prev(coerce_datetime(utcnow()))]
            if last_automated_data_interval is not None:
                start_time_candidates.append(self._get_next(last_automated_data_interval.end))
            elif restriction.earliest is None:
                # Run immediately has no effect if there is restriction on earliest
                start_time_candidates.append(self._calc_first_run())
            if restriction.earliest is not None:
                start_time_candidates.append(self._align_to_next(restriction.earliest))
            next_start_time = max(start_time_candidates)
        if restriction.latest is not None and restriction.latest < next_start_time:
            return None
        return DagRunInfo.interval(
            # pendulum.Datetime ± timedelta should return pendulum.Datetime
            # however mypy decide that output would be datetime.datetime
            next_start_time - self._interval,  # type: ignore[arg-type]
            next_start_time,
        )

    def _calc_first_run(self):
        """
        If no start_time is set, determine the start.

        If True, always prefer past run, if False, never. If None, if within 10% of next run,
        if timedelta, if within that timedelta from past run.
        """
        now = coerce_datetime(utcnow())
        past_run_time = self._align_to_prev(now)
        next_run_time = self._align_to_next(now)
        if self.run_immediately is True:  # not truthy, actually set to True
            return past_run_time

        gap_between_runs = next_run_time - past_run_time
        gap_to_past = now - past_run_time
        if isinstance(self.run_immediately, datetime.timedelta):
            buffer_between_runs = self.run_immediately
        else:
            buffer_between_runs = max(gap_between_runs / 10, datetime.timedelta(minutes=5))
        if gap_to_past <= buffer_between_runs:
            return past_run_time
        else:
            return next_run_time


class MultipleCronTriggerTimetable(Timetable):
    """
    Timetable that triggers DAG runs according to multiple cron expressions.

    This combines multiple ``CronTriggerTimetable`` instances underneath, and
    triggers a DAG run whenever one of the timetables want to trigger a run.

    Only at most one run is triggered for any given time, even if more than one
    timetable fires at the same time.
    """

    def __init__(
        self,
        *crons: str,
        timezone: str | Timezone | FixedTimezone,
        interval: datetime.timedelta | relativedelta = datetime.timedelta(),
        run_immediately: bool | datetime.timedelta = False,
    ) -> None:
        if not crons:
            raise ValueError("cron expression required")
        self._timetables = [
            CronTriggerTimetable(cron, timezone=timezone, interval=interval, run_immediately=run_immediately)
            for cron in crons
        ]
        self.description = ", ".join(t.description for t in self._timetables)

    @classmethod
    def deserialize(cls, data: dict[str, Any]) -> Timetable:
        from airflow.serialization.serialized_objects import decode_timezone

        return cls(
            *data["expressions"],
            timezone=decode_timezone(data["timezone"]),
            interval=_deserialize_interval(data["interval"]),
            run_immediately=_deserialize_run_immediately(data["run_immediately"]),
        )

    def serialize(self) -> dict[str, Any]:
        from airflow.serialization.serialized_objects import encode_timezone

        # All timetables share the same timezone, interval, and run_immediately
        # values, so we can just use the first to represent them.
        timetable = self._timetables[0]
        return {
            "expressions": [t._expression for t in self._timetables],
            "timezone": encode_timezone(timetable._timezone),
            "interval": _serialize_interval(timetable._interval),
            "run_immediately": _serialize_run_immediately(timetable.run_immediately),
        }

    @property
    def summary(self) -> str:
        return ", ".join(t.summary for t in self._timetables)

    def infer_manual_data_interval(self, *, run_after: DateTime) -> DataInterval:
        return min(
            (t.infer_manual_data_interval(run_after=run_after) for t in self._timetables),
            key=operator.attrgetter("start"),
        )

    def next_dagrun_info(
        self,
        *,
        last_automated_data_interval: DataInterval | None,
        restriction: TimeRestriction,
    ) -> DagRunInfo | None:
        infos = (
            timetable.next_dagrun_info(
                last_automated_data_interval=last_automated_data_interval,
                restriction=restriction,
            )
            for timetable in self._timetables
        )
        return min(infos, key=self._dagrun_info_sort_key)

    @staticmethod
    def _dagrun_info_sort_key(info: DagRunInfo | None) -> float:
        """
        Sort key for DagRunInfo values.

        This is passed as the sort key to ``min`` in ``next_dagrun_info`` to
        find the next closest run, ordered by logical date.

        The sort is done by simply returning the logical date converted to a
        Unix timestamp. If the input is *None* (no next run), *inf* is returned
        so it's selected last.
        """
        if info is None:
            return math.inf
        return info.logical_date.timestamp()
