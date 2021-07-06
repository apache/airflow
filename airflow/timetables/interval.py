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

import datetime
from typing import Any, Optional

from pendulum import DateTime

from airflow.timetables.base import DagRunInfo, TimeRestriction, Timetable
from airflow.timetables.schedules import CronSchedule, Delta, DeltaSchedule, Schedule


class _DataIntervalTimetable(Timetable):
    """Basis for timetable implementations that schedule data intervals.

    This kind of timetable classes create periodic data intervals from an
    underlying schedule representation (e.g. a cron expression, or a timedelta
    instance), and schedule a DagRun at the end of each interval.
    """

    _schedule: Schedule

    def __eq__(self, other: Any) -> bool:
        """Delegate to the schedule."""
        if not isinstance(other, _DataIntervalTimetable):
            return NotImplemented
        return self._schedule == other._schedule

    def validate(self) -> None:
        self._schedule.validate()

    def next_dagrun_info(
        self,
        last_automated_dagrun: Optional[DateTime],
        restriction: TimeRestriction,
    ) -> Optional[DagRunInfo]:
        earliest = restriction.earliest
        if not restriction.catchup:
            earliest = self._schedule.skip_to_latest(earliest)
        if last_automated_dagrun is None:
            # First run; schedule the run at the first available time matching
            # the schedule, and retrospectively create a data interval for it.
            if earliest is None:
                return None
            start = self._schedule.align(earliest)
        else:
            # There's a previous run. Create a data interval starting from when
            # the end of the previous interval.
            start = self._schedule.get_next(last_automated_dagrun)
        if restriction.latest is not None and start > restriction.latest:
            return None
        end = self._schedule.get_next(start)
        return DagRunInfo.interval(start=start, end=end)


class CronDataIntervalTimetable(_DataIntervalTimetable):
    """Timetable that schedules data intervals with a cron expression.

    This corresponds to ``schedule_interval=<cron>``, where ``<cron>`` is either
    a five/six-segment representation, or one of ``cron_presets``.

    Don't pass ``@once`` in here; use ``OnceTimetable`` instead.
    """

    def __init__(self, cron: str, timezone: datetime.tzinfo) -> None:
        self._schedule = CronSchedule(cron, timezone)


class DeltaDataIntervalTimetable(_DataIntervalTimetable):
    """Timetable that schedules data intervals with a time delta.

    This corresponds to ``schedule_interval=<delta>``, where ``<delta>`` is
    either a ``datetime.timedelta`` or ``dateutil.relativedelta.relativedelta``
    instance.
    """

    def __init__(self, delta: Delta) -> None:
        self._schedule = DeltaSchedule(delta)
