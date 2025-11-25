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

import attrs

from airflow.sdk.definitions.timetables._cron import CronMixin
from airflow.sdk.definitions.timetables._delta import DeltaMixin
from airflow.sdk.definitions.timetables.base import BaseTimetable

if TYPE_CHECKING:
    from dateutil.relativedelta import relativedelta
    from pendulum.tz.timezone import FixedTimezone, Timezone


@attrs.define
class DeltaTriggerTimetable(DeltaMixin, BaseTimetable):
    """
    Timetable that triggers DAG runs according to a cron expression.

    This is different from ``DeltaDataIntervalTimetable``, where the delta value
    specifies the *data interval* of a DAG run. With this timetable, the data
    intervals are specified independently. Also for the same reason, this
    timetable kicks off a DAG run immediately at the start of the period,
    instead of needing to wait for one data interval to pass.

    :param delta: How much time to wait between each run.
    :param interval: The data interval of each run. Default is 0.
    """

    interval: datetime.timedelta | relativedelta = attrs.field(kw_only=True, default=datetime.timedelta())


@attrs.define
class CronTriggerTimetable(CronMixin, BaseTimetable):
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

    interval: datetime.timedelta | relativedelta = attrs.field(kw_only=True, default=datetime.timedelta())
    run_immediately: bool | datetime.timedelta = attrs.field(kw_only=True, default=False)


@attrs.define(init=False)
class MultipleCronTriggerTimetable(BaseTimetable):
    """
    Timetable that triggers DAG runs according to multiple cron expressions.

    This combines multiple ``CronTriggerTimetable`` instances underneath, and
    triggers a DAG run whenever one of the timetables want to trigger a run.

    Only at most one run is triggered for any given time, even if more than one
    timetable fires at the same time.
    """

    timetables: list[CronTriggerTimetable]

    def __init__(
        self,
        *crons: str,
        timezone: str | Timezone | FixedTimezone,
        interval: datetime.timedelta | relativedelta = datetime.timedelta(),
        run_immediately: bool | datetime.timedelta = False,
    ) -> None:
        if not crons:
            raise ValueError("cron expression required")
        self.__attrs_init__(  # type: ignore[attr-defined]
            [
                CronTriggerTimetable(cron, timezone, interval=interval, run_immediately=run_immediately)
                for cron in crons
            ],
        )
