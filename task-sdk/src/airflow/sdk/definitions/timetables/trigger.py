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
from types import NoneType
from typing import TYPE_CHECKING

import attrs

from airflow.sdk.bases.timetable import BaseTimetable
from airflow.sdk.definitions.timetables._cron import CronMixin
from airflow.sdk.definitions.timetables._delta import DeltaMixin

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
    Timetable that triggers Dag runs according to a cron expression.

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

    *run_immediately* controls, if no *start_time* is given to the Dag, when
    the first run of the Dag should be scheduled. It has no effect if there
    already exist runs for this Dag.

    * If *True*, always run immediately the most recent possible Dag run.
    * If *False*, wait to run until the next scheduled time in the future.
    * If passed a ``timedelta``, will run the most recent possible Dag run
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
    Timetable that triggers Dag runs according to multiple cron expressions.

    This combines multiple ``CronTriggerTimetable`` instances underneath, and
    triggers a Dag run whenever one of the timetables want to trigger a run.

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


@attrs.define
class CronPartitionTimetable(CronTriggerTimetable):
    """
    Timetable that triggers Dag runs according to a cron expression.

    Creates runs for partition keys.

    The cron expression determines the sequence of run dates. And
    the partition dates are derived from those according to the ``run_offset``.
    The partition key is then formatted using the partition date.

    A ``run_offset`` of 1 means the partition_date will be one cron interval
    after the run date; negative means the partition date will be one cron
    interval prior to the run date.

    :param cron: cron string that defines when to run
    :param timezone: Which timezone to use to interpret the cron string
    :param run_offset: Integer offset that determines which partition date to run for.
        The partition key will be derived from the partition date.
    :param key_format: How to translate the partition date into a string partition key.

    *run_immediately* controls, if no *start_time* is given to the Dag, when
    the first run of the Dag should be scheduled. It has no effect if there already exist runs for this Dag.

    * If *True*, always run immediately the most recent possible Dag run.
    * If *False*, wait to run until the next scheduled time in the future.
    * If passed a ``timedelta``, will run the most recent possible Dag run
      if that run's ``data_interval_end`` is within timedelta of now.
    * If *None*, the timedelta is calculated as 10% of the time between the
      most recent past scheduled time and the next scheduled time. E.g. if
      running every hour, this would run the previous time if less than 6
      minutes had past since the previous run time, otherwise it would wait
      until the next hour.

    # todo: AIP-76 talk about how we can have auto-reprocessing of partitions
    # todo: AIP-76 we could allow a tuple of integer + time-based

    """

    run_offset: int | datetime.timedelta | relativedelta | None = None
    key_format: str = "%Y-%m-%dT%H:%M:%S"  # todo: AIP-76 we can't infer partition date from this, so we need to store it separately

    def __init__(
        self,
        cron: str,
        *,
        timezone: str | Timezone | FixedTimezone,
        run_offset: int | datetime.timedelta | relativedelta | None = None,
        run_immediately: bool | datetime.timedelta = False,
        key_format: str = "%Y-%m-%dT%H:%M:%S",  # todo: AIP-76 we can't infer partition date from this, so we need to store it separately
    ) -> None:
        # super().__init__(cron, timezone=timezone, run_immediately=run_immediately)
        if not isinstance(run_offset, (int, NoneType)):
            # todo: AIP-76 implement timedelta / relative delta?
            raise ValueError("Run offset other than integer not supported yet.")
        self.__attrs_init__(  # type: ignore[attr-defined]
            cron,
            timezone=timezone,
            run_offset=run_offset,
            run_immediately=run_immediately,
            key_format=key_format,
        )
