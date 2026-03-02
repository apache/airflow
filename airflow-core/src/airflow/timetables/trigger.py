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
import functools
import math
import operator
import time
from types import NoneType
from typing import TYPE_CHECKING, Any

import structlog

from airflow._shared.timezones import timezone
from airflow._shared.timezones.timezone import coerce_datetime, parse_timezone, utcnow
from airflow.timetables._cron import CronMixin
from airflow.timetables._delta import DeltaMixin
from airflow.timetables.base import DagRunInfo, DataInterval, Timetable
from airflow.utils.strings import get_random_string

if TYPE_CHECKING:
    from dateutil.relativedelta import relativedelta
    from pendulum import DateTime
    from pendulum.tz.timezone import FixedTimezone, Timezone

    from airflow.models.dag import DagModel
    from airflow.models.dagrun import DagRun
    from airflow.timetables.base import TimeRestriction
    from airflow.utils.types import DagRunType

log = structlog.get_logger()


class _TriggerTimetable(Timetable):
    _interval: datetime.timedelta | relativedelta

    def infer_manual_data_interval(self, *, run_after: DateTime) -> DataInterval:
        return DataInterval(
            coerce_datetime(run_after - self._interval),
            run_after,
        )

    def _calc_first_run(self) -> DateTime:
        """
        If no start_time is set, determine the start.

        If True, always prefer past run, if False, never. If None, if within 10% of next run,
        if timedelta, if within that timedelta from past run.
        """
        raise NotImplementedError()

    def _align_to_next(self, current: DateTime) -> DateTime:
        raise NotImplementedError()

    def _align_to_prev(self, current: DateTime) -> DateTime:
        raise NotImplementedError()

    def _get_next(self, current: DateTime) -> DateTime:
        raise NotImplementedError()

    def _get_prev(self, current: DateTime) -> DateTime:
        raise NotImplementedError()

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
            coerce_datetime(next_start_time - self._interval),
            next_start_time,
        )


class DeltaTriggerTimetable(DeltaMixin, _TriggerTimetable):
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

    def __init__(
        self,
        delta: datetime.timedelta | relativedelta,
        *,
        interval: datetime.timedelta | relativedelta = datetime.timedelta(),
    ) -> None:
        super().__init__(delta)
        self._interval = interval

    @classmethod
    def deserialize(cls, data: dict[str, Any]) -> Timetable:
        from airflow.serialization.decoders import decode_interval

        return cls(
            decode_interval(data["delta"]),
            interval=decode_interval(data["interval"]),
        )

    def serialize(self) -> dict[str, Any]:
        from airflow.serialization.encoders import encode_interval

        return {
            "delta": encode_interval(self._delta),
            "interval": encode_interval(self._interval),
        }

    def _calc_first_run(self) -> DateTime:
        return self._align_to_prev(coerce_datetime(utcnow()))


class CronTriggerTimetable(CronMixin, _TriggerTimetable):
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
        self._run_immediately = run_immediately

    @classmethod
    def deserialize(cls, data: dict[str, Any]) -> Timetable:
        from airflow.serialization.decoders import decode_interval, decode_run_immediately

        return cls(
            data["expression"],
            timezone=parse_timezone(data["timezone"]),
            interval=decode_interval(data["interval"]),
            run_immediately=decode_run_immediately(data.get("run_immediately", False)),
        )

    def serialize(self) -> dict[str, Any]:
        from airflow.serialization.encoders import encode_interval, encode_run_immediately, encode_timezone

        return {
            "expression": self._expression,
            "timezone": encode_timezone(self._timezone),
            "interval": encode_interval(self._interval),
            "run_immediately": encode_run_immediately(self._run_immediately),
        }

    def _calc_first_run(self) -> DateTime:
        """
        If no start_time is set, determine the start.

        If True, always prefer past run, if False, never. If None, if within 10% of next run,
        if timedelta, if within that timedelta from past run.
        """
        now = coerce_datetime(utcnow())
        past_run_time = self._align_to_prev(now)
        next_run_time = self._align_to_next(now)
        if self._run_immediately is True:  # Check for 'True' exactly because deltas also evaluate to true.
            return past_run_time

        gap_between_runs = next_run_time - past_run_time
        gap_to_past = now - past_run_time
        if isinstance(self._run_immediately, datetime.timedelta):
            buffer_between_runs = self._run_immediately
        else:
            buffer_between_runs = max(gap_between_runs / 10, datetime.timedelta(minutes=5))
        if gap_to_past <= buffer_between_runs:
            return past_run_time
        return next_run_time


class MultipleCronTriggerTimetable(Timetable):
    """
    Timetable that triggers Dag runs according to multiple cron expressions.

    This combines multiple ``CronTriggerTimetable`` instances underneath, and
    triggers a Dag run whenever one of the timetables want to trigger a run.

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
        from airflow.serialization.decoders import decode_interval, decode_run_immediately

        return cls(
            *data["expressions"],
            timezone=parse_timezone(data["timezone"]),
            interval=decode_interval(data["interval"]),
            run_immediately=decode_run_immediately(data["run_immediately"]),
        )

    def serialize(self) -> dict[str, Any]:
        from airflow.serialization.encoders import encode_interval, encode_run_immediately, encode_timezone

        # All timetables share the same timezone, interval, and run_immediately
        # values, so we can just use the first to represent them.
        timetable = self._timetables[0]
        return {
            "expressions": [t._expression for t in self._timetables],
            "timezone": encode_timezone(timetable._timezone),
            "interval": encode_interval(timetable._interval),
            "run_immediately": encode_run_immediately(timetable._run_immediately),
        }

    @property
    def summary(self) -> str:
        return ", ".join(t.summary for t in self._timetables)

    def infer_manual_data_interval(self, *, run_after: DateTime) -> DataInterval:
        intervals = (t.infer_manual_data_interval(run_after=run_after) for t in self._timetables)
        return min(
            (x for x in intervals if x),
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
        if restriction.catchup:
            select_key = self._dagrun_info_sort_key_catchup
        else:
            select_key = functools.partial(self._dagrun_info_sort_key_no_catchup, now=time.time())
        return min(infos, key=select_key)

    @staticmethod
    def _dagrun_info_sort_key_catchup(info: DagRunInfo | None) -> float:
        """
        Sort key for DagRunInfo values when catchup=True.

        This is passed as the sort key to ``min`` in ``next_dagrun_info`` to
        find the next closest run, ordered by logical date.

        The sort is done by simply returning the logical date converted to a
        Unix timestamp. If the input is *None* (no next run), *inf* is returned
        so it's selected last.
        """
        if info is None or info.logical_date is None:
            return math.inf
        return info.logical_date.timestamp()

    @staticmethod
    def _dagrun_info_sort_key_no_catchup(info: DagRunInfo | None, *, now: float) -> float:
        """
        Sort key for DagRunInfo values when catchup=False.

        When catchup is disabled, we want to ignore as many runs as possible
        without going over the current time, but if no runs should happen right
        now, we want to choose the earliest opportunity.

        Combining with the ``min`` sorter in ``next_dagrun_info``, we should
        order values by ``-logical_date`` if they are earlier than or at current
        time, but ``+logical_date`` if later.
        """
        if info is None or info.logical_date is None:
            return math.inf
        if (ts := info.logical_date.timestamp()) <= now:
            return -ts
        return ts


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

    # todo: AIP-76 talk about how we can have auto-reprocessing of partitions
    # todo: AIP-76 we could allow a tuple of integer + time-based

    """

    def __init__(
        self,
        cron: str,
        *,
        timezone: str | Timezone | FixedTimezone,
        run_offset: int | datetime.timedelta | relativedelta | None = None,
        run_immediately: bool | datetime.timedelta = False,
        key_format: str = "%Y-%m-%dT%H:%M:%S",  # todo: AIP-76 we can't infer partition date from this, so we need to store it separately
    ) -> None:
        super().__init__(cron, timezone=timezone, run_immediately=run_immediately)
        if not isinstance(run_offset, (int, NoneType)):
            # todo: AIP-76 implement timedelta / relative delta?
            raise ValueError("Run offset other than integer not supported yet.")
        self._run_offset = run_offset or 0
        self._key_format = key_format

    @classmethod
    def deserialize(cls, data: dict[str, Any]) -> Timetable:
        from airflow.serialization.decoders import decode_run_immediately

        offset = data["run_offset"]
        if not isinstance(offset, (int, NoneType)):
            offset = None
            log.warning(
                "Unexpected offset type on deserialization. Only int supported in this version.",
                run_offset=offset,
            )

        return cls(
            cron=data["expression"],
            timezone=parse_timezone(data["timezone"]),
            run_offset=offset,
            run_immediately=decode_run_immediately(data.get("run_immediately", False)),
            key_format=data["key_format"],
        )

    def serialize(self) -> dict[str, Any]:
        from airflow.serialization.encoders import encode_run_immediately, encode_timezone

        return {
            "expression": self._expression,
            "timezone": encode_timezone(self._timezone),
            "run_immediately": encode_run_immediately(self._run_immediately),
            "run_offset": self._run_offset,
            "key_format": self._key_format,
        }

    def _get_partition_date(self, *, run_date) -> DateTime:
        if self._run_offset == 0:
            return run_date
        # we will need to apply offset to determine run date
        partition_date = timezone.coerce_datetime(run_date)
        log.info(
            "applying offset to partition date", partition_date=partition_date, run_offset=self._run_offset
        )
        iter_func = self._get_next if self._run_offset > 0 else self._get_prev
        for _ in range(abs(self._run_offset)):
            partition_date = iter_func(partition_date)
        log.info("new partition date", partition_date=partition_date)
        return partition_date

    def next_dagrun_info_v2(
        self,
        *,
        last_dagrun_info: DagRunInfo | None,
        restriction: TimeRestriction,
    ) -> DagRunInfo | None:
        # todo: AIP-76 add test for this logic
        # todo: AIP-76 we will have to ensure that the start / end times apply to the partition date ideally,
        #  rather than just the run after

        if restriction.catchup:
            if last_dagrun_info is not None:
                next_start_time = self._get_next(last_dagrun_info.run_after)
            elif restriction.earliest is None:
                next_start_time = self._calc_first_run()
            else:
                next_start_time = self._align_to_next(restriction.earliest)
        else:
            prev_candidate = self._align_to_prev(coerce_datetime(utcnow()))
            start_time_candidates = [prev_candidate]
            if last_dagrun_info is not None:
                next_candidate = self._get_next(last_dagrun_info.run_after)
                start_time_candidates.append(next_candidate)
            elif restriction.earliest is None:
                # Run immediately has no effect if there is restriction on earliest
                first_run = self._calc_first_run()
                start_time_candidates.append(first_run)
            if restriction.earliest is not None:
                earliest = self._align_to_next(restriction.earliest)
                start_time_candidates.append(earliest)
            next_start_time = max(start_time_candidates)
        if restriction.latest is not None and restriction.latest < next_start_time:
            return None

        partition_date, partition_key = self._get_partition_info(run_date=next_start_time)
        return DagRunInfo(
            run_after=next_start_time,
            partition_date=partition_date,
            partition_key=partition_key,
            data_interval=None,
        )

    def _get_partition_info(self, run_date: DateTime) -> tuple[DateTime, str]:
        # todo: AIP-76 it does not make sense that we would infer partition info from run date
        #  in general, because they might not be 1-1
        partition_date = self._get_partition_date(run_date=run_date)
        partition_key = self._format_key(partition_date)
        return partition_date, partition_key

    def _format_key(self, partition_date: DateTime) -> str:
        return partition_date.strftime(self._key_format)

    def generate_run_id(
        self,
        *,
        run_type: DagRunType,
        run_after: DateTime,
        data_interval: DataInterval | None,
        **extra,
    ) -> str:
        suffix = run_after.isoformat()
        if partition_key := extra.get("partition_key"):
            suffix = f"{suffix}__{partition_key}"
        suffix = f"{suffix}__{get_random_string()}"
        return run_type.generate_run_id(suffix=suffix)

    def next_run_info_from_dag_model(self, *, dag_model: DagModel) -> DagRunInfo:
        run_after = timezone.coerce_datetime(dag_model.next_dagrun_create_after)
        if TYPE_CHECKING:
            assert run_after is not None
        partition_date, partition_key = self._get_partition_info(run_date=run_after)
        return DagRunInfo(
            run_after=run_after,
            data_interval=None,
            partition_date=partition_date,
            partition_key=partition_key,
        )

    def run_info_from_dag_run(self, *, dag_run: DagRun) -> DagRunInfo:
        run_after = timezone.coerce_datetime(dag_run.run_after)
        # todo: AIP-76 store this on DagRun so we don't need to recalculate?
        # todo: AIP-76 this needs to be public
        partition_date = self._get_partition_date(run_date=dag_run.run_after)
        return DagRunInfo(
            run_after=run_after,
            data_interval=None,
            partition_date=partition_date,
            partition_key=dag_run.partition_key,
        )
