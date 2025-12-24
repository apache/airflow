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
from typing import TYPE_CHECKING, Any

import structlog

from airflow._shared.timezones.timezone import coerce_datetime, parse_timezone, utcnow
from airflow.timetables._cron import CronMixin
from airflow.timetables._delta import DeltaMixin
from airflow.timetables.base import DagRunInfo, DataInterval, Timetable
from airflow.utils.strings import get_random_string

if TYPE_CHECKING:
    from dateutil.relativedelta import relativedelta
    from pendulum import DateTime
    from pendulum.tz.timezone import FixedTimezone, Timezone

    from airflow.timetables.base import TimeRestriction
    from airflow.utils.types import DagRunType

log = structlog.get_logger()


class _TriggerTimetable(Timetable):
    _interval: datetime.timedelta | relativedelta

    def __init__(self, *args, partitions: bool | None = None, **kwargs):
        self.partitions: bool | None = partitions

    def infer_manual_data_interval(self, *, run_after: DateTime) -> DataInterval | None:
        if self.partitions:
            return None
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
        partitions: bool = False,
    ) -> None:
        super().__init__(cron, timezone, partitions=partitions)
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
            partitions=data["partitions"],  # todo: AIP-76 not this
        )

    def serialize(self) -> dict[str, Any]:
        from airflow.serialization.encoders import encode_interval, encode_run_immediately, encode_timezone

        return {
            "expression": self._expression,
            "timezone": encode_timezone(self._timezone),
            "interval": encode_interval(self._interval),
            "run_immediately": encode_run_immediately(self._run_immediately),
            "partitions": self.partitions,
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
        from airflow.serialization.decoders import decode_interval, decode_run_immediately

        return cls(
            *data["expressions"],
            timezone=parse_timezone(data["timezone"]),
            interval=decode_interval(data["interval"]),
            run_immediately=decode_run_immediately(data["run_immediately"]),
        )

    def serialize(self) -> dict[str, Any]:
        from airflow.serialization.encoders import encode_interval, encode_run_immediately, encode_timezone

        # All cron timetables share the same timezone, interval, and run_immediately
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

    def infer_manual_data_interval(self, *, run_after: DateTime) -> DataInterval | None:
        try:
            return min(
                (
                    t.infer_manual_data_interval(run_after=run_after)
                    for t in self._timetables
                    if t is not None
                ),
                key=operator.attrgetter("start"),
            )
        except ValueError:
            return None

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


class CronPartitionTimetable(CronMixin, _TriggerTimetable):
    """
    Timetable that triggers DAG runs according to a cron expression.

    Creates runs for partition keys.

    The cron expression determines the sequence of partition dates.
    The user can control when those partitions are run by supplying a
    run_offset.  Positive values will run after the partition date.
    Negative values will run before the partition date.

    :param cron: cron string that defines when to run

    :param timezone: Which timezone to use to interpret the cron string
    :param run_offset: Integer offset that determines when the partition
        run should be created.  Time-based offsets are not yet supported.
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
        key_format: str = "%Y-%m-%d",
    ) -> None:
        super().__init__(cron, timezone)
        self._run_immediately = run_immediately
        if not isinstance(run_offset, (int, None)):
            # todo: AIP-76 implement timedelta / relative delta
            raise ValueError("Run offset other than integer not supported yet.")
        self._run_offset = run_offset
        self._key_format = key_format

    @classmethod
    def deserialize(cls, data: dict[str, Any]) -> Timetable:
        from airflow.serialization.decoders import decode_run_immediately

        offset = data["run_offset"]
        if not isinstance(offset, int):
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

    def _get_run_date(self, partition_date: DateTime) -> tuple[DateTime, DateTime]:
        # todo: AIP-76 this may not need to return tuple
        curr_partition_date = self._align_to_prev(partition_date)
        if curr_partition_date != partition_date:
            self.log.warning(
                "Given partition_date does not align with scheme. Aligning to previous partition date.",
                partition_date=partition_date,
                curr_partition_date=curr_partition_date,
                cron_expression=self._expression,
            )

        # todo: AIP-76 possible shorthand syntax for cron partitions... "P:0 0 * * *"
        #  perhaps we could provide a way to register such keys

        if self._run_offset == 0:
            run_date = curr_partition_date
            return curr_partition_date, run_date
        run_date = curr_partition_date  # we will need to apply offset to determine run date
        iter_func = self._get_next if self._run_offset > 0 else self._get_prev
        for _ in range(self._run_offset):
            run_date = iter_func(run_date)
        return curr_partition_date, run_date

    def _calc_first_run(self) -> tuple[DateTime, DateTime]:
        """
        If no start_time is set, determine the start.

        If True, always prefer past run, if False, never. If None, if within 10% of next run,
        if timedelta, if within that timedelta from past run.
        """
        now = coerce_datetime(utcnow())
        past_partition_date = self._align_to_prev(now)
        past_partition_date, past_run_date = self._get_run_date(past_partition_date)
        next_partition_date = self._align_to_next(now)
        next_partition_date, next_run_date = self._get_run_date(next_partition_date)
        if self._run_immediately is True:  # Check for 'True' exactly because deltas also evaluate to true.
            partition_date, run_date = self._get_run_date(past_partition_date)
            return partition_date, run_date

        gap_between_runs = next_run_date - past_run_date
        gap_to_past = now - past_run_date
        if isinstance(self._run_immediately, datetime.timedelta):
            buffer_between_runs = self._run_immediately
        else:
            buffer_between_runs = max(gap_between_runs / 10, datetime.timedelta(minutes=5))
        if gap_to_past <= buffer_between_runs:
            return past_partition_date, past_run_date
        return next_partition_date, next_run_date

    def _partition_date_from_dagrun_info(self, info: DagRunInfo | None) -> DateTime | None:
        if not info:
            return None
        if info.partition_date:
            return info.partition_date
        if info.logical_date:
            return info.logical_date
        return info.run_after

    def next_dagrun_info_v2(
        self,
        *,
        last_dagrun_info: DagRunInfo | None,
        restriction: TimeRestriction,
    ) -> DagRunInfo | None:
        # todo: AIP-76 add test for this logic
        last_partition_date = self._partition_date_from_dagrun_info(last_dagrun_info)

        if restriction.catchup:
            if last_partition_date is not None:
                partition_date = self._get_next(last_partition_date)
                partition_date, run_date = self._get_run_date(partition_date)
            elif restriction.earliest is None:
                partition_date, run_date = self._calc_first_run()
            else:
                partition_date = self._align_to_next(restriction.earliest)
                partition_date, run_date = self._get_run_date(partition_date)
        else:
            partition_date_candidates = [self._align_to_prev(coerce_datetime(utcnow()))]
            if last_partition_date is not None:
                partition_date_candidates.append(self._get_next(last_partition_date))
            elif restriction.earliest is None:
                # Run immediately has no effect if there is restriction on earliest
                partition_date, run_date = self._calc_first_run()
                partition_date_candidates.append(partition_date)
            if restriction.earliest is not None:
                partition_date_candidates.append(self._align_to_next(restriction.earliest))
            partition_date = max(partition_date_candidates)
            partition_date, run_date = self._get_run_date(partition_date)
        if restriction.latest is not None and restriction.latest < partition_date:
            return None
        return DagRunInfo(
            run_after=run_date,
            partition_date=partition_date,
            partition_key=self._format_key(partition_date),
            data_interval=None,
        )

    def _format_key(self, partition_date: DateTime):
        return partition_date.strftime(self._key_format)

    def get_partition_dagrun_info(self, *, partition_date: DateTime) -> DagRunInfo:
        """
        Get dagrun info for date.

        :param partition_date: The partition date for this run.
        """
        partition_date, run_after = self._get_run_date(partition_date)
        partition_key = self._format_key(partition_date)
        return DagRunInfo(
            run_after=run_after,
            data_interval=None,
            partition_date=partition_date,
            partition_key=partition_key,
        )

    def generate_run_id(
        self,
        *,
        run_type: DagRunType,
        run_after: DateTime,
        data_interval: DataInterval | None,
        partition_key: str,
        **extra,
    ) -> str:
        partition_key = extra.get("partition_key")
        components = [
            run_after.isoformat(),
            partition_key,
            get_random_string(),
        ]
        return run_type.generate_run_id(suffix="__".join(components))
