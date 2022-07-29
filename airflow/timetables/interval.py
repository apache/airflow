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
from typing import Any, Dict, Optional, Union

from cron_descriptor import CasingTypeEnum, ExpressionDescriptor, FormatException, MissingFieldException
from croniter import CroniterBadCronError, CroniterBadDateError, croniter
from dateutil.relativedelta import relativedelta
from pendulum import DateTime
from pendulum.tz.timezone import Timezone

from airflow.compat.functools import cached_property
from airflow.exceptions import AirflowTimetableInvalid
from airflow.timetables.base import DagRunInfo, DataInterval, TimeRestriction, Timetable
from airflow.utils.dates import cron_presets
from airflow.utils.timezone import convert_to_utc, make_aware, make_naive

Delta = Union[datetime.timedelta, relativedelta]


class _DataIntervalTimetable(Timetable):
    """Basis for timetable implementations that schedule data intervals.

    This kind of timetable classes create periodic data intervals from an
    underlying schedule representation (e.g. a cron expression, or a timedelta
    instance), and schedule a DagRun at the end of each interval.
    """

    def _align(self, current: DateTime) -> DateTime:
        """Align given time to the scheduled.

        For fixed schedules (e.g. every midnight); this finds the next time that
        aligns to the declared time, if the given time does not align. If the
        schedule is not fixed (e.g. every hour), the given time is returned.
        """
        raise NotImplementedError()

    def _get_next(self, current: DateTime) -> DateTime:
        """Get the first schedule after the current time."""
        raise NotImplementedError()

    def _get_prev(self, current: DateTime) -> DateTime:
        """Get the last schedule before the current time."""
        raise NotImplementedError()

    def _skip_to_latest(self, earliest: Optional[DateTime]) -> DateTime:
        """Bound the earliest time a run can be scheduled.

        This is called when ``catchup=False``. See docstring of subclasses for
        exact skipping behaviour of a schedule.
        """
        raise NotImplementedError()

    def next_dagrun_info(
        self,
        *,
        last_automated_data_interval: Optional[DataInterval],
        restriction: TimeRestriction,
    ) -> Optional[DagRunInfo]:
        earliest = restriction.earliest
        if not restriction.catchup:
            earliest = self._skip_to_latest(earliest)
        elif earliest is not None:
            earliest = self._align(earliest)
        if last_automated_data_interval is None:
            # First run; schedule the run at the first available time matching
            # the schedule, and retrospectively create a data interval for it.
            if earliest is None:
                return None
            start = earliest
        else:  # There's a previous run.
            if earliest is not None:
                # Catchup is False or DAG has new start date in the future.
                # Make sure we get the later one.
                start = max(last_automated_data_interval.end, earliest)
            else:
                # Data interval starts from the end of the previous interval.
                start = last_automated_data_interval.end
        if restriction.latest is not None and start > restriction.latest:
            return None
        end = self._get_next(start)
        return DagRunInfo.interval(start=start, end=end)


def _is_schedule_fixed(expression: str) -> bool:
    """Figures out if the schedule has a fixed time (e.g. 3 AM every day).

    :return: True if the schedule has a fixed time, False if not.

    Detection is done by "peeking" the next two cron trigger time; if the
    two times have the same minute and hour value, the schedule is fixed,
    and we *don't* need to perform the DST fix.

    This assumes DST happens on whole minute changes (e.g. 12:59 -> 12:00).
    """
    cron = croniter(expression)
    next_a = cron.get_next(datetime.datetime)
    next_b = cron.get_next(datetime.datetime)
    return next_b.minute == next_a.minute and next_b.hour == next_a.hour


class _CronMixin:
    def __init__(self, cron: str, timezone: Union[str, Timezone]) -> None:
        self._expression = cron_presets.get(cron, cron)

        if isinstance(timezone, str):
            timezone = Timezone(timezone)
        self._timezone = timezone

        descriptor = ExpressionDescriptor(
            expression=self._expression, casing_type=CasingTypeEnum.Sentence, use_24hour_time_format=True
        )
        try:
            # checking for more than 5 parameters in Cron and avoiding evaluation for now,
            # as Croniter has inconsistent evaluation with other libraries
            if len(croniter(self._expression).expanded) > 5:
                raise FormatException()
            interval_description = descriptor.get_description()
        except (CroniterBadCronError, FormatException, MissingFieldException):
            interval_description = ""
        self.description = interval_description

    @classmethod
    def deserialize(cls, data: Dict[str, Any]) -> "Timetable":
        from airflow.serialization.serialized_objects import decode_timezone

        # We ignore typing on the next line because mypy expects it to return _CronMixin type.
        # However, this should return Timetable since it should only be called against a timetable subclass
        return cls(data["expression"], decode_timezone(data["timezone"]))  # type: ignore

    def __eq__(self, other: Any) -> bool:
        """Both expression and timezone should match.

        This is only for testing purposes and should not be relied on otherwise.
        """
        if not isinstance(other, type(self)):
            return NotImplemented
        return self._expression == other._expression and self._timezone == other._timezone

    @property
    def summary(self) -> str:
        return self._expression

    def serialize(self) -> Dict[str, Any]:
        from airflow.serialization.serialized_objects import encode_timezone

        return {"expression": self._expression, "timezone": encode_timezone(self._timezone)}

    def validate(self) -> None:
        try:
            croniter(self._expression)
        except (CroniterBadCronError, CroniterBadDateError) as e:
            raise AirflowTimetableInvalid(str(e))

    @cached_property
    def _should_fix_dst(self) -> bool:
        # This is lazy so instantiating a schedule does not immediately raise
        # an exception. Validity is checked with validate() during DAG-bagging.
        return not _is_schedule_fixed(self._expression)

    def _get_next(self, current: DateTime) -> DateTime:
        """Get the first schedule after specified time, with DST fixed."""
        naive = make_naive(current, self._timezone)
        cron = croniter(self._expression, start_time=naive)
        scheduled = cron.get_next(datetime.datetime)
        if not self._should_fix_dst:
            return convert_to_utc(make_aware(scheduled, self._timezone))
        delta = scheduled - naive
        return convert_to_utc(current.in_timezone(self._timezone) + delta)

    def _get_prev(self, current: DateTime) -> DateTime:
        """Get the first schedule before specified time, with DST fixed."""
        naive = make_naive(current, self._timezone)
        cron = croniter(self._expression, start_time=naive)
        scheduled = cron.get_prev(datetime.datetime)
        if not self._should_fix_dst:
            return convert_to_utc(make_aware(scheduled, self._timezone))
        delta = naive - scheduled
        return convert_to_utc(current.in_timezone(self._timezone) - delta)

    def _align(self, current: DateTime) -> DateTime:
        """Get the next scheduled time.

        This is ``current + interval``, unless ``current`` falls right on the
        interval boundary, when ``current`` is returned.
        """
        next_time = self._get_next(current)
        if self._get_prev(next_time) != current:
            return next_time
        return current


class CronDataIntervalTimetable(_CronMixin, _DataIntervalTimetable):
    """Timetable that schedules data intervals with a cron expression.

    This corresponds to ``schedule_interval=<cron>``, where ``<cron>`` is either
    a five/six-segment representation, or one of ``cron_presets``.

    The implementation extends on croniter to add timezone awareness. This is
    because croniter works only with naive timestamps, and cannot consider DST
    when determining the next/previous time.

    Don't pass ``@once`` in here; use ``OnceTimetable`` instead.
    """

    def _skip_to_latest(self, earliest: Optional[DateTime]) -> DateTime:
        """Bound the earliest time a run can be scheduled.

        The logic is that we move start_date up until one period before, so the
        current time is AFTER the period end, and the job can be created...

        This is slightly different from the delta version at terminal values.
        If the next schedule should start *right now*, we want the data interval
        that start now, not the one that ends now.
        """
        current_time = DateTime.utcnow()
        last_start = self._get_prev(current_time)
        next_start = self._get_next(last_start)
        if next_start == current_time:  # Current time is on interval boundary.
            new_start = last_start
        elif next_start > current_time:  # Current time is between boundaries.
            new_start = self._get_prev(last_start)
        else:
            raise AssertionError("next schedule shouldn't be earlier")
        if earliest is None:
            return new_start
        return max(new_start, self._align(earliest))

    def infer_manual_data_interval(self, *, run_after: DateTime) -> DataInterval:
        # Get the last complete period before run_after, e.g. if a DAG run is
        # scheduled at each midnight, the data interval of a manually triggered
        # run at 1am 25th is between 0am 24th and 0am 25th.
        end = self._get_prev(self._align(run_after))
        return DataInterval(start=self._get_prev(end), end=end)


class CronTriggerTimetable(_CronMixin, Timetable):
    """A cron-compliant timetable.

    The main difference from ``CronDataIntervalTimetable`` is that a first
    DAG Run is kicked off at the start of the period like a normal cron,
    while a first DAG Run of ``CronDataIntervalTimetable`` starts immediately
    after the DAG is registered.

    Note that this timetable does not care the idea of *data interval*. It
    means the value of ``data_interval_start``, ``data_interval_end`` and
    legacy ``execution_date`` are the same - the time when a DAG run is triggered.

    Don't pass ``@once`` in here; use ``OnceTimetable`` instead.
    """

    def infer_manual_data_interval(self, *, run_after: DateTime) -> DataInterval:
        return DataInterval.exact(run_after)

    def next_dagrun_info(
        self,
        *,
        last_automated_data_interval: Optional[DataInterval],
        restriction: TimeRestriction,
    ) -> Optional[DagRunInfo]:
        if restriction.catchup:
            if last_automated_data_interval is None:
                if restriction.earliest is None:
                    return None
                next_start_time = self._align(restriction.earliest)
            else:
                next_start_time = self._get_next(last_automated_data_interval.end)
        else:
            current_time = DateTime.utcnow()
            if restriction.earliest is not None and current_time < restriction.earliest:
                next_start_time = self._align(restriction.earliest)
            else:
                next_start_time = self._align(current_time)
        if restriction.latest is not None and restriction.latest < next_start_time:
            return None
        return DagRunInfo.exact(next_start_time)


class DeltaDataIntervalTimetable(_DataIntervalTimetable):
    """Timetable that schedules data intervals with a time delta.

    This corresponds to ``schedule_interval=<delta>``, where ``<delta>`` is
    either a ``datetime.timedelta`` or ``dateutil.relativedelta.relativedelta``
    instance.
    """

    def __init__(self, delta: Delta) -> None:
        self._delta = delta

    @classmethod
    def deserialize(cls, data: Dict[str, Any]) -> "Timetable":
        from airflow.serialization.serialized_objects import decode_relativedelta

        delta = data["delta"]
        if isinstance(delta, dict):
            return cls(decode_relativedelta(delta))
        return cls(datetime.timedelta(seconds=delta))

    def __eq__(self, other: Any) -> bool:
        """The offset should match.

        This is only for testing purposes and should not be relied on otherwise.
        """
        if not isinstance(other, DeltaDataIntervalTimetable):
            return NotImplemented
        return self._delta == other._delta

    @property
    def summary(self) -> str:
        return str(self._delta)

    def serialize(self) -> Dict[str, Any]:
        from airflow.serialization.serialized_objects import encode_relativedelta

        delta: Any
        if isinstance(self._delta, datetime.timedelta):
            delta = self._delta.total_seconds()
        else:
            delta = encode_relativedelta(self._delta)
        return {"delta": delta}

    def validate(self) -> None:
        now = datetime.datetime.now()
        if (now + self._delta) <= now:
            raise AirflowTimetableInvalid(f"schedule interval must be positive, not {self._delta!r}")

    def _get_next(self, current: DateTime) -> DateTime:
        return convert_to_utc(current + self._delta)

    def _get_prev(self, current: DateTime) -> DateTime:
        return convert_to_utc(current - self._delta)

    def _align(self, current: DateTime) -> DateTime:
        return current

    def _skip_to_latest(self, earliest: Optional[DateTime]) -> DateTime:
        """Bound the earliest time a run can be scheduled.

        The logic is that we move start_date up until one period before, so the
        current time is AFTER the period end, and the job can be created...

        This is slightly different from the cron version at terminal values.
        """
        new_start = self._get_prev(DateTime.utcnow())
        if earliest is None:
            return new_start
        return max(new_start, earliest)

    def infer_manual_data_interval(self, run_after: DateTime) -> DataInterval:
        return DataInterval(start=self._get_prev(run_after), end=run_after)
