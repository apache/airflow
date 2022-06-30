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
from typing import Any, Collection, Dict, Optional, Union

from cron_descriptor import CasingTypeEnum, ExpressionDescriptor, FormatException, MissingFieldException
from croniter import CroniterBadCronError, CroniterBadDateError, croniter
from dateutil.relativedelta import relativedelta
from pendulum import DateTime
from pendulum.tz.timezone import Timezone

from airflow.exceptions import AirflowTimetableInvalid
from airflow.timetables.base import DagRunInfo, DataInterval, TimeRestriction, Timetable
from airflow.utils.dates import cron_presets
from airflow.utils.timezone import convert_to_utc

Delta = Union[datetime.timedelta, relativedelta]


class _DataIntervalTimetable(Timetable):
    """Basis for timetable implementations that schedule data intervals.

    This kind of timetable classes create periodic data intervals from an
    underlying schedule representation (e.g. a cron expression, or a timedelta
    instance), and schedule a DagRun at the end of each interval.
    """

    def _skip_to_latest(self, earliest: Optional[DateTime]) -> DateTime:
        """Bound the earliest time a run can be scheduled.

        This is called when ``catchup=False``. See docstring of subclasses for
        exact skipping behaviour of a schedule.
        """
        raise NotImplementedError()

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


class CronDataIntervalTimetable(_DataIntervalTimetable):
    """
    Timetable that schedules data intervals using one or more cron expressions.

    This corresponds to ``schedule_interval=<cron>``, where ``<cron>`` is either:
    - A five-segment cron representation
    - One of ``cron_presets``
    - Or a collection containing values from the above

    The implementation extends on croniter to add timezone awareness. This is
    because croniter works only with naive timestamps, and cannot consider DST
    when determining the next/previous time.

    Don't pass ``@once`` in here; use ``OnceTimetable`` instead.
    """

    def __init__(self, crons: Union[str, Collection[str]], timezone: Union[str, Timezone]) -> None:
        """
        :param crons: One or more cron expressions
        :param timezone:
        """
        if isinstance(crons, str):
            crons = [crons]

        self._expressions = {cron_presets.get(expression, expression) for expression in crons}
        cron_descriptions = set()

        for cron_expression in self._expressions:
            descriptor = ExpressionDescriptor(
                expression=cron_expression, casing_type=CasingTypeEnum.Sentence, use_24hour_time_format=True
            )

            try:
                # Check if cron expression contains more than 5 elements and avoid evaluation for now as
                # Croniter has inconsistent evaluation with other libraries
                if len(croniter(cron_expression).expanded) > 5:
                    raise FormatException()
                interval_description = descriptor.get_description()
            except (CroniterBadCronError, FormatException, MissingFieldException):
                interval_description = ""

            cron_descriptions.add(interval_description)

        self.description = " and ".join(cron_descriptions)

        if isinstance(timezone, str):
            timezone = Timezone(timezone)
        self._timezone = timezone

    @classmethod
    def deserialize(cls, data: Dict[str, Any]) -> "Timetable":
        from airflow.serialization.serialized_objects import decode_timezone

        return cls(data["expressions"], decode_timezone(data["timezone"]))

    def __eq__(self, other: Any) -> bool:
        """
        Both expression and timezone should match.
        This is only for testing purposes and should not be relied on otherwise.
        """
        if not isinstance(other, CronDataIntervalTimetable):
            return NotImplemented
        return self._expressions == other._expressions and self._timezone == other._timezone

    @property
    def summary(self) -> str:
        return ", ".join(self._expressions)

    def serialize(self) -> Dict[str, Any]:
        from airflow.serialization.serialized_objects import encode_timezone

        return {"expressions": self._expressions, "timezone": encode_timezone(self._timezone)}

    def validate(self) -> None:
        try:
            for cron_expression in self._expressions:
                croniter(cron_expression)
        except (CroniterBadCronError, CroniterBadDateError) as e:
            raise AirflowTimetableInvalid(str(e))

    def _get_next(self, current: DateTime) -> DateTime:
        """Get the first schedule after specified time in UTC."""
        crons = {croniter(expression, start_time=current) for expression in self._expressions}
        earliest_datetime = min(cron.get_next(datetime.datetime) for cron in crons)
        return convert_to_utc(earliest_datetime)

    def _get_prev(self, current: DateTime) -> DateTime:
        """Get the first schedule before specified time in UTC."""
        crons = {croniter(expression, start_time=current) for expression in self._expressions}
        latest_datetime = max(cron.get_prev(datetime.datetime) for cron in crons)
        return convert_to_utc(latest_datetime)

    def _align(self, current: DateTime) -> DateTime:
        """Get the next scheduled time.

        This is ``current + interval``, unless ``current`` falls right on the
        interval boundary, when ``current`` is returned.
        """
        next_time = self._get_next(current)
        if self._get_prev(next_time) != current:
            return next_time
        return current

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
