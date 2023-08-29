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
from typing import TYPE_CHECKING, Any, Union

from dateutil.relativedelta import relativedelta
from pendulum import DateTime

from airflow.exceptions import AirflowTimetableInvalid
from airflow.timetables._cron import CronMixin
from airflow.timetables.base import DagRunInfo, DataInterval, Timetable
from airflow.utils.timezone import convert_to_utc

if TYPE_CHECKING:
    from airflow.timetables.base import TimeRestriction

Delta = Union[datetime.timedelta, relativedelta]


class _DataIntervalTimetable(Timetable):
    """Basis for timetable implementations that schedule data intervals.

    This kind of timetable classes create periodic data intervals from an
    underlying schedule representation (e.g. a cron expression, or a timedelta
    instance), and schedule a DagRun at the end of each interval.
    """

    def _skip_to_latest(self, earliest: DateTime | None) -> DateTime:
        """Bound the earliest time a run can be scheduled.

        This is called when ``catchup=False``. See docstring of subclasses for
        exact skipping behaviour of a schedule.
        """
        raise NotImplementedError()

    def _align_to_next(self, current: DateTime) -> DateTime:
        """Align given time to the next scheduled time.

        For fixed schedules (e.g. every midnight); this finds the next time that
        aligns to the declared time, if the given time does not align. If the
        schedule is not fixed (e.g. every hour), the given time is returned.
        """
        raise NotImplementedError()

    def _align_to_prev(self, current: DateTime) -> DateTime:
        """Align given time to the previous scheduled time.

        For fixed schedules (e.g. every midnight); this finds the prev time that
        aligns to the declared time, if the given time does not align. If the
        schedule is not fixed (e.g. every hour), the given time is returned.

        It is not enough to use ``_get_prev(_align_to_next())``, since when a
        DAG's schedule changes, this alternative would make the first scheduling
        after the schedule change remain the same.
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
        last_automated_data_interval: DataInterval | None,
        restriction: TimeRestriction,
    ) -> DagRunInfo | None:
        earliest = restriction.earliest
        if not restriction.catchup:
            earliest = self._skip_to_latest(earliest)
        elif earliest is not None:
            earliest = self._align_to_next(earliest)
        if last_automated_data_interval is None:
            # First run; schedule the run at the first available time matching
            # the schedule, and retrospectively create a data interval for it.
            if earliest is None:
                return None
            start = earliest
        else:  # There's a previous run.
            # Alignment is needed when DAG has new schedule interval.
            align_last_data_interval_end = self._align_to_prev(last_automated_data_interval.end)
            if earliest is not None:
                # Catchup is False or DAG has new start date in the future.
                # Make sure we get the later one.
                start = max(align_last_data_interval_end, earliest)
            else:
                # Data interval starts from the end of the previous interval.
                start = align_last_data_interval_end
        if restriction.latest is not None and start > restriction.latest:
            return None
        end = self._get_next(start)
        return DagRunInfo.interval(start=start, end=end)


class CronDataIntervalTimetable(CronMixin, _DataIntervalTimetable):
    """Timetable that schedules data intervals with a cron expression.

    This corresponds to ``schedule=<cron>``, where ``<cron>`` is either
    a five/six-segment representation, or one of ``cron_presets``.

    The implementation extends on croniter to add timezone awareness. This is
    because croniter works only with naive timestamps, and cannot consider DST
    when determining the next/previous time.

    Don't pass ``@once`` in here; use ``OnceTimetable`` instead.
    """

    @classmethod
    def deserialize(cls, data: dict[str, Any]) -> Timetable:
        from airflow.serialization.serialized_objects import decode_timezone

        return cls(data["expression"], decode_timezone(data["timezone"]))

    def serialize(self) -> dict[str, Any]:
        from airflow.serialization.serialized_objects import encode_timezone

        return {"expression": self._expression, "timezone": encode_timezone(self._timezone)}

    def _skip_to_latest(self, earliest: DateTime | None) -> DateTime:
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
        return max(new_start, self._align_to_next(earliest))

    def infer_manual_data_interval(self, *, run_after: DateTime) -> DataInterval:
        # Get the last complete period before run_after, e.g. if a DAG run is
        # scheduled at each midnight, the data interval of a manually triggered
        # run at 1am 25th is between 0am 24th and 0am 25th.
        end = self._align_to_prev(run_after)
        return DataInterval(start=self._get_prev(end), end=end)


class DeltaDataIntervalTimetable(_DataIntervalTimetable):
    """Timetable that schedules data intervals with a time delta.

    This corresponds to ``schedule=<delta>``, where ``<delta>`` is
    either a ``datetime.timedelta`` or ``dateutil.relativedelta.relativedelta``
    instance.
    """

    def __init__(self, delta: Delta) -> None:
        self._delta = delta

    @classmethod
    def deserialize(cls, data: dict[str, Any]) -> Timetable:
        from airflow.serialization.serialized_objects import decode_relativedelta

        delta = data["delta"]
        if isinstance(delta, dict):
            return cls(decode_relativedelta(delta))
        return cls(datetime.timedelta(seconds=delta))

    def __eq__(self, other: Any) -> bool:
        """
        Return if the offsets match.

        This is only for testing purposes and should not be relied on otherwise.
        """
        if not isinstance(other, DeltaDataIntervalTimetable):
            return NotImplemented
        return self._delta == other._delta

    @property
    def summary(self) -> str:
        return str(self._delta)

    def serialize(self) -> dict[str, Any]:
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

    def _align_to_next(self, current: DateTime) -> DateTime:
        return current

    def _align_to_prev(self, current: DateTime) -> DateTime:
        return current

    def _skip_to_latest(self, earliest: DateTime | None) -> DateTime:
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
