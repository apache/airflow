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

from typing import Any, Dict, NamedTuple, Optional

from pendulum import DateTime

from airflow.typing_compat import Protocol


class DataInterval(NamedTuple):
    """A data interval for a DagRun to operate over.

    Both ``start`` and ``end`` **MUST** be "aware", i.e. contain timezone
    information.
    """

    start: DateTime
    end: DateTime

    @classmethod
    def exact(cls, at: DateTime) -> "DataInterval":
        """Represent an "interval" containing only an exact time."""
        return cls(start=at, end=at)


class TimeRestriction(NamedTuple):
    """Restriction on when a DAG can be scheduled for a run.

    Specifically, the run must not be earlier than ``earliest``, nor later than
    ``latest``. If ``catchup`` is *False*, the run must also not be earlier than
    the current time, i.e. "missed" schedules are not backfilled.

    These values are generally set on the DAG or task's ``start_date``,
    ``end_date``, and ``catchup`` arguments.

    Both ``earliest`` and ``latest``, if not *None*, are inclusive; a DAG run
    can happen exactly at either point of time. They are guaranteed to be aware
    (i.e. contain timezone information) for ``TimeRestriction`` instances
    created by Airflow.
    """

    earliest: Optional[DateTime]
    latest: Optional[DateTime]
    catchup: bool


class DagRunInfo(NamedTuple):
    """Information to schedule a DagRun.

    Instances of this will be returned by timetables when they are asked to
    schedule a DagRun creation.
    """

    run_after: DateTime
    """The earliest time this DagRun is created and its tasks scheduled.

    This **MUST** be "aware", i.e. contain timezone information.
    """

    data_interval: DataInterval
    """The data interval this DagRun to operate over."""

    @classmethod
    def exact(cls, at: DateTime) -> "DagRunInfo":
        """Represent a run on an exact time."""
        return cls(run_after=at, data_interval=DataInterval.exact(at))

    @classmethod
    def interval(cls, start: DateTime, end: DateTime) -> "DagRunInfo":
        """Represent a run on a continuous schedule.

        In such a schedule, each data interval starts right after the previous
        one ends, and each run is scheduled right after the interval ends. This
        applies to all schedules prior to AIP-39 except ``@once`` and ``None``.
        """
        return cls(run_after=end, data_interval=DataInterval(start, end))

    @property
    def logical_date(self: "DagRunInfo") -> DateTime:
        """Infer the logical date to represent a DagRun.

        This replaces ``execution_date`` in Airflow 2.1 and prior. The idea is
        essentially the same, just a different name.
        """
        return self.data_interval.start


class Timetable(Protocol):
    """Protocol that all Timetable classes are expected to implement."""

    description: str = ""
    """Human-readable description of the timetable.

    For example, this can produce something like ``'At 21:30, only on Friday'``
    from the cron expression ``'30 21 * * 5'``. This is used in the webserver UI.
    """

    periodic: bool = True
    """Whether this timetable runs periodically.

    This defaults to and should generally be *True*, but some special setups
    like ``schedule_interval=None`` and ``"@once"`` set it to *False*.
    """

    can_run: bool = True
    """Whether this timetable can actually schedule runs.

    This defaults to and should generally be *True*, but ``NullTimetable`` sets
    this to *False*.
    """

    @classmethod
    def deserialize(cls, data: Dict[str, Any]) -> "Timetable":
        """Deserialize a timetable from data.

        This is called when a serialized DAG is deserialized. ``data`` will be
        whatever was returned by ``serialize`` during DAG serialization. The
        default implementation constructs the timetable without any arguments.
        """
        return cls()

    def serialize(self) -> Dict[str, Any]:
        """Serialize the timetable for JSON encoding.

        This is called during DAG serialization to store timetable information
        in the database. This should return a JSON-serializable dict that will
        be fed into ``deserialize`` when the DAG is deserialized. The default
        implementation returns an empty dict.
        """
        return {}

    def validate(self) -> None:
        """Validate the timetable is correctly specified.

        Override this method to provide run-time validation raised when a DAG
        is put into a dagbag. The default implementation does nothing.

        :raises: AirflowTimetableInvalid on validation failure.
        """

    @property
    def summary(self) -> str:
        """A short summary for the timetable.

        This is used to display the timetable in the web UI. A cron expression
        timetable, for example, can use this to display the expression. The
        default implementation returns the timetable's type name.
        """
        return type(self).__name__

    def infer_manual_data_interval(self, *, run_after: DateTime) -> DataInterval:
        """When a DAG run is manually triggered, infer a data interval for it.

        This is used for e.g. manually-triggered runs, where ``run_after`` would
        be when the user triggers the run. The default implementation raises
        ``NotImplementedError``.
        """
        raise NotImplementedError()

    def next_dagrun_info(
        self,
        *,
        last_automated_data_interval: Optional[DataInterval],
        restriction: TimeRestriction,
    ) -> Optional[DagRunInfo]:
        """Provide information to schedule the next DagRun.

        The default implementation raises ``NotImplementedError``.

        :param last_automated_data_interval: The data interval of the associated
            DAG's last scheduled or backfilled run (manual runs not considered).
        :param restriction: Restriction to apply when scheduling the DAG run.
            See documentation of :class:`TimeRestriction` for details.

        :return: Information on when the next DagRun can be scheduled. None
            means a DagRun will not happen. This does not mean no more runs
            will be scheduled even again for this DAG; the timetable can return
            a DagRunInfo object when asked at another time.
        """
        raise NotImplementedError()
