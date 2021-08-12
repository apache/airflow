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

from typing import NamedTuple, Optional

from pendulum import DateTime

from airflow.typing_compat import Protocol


class DataInterval(NamedTuple):
    """A data interval for a DagRun to operate over.

    The represented interval is ``[start, end)``.
    """

    start: DateTime
    end: DateTime


class TimeRestriction(NamedTuple):
    """Restriction on when a DAG can be scheduled for a run.

    Specifically, the run must not be earlier than ``earliest``, nor later than
    ``latest``. If ``catchup`` is *False*, the run must also not be earlier than
    the current time, i.e. "missed" schedules are not backfilled.

    These values are generally set on the DAG or task's ``start_date``,
    ``end_date``, and ``catchup`` arguments.

    Both ``earliest`` and ``latest`` are inclusive; a DAG run can happen exactly
    at either point of time.
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
    """The earliest time this DagRun is created and its tasks scheduled."""

    data_interval: Optional[DataInterval]
    """The data interval this DagRun to operate over, if applicable."""

    @classmethod
    def interval(cls, start: DateTime, end: DateTime) -> "DagRunInfo":
        """Represent a run on a continuous schedule.

        In such a schedule, each data interval starts right after the previous
        one ends, and each run is scheduled right after the interval ends. This
        applies to all schedules prior to AIP-39 except ``@once`` and ``None``.
        """
        return cls(run_after=end, data_interval=DataInterval(start, end))

    @property
    def schedule_date(self) -> DateTime:
        """Infer the schedule date to use for the actual DagRun.

        For backward compatibility, this needs to match how ``execution_date``
        is calculated before AIP-39. If there is a data interval, this is the
        beginning of the interval; if there is not, ``run_after`` is used.
        """
        if self.data_interval is None:
            return self.run_after
        return self.data_interval.start


class Timetable(Protocol):
    """Protocol that all Timetable classes are expected to implement."""

    def validate(self) -> None:
        """Validate the timetable is correctly specified.

        This should raise AirflowTimetableInvalid on validation failure.
        """
        raise NotImplementedError()

    def infer_data_interval(self, run_after: DateTime) -> Optional[DataInterval]:
        """When a DAG run is manually triggered, infer a data interval for it.

        This is used for e.g. manually-triggered runs, where ``run_after`` would
        be when the user triggers the run.
        """
        raise NotImplementedError()

    def next_dagrun_info(
        self,
        last_automated_dagrun: Optional[DateTime],
        restriction: TimeRestriction,
    ) -> Optional[DagRunInfo]:
        """Provide information to schedule the next DagRun.

        :param last_automated_dagrun: The ``execution_date`` of the associated
            DAG's last scheduled or backfilled run (manual runs not considered).
        :param restriction: Restriction to apply when scheduling the DAG run.
            See documentation of :class:`TimeRestriction` for details.

        :return: Information on when the next DagRun can be scheduled. None
            means a DagRun will not happen. This does not mean no more runs
            will be scheduled even again for this DAG; the timetable can return
            a DagRunInfo object when asked at another time.
        """
        raise NotImplementedError()
