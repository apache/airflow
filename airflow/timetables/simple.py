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

import operator
from typing import TYPE_CHECKING, Any, Collection

from pendulum import DateTime

from airflow.timetables.base import DagRunInfo, DataInterval, TimeRestriction, Timetable

if TYPE_CHECKING:
    from sqlalchemy import Session

    from airflow.models.dataset import DatasetEvent
    from airflow.utils.types import DagRunType


class _TrivialTimetable(Timetable):
    """Some code reuse for "trivial" timetables that has nothing complex."""

    periodic = False
    run_ordering = ("execution_date",)

    @classmethod
    def deserialize(cls, data: dict[str, Any]) -> Timetable:
        return cls()

    def __eq__(self, other: Any) -> bool:
        """As long as *other* is of the same type.

        This is only for testing purposes and should not be relied on otherwise.
        """
        if not isinstance(other, type(self)):
            return NotImplemented
        return True

    def serialize(self) -> dict[str, Any]:
        return {}

    def infer_manual_data_interval(self, *, run_after: DateTime) -> DataInterval:
        return DataInterval.exact(run_after)


class NullTimetable(_TrivialTimetable):
    """Timetable that never schedules anything.

    This corresponds to ``schedule=None``.
    """

    can_be_scheduled = False
    description: str = "Never, external triggers only"

    @property
    def summary(self) -> str:
        return "None"

    def next_dagrun_info(
        self,
        *,
        last_automated_data_interval: DataInterval | None,
        restriction: TimeRestriction,
    ) -> DagRunInfo | None:
        return None


class OnceTimetable(_TrivialTimetable):
    """Timetable that schedules the execution once as soon as possible.

    This corresponds to ``schedule="@once"``.
    """

    description: str = "Once, as soon as possible"

    @property
    def summary(self) -> str:
        return "@once"

    def next_dagrun_info(
        self,
        *,
        last_automated_data_interval: DataInterval | None,
        restriction: TimeRestriction,
    ) -> DagRunInfo | None:
        if last_automated_data_interval is not None:
            return None  # Already run, no more scheduling.
        if restriction.earliest is None:  # No start date, won't run.
            return None
        # "@once" always schedule to the start_date determined by the DAG and
        # tasks, regardless of catchup or not. This has been the case since 1.10
        # and we're inheriting it. See AIRFLOW-1928.
        run_after = restriction.earliest
        if restriction.latest is not None and run_after > restriction.latest:
            return None
        return DagRunInfo.exact(run_after)


class ContinuousTimetable(_TrivialTimetable):
    """Timetable that schedules continually, while still respecting start_date and end_date.

    This corresponds to ``schedule="@continuous"``.
    """

    description: str = "As frequently as possible, but only one run at a time."

    active_runs_limit = 1  # Continuous DAGRuns should be constrained to one run at a time

    @property
    def summary(self) -> str:
        return "@continuous"

    def next_dagrun_info(
        self,
        *,
        last_automated_data_interval: DataInterval | None,
        restriction: TimeRestriction,
    ) -> DagRunInfo | None:
        if restriction.earliest is None:  # No start date, won't run.
            return None
        if last_automated_data_interval is not None:  # has already run once
            start = last_automated_data_interval.end
            end = DateTime.utcnow()
        else:  # first run
            start = restriction.earliest
            end = max(restriction.earliest, DateTime.utcnow())  # won't run any earlier than start_date

        if restriction.latest is not None and end > restriction.latest:
            return None

        return DagRunInfo.interval(start, end)


class DatasetTriggeredTimetable(_TrivialTimetable):
    """Timetable that never schedules anything.

    This should not be directly used anywhere, but only set if a DAG is triggered by datasets.

    :meta private:
    """

    description: str = "Triggered by datasets"

    @property
    def summary(self) -> str:
        return "Dataset"

    def generate_run_id(
        self,
        *,
        run_type: DagRunType,
        logical_date: DateTime,
        data_interval: DataInterval | None,
        session: Session | None = None,
        events: Collection[DatasetEvent] | None = None,
        **extra,
    ) -> str:
        from airflow.models.dagrun import DagRun

        return DagRun.generate_run_id(run_type, logical_date)

    def data_interval_for_events(
        self,
        logical_date: DateTime,
        events: Collection[DatasetEvent],
    ) -> DataInterval:

        if not events:
            return DataInterval(logical_date, logical_date)

        start = min(
            events, key=operator.attrgetter("source_dag_run.data_interval_start")
        ).source_dag_run.data_interval_start
        end = max(
            events, key=operator.attrgetter("source_dag_run.data_interval_end")
        ).source_dag_run.data_interval_end
        return DataInterval(start, end)

    def next_dagrun_info(
        self,
        *,
        last_automated_data_interval: DataInterval | None,
        restriction: TimeRestriction,
    ) -> DagRunInfo | None:
        return None
