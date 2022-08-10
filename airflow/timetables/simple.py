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

from typing import Any, Dict, Optional

from pendulum import DateTime

from airflow.timetables.base import DagRunInfo, DataInterval, TimeRestriction, Timetable


class _TrivialTimetable(Timetable):
    """Some code reuse for "trivial" timetables that has nothing complex."""

    periodic = False
    can_run = False

    @classmethod
    def deserialize(cls, data: Dict[str, Any]) -> "Timetable":
        return cls()

    def __eq__(self, other: Any) -> bool:
        """As long as *other* is of the same type.

        This is only for testing purposes and should not be relied on otherwise.
        """
        if not isinstance(other, type(self)):
            return NotImplemented
        return True

    def serialize(self) -> Dict[str, Any]:
        return {}

    def infer_manual_data_interval(self, *, run_after: DateTime) -> DataInterval:
        return DataInterval.exact(run_after)


class NullTimetable(_TrivialTimetable):
    """Timetable that never schedules anything.

    This corresponds to ``schedule=None``.
    """

    description: str = "Never, external triggers only"

    @property
    def summary(self) -> str:
        return "None"

    def next_dagrun_info(
        self,
        *,
        last_automated_data_interval: Optional[DataInterval],
        restriction: TimeRestriction,
    ) -> Optional[DagRunInfo]:
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
        last_automated_data_interval: Optional[DataInterval],
        restriction: TimeRestriction,
    ) -> Optional[DagRunInfo]:
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


class DatasetTriggeredTimetable(NullTimetable):
    """Timetable that never schedules anything.

    This should not be directly used anywhere, but only set if a DAG is triggered by datasets.

    :meta private:
    """

    description: str = "Triggered by datasets"

    @property
    def summary(self) -> str:
        return "Dataset"
