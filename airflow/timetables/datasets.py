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

import collections.abc
import typing

import attrs

from airflow.datasets import Dataset
from airflow.exceptions import AirflowTimetableInvalid
from airflow.timetables.simple import DatasetTriggeredTimetable as DatasetTriggeredSchedule
from airflow.utils.types import DagRunType

if typing.TYPE_CHECKING:
    import pendulum

    from airflow.timetables.base import DagRunInfo, DataInterval, TimeRestriction, Timetable


class DatasetOrTimeSchedule(DatasetTriggeredSchedule):
    """Combine time-based scheduling with event-based scheduling."""

    def __init__(self, timetable: Timetable, datasets: collections.abc.Collection[Dataset]) -> None:
        self.timetable = timetable
        self.datasets = datasets

        self.description = f"Triggered by datasets or {timetable.description}"
        self.periodic = timetable.periodic
        self._can_be_scheduled = timetable._can_be_scheduled

        self.run_ordering = timetable.run_ordering
        self.active_runs_limit = timetable.active_runs_limit

    @classmethod
    def deserialize(cls, data: dict[str, typing.Any]) -> Timetable:
        from airflow.serialization.serialized_objects import decode_timetable

        return cls(
            timetable=decode_timetable(data["timetable"]), datasets=[Dataset(**d) for d in data["datasets"]]
        )

    def serialize(self) -> dict[str, typing.Any]:
        from airflow.serialization.serialized_objects import encode_timetable

        return {
            "timetable": encode_timetable(self.timetable),
            "datasets": [attrs.asdict(e) for e in self.datasets],
        }

    def validate(self) -> None:
        if isinstance(self.timetable, DatasetTriggeredSchedule):
            raise AirflowTimetableInvalid("cannot nest dataset timetables")
        if not isinstance(self.datasets, collections.abc.Collection) or not all(
            isinstance(d, Dataset) for d in self.datasets
        ):
            raise AirflowTimetableInvalid("all elements in 'event' must be datasets")

    @property
    def summary(self) -> str:
        return f"Dataset or {self.timetable.summary}"

    def infer_manual_data_interval(self, *, run_after: pendulum.DateTime) -> DataInterval:
        return self.timetable.infer_manual_data_interval(run_after=run_after)

    def next_dagrun_info(
        self, *, last_automated_data_interval: DataInterval | None, restriction: TimeRestriction
    ) -> DagRunInfo | None:
        return self.timetable.next_dagrun_info(
            last_automated_data_interval=last_automated_data_interval,
            restriction=restriction,
        )

    def generate_run_id(self, *, run_type: DagRunType, **kwargs: typing.Any) -> str:
        if run_type != DagRunType.DATASET_TRIGGERED:
            return self.timetable.generate_run_id(run_type=run_type, **kwargs)
        return super().generate_run_id(run_type=run_type, **kwargs)
