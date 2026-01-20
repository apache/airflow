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

import typing

from airflow.exceptions import AirflowTimetableInvalid
from airflow.serialization.definitions.assets import SerializedAsset, SerializedAssetBase
from airflow.timetables.simple import AssetTriggeredTimetable
from airflow.utils.types import DagRunType

if typing.TYPE_CHECKING:
    from collections.abc import Collection

    import pendulum

    from airflow.timetables.base import DagRunInfo, DataInterval, TimeRestriction, Timetable


class AssetOrTimeSchedule(AssetTriggeredTimetable):
    """Combine time-based scheduling with event-based scheduling."""

    def __init__(
        self,
        *,
        timetable: Timetable,
        assets: Collection[SerializedAsset] | SerializedAssetBase,
    ) -> None:
        super().__init__(assets)
        self.timetable = timetable
        self.description = f"Triggered by assets or {timetable.description}"
        self.periodic = timetable.periodic
        self.can_be_scheduled = timetable.can_be_scheduled
        self.active_runs_limit = timetable.active_runs_limit

    @classmethod
    def deserialize(cls, data: dict[str, typing.Any]) -> Timetable:
        from airflow.serialization.decoders import decode_asset_like, decode_timetable

        return cls(
            assets=decode_asset_like(data["asset_condition"]),
            timetable=decode_timetable(data["timetable"]),
        )

    def validate(self) -> None:
        if isinstance(self.timetable, AssetTriggeredTimetable):
            raise AirflowTimetableInvalid("cannot nest asset timetables")
        if not isinstance(self.asset_condition, SerializedAssetBase):
            raise AirflowTimetableInvalid("all elements in 'assets' must be assets")

    def serialize(self) -> dict[str, typing.Any]:
        from airflow.serialization.encoders import encode_asset_like, encode_timetable

        return {
            "asset_condition": encode_asset_like(self.asset_condition),
            "timetable": encode_timetable(self.timetable),
        }

    @property
    def summary(self) -> str:
        return f"Asset or {self.timetable.summary}"

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
        if run_type != DagRunType.ASSET_TRIGGERED:
            return self.timetable.generate_run_id(run_type=run_type, **kwargs)
        return super().generate_run_id(run_type=run_type, **kwargs)
