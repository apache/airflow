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

from collections.abc import Iterable
from datetime import datetime
from enum import Enum
from typing import TYPE_CHECKING

from pydantic import AliasPath, AwareDatetime, Field, NonNegativeInt, model_validator

from airflow._shared.timezones import timezone
from airflow.api_fastapi.core_api.base import BaseModel, StrictBaseModel
from airflow.api_fastapi.core_api.datamodels.dag_versions import DagVersionResponse
from airflow.timetables.base import DataInterval
from airflow.utils.state import DagRunState
from airflow.utils.types import DagRunTriggeredByType, DagRunType

if TYPE_CHECKING:
    from airflow.serialization.serialized_objects import SerializedDAG


class DAGRunPatchStates(str, Enum):
    """Enum for DAG Run states when updating a DAG Run."""

    QUEUED = DagRunState.QUEUED
    SUCCESS = DagRunState.SUCCESS
    FAILED = DagRunState.FAILED


class DAGRunPatchBody(StrictBaseModel):
    """DAG Run Serializer for PATCH requests."""

    state: DAGRunPatchStates | None = None
    note: str | None = Field(None, max_length=1000)


class DAGRunClearBody(StrictBaseModel):
    """DAG Run serializer for clear endpoint body."""

    dry_run: bool = True
    only_failed: bool = False
    run_on_latest_version: bool = Field(
        default=False,
        description="(Experimental) Run on the latest bundle version of the Dag after clearing the Dag Run.",
    )


class DAGRunResponse(BaseModel):
    """DAG Run serializer for responses."""

    dag_run_id: str = Field(validation_alias="run_id")
    dag_id: str
    logical_date: datetime | None
    queued_at: datetime | None
    start_date: datetime | None
    end_date: datetime | None
    duration: float | None
    data_interval_start: datetime | None
    data_interval_end: datetime | None
    run_after: datetime
    last_scheduling_decision: datetime | None
    run_type: DagRunType
    state: DagRunState
    triggered_by: DagRunTriggeredByType | None
    triggering_user_name: str | None
    conf: dict | None
    note: str | None
    dag_versions: list[DagVersionResponse]
    bundle_version: str | None
    dag_display_name: str = Field(validation_alias=AliasPath("dag_model", "dag_display_name"))
    partition_key: str | None


class DAGRunCollectionResponse(BaseModel):
    """DAG Run Collection serializer for responses."""

    dag_runs: Iterable[DAGRunResponse]
    total_entries: int


class TriggerDAGRunPostBody(StrictBaseModel):
    """Trigger DAG Run Serializer for POST body."""

    dag_run_id: str | None = None
    data_interval_start: AwareDatetime | None = None
    data_interval_end: AwareDatetime | None = None
    logical_date: AwareDatetime | None
    run_after: datetime | None = Field(default_factory=timezone.utcnow)

    conf: dict | None = Field(default_factory=dict)
    note: str | None = None
    partition_key: str | None = None

    @model_validator(mode="after")
    def check_data_intervals(self):
        if (self.data_interval_start is None) != (self.data_interval_end is None):
            raise ValueError(
                "Either both data_interval_start and data_interval_end must be provided or both must be None"
            )
        return self

    def validate_context(self, dag: SerializedDAG) -> dict:
        coerced_logical_date = timezone.coerce_datetime(self.logical_date)
        run_after = self.run_after or timezone.utcnow()
        data_interval = None
        if coerced_logical_date:
            if self.data_interval_start and self.data_interval_end:
                data_interval = DataInterval(
                    start=timezone.coerce_datetime(self.data_interval_start),
                    end=timezone.coerce_datetime(self.data_interval_end),
                )
            else:
                data_interval = dag.timetable.infer_manual_data_interval(
                    run_after=coerced_logical_date
                )

        run_id = self.dag_run_id or dag.timetable.generate_run_id(
            run_type=DagRunType.MANUAL,
            run_after=timezone.coerce_datetime(run_after),
            data_interval=data_interval,
        )
        return {
            "run_id": run_id,
            "logical_date": coerced_logical_date,
            "data_interval": data_interval,
            "run_after": run_after,
            "conf": self.conf,
            "note": self.note,
            "partition_key": self.partition_key,
        }


class DAGRunsBatchBody(StrictBaseModel):
    """List DAG Runs body for batch endpoint."""

    order_by: str | None = None
    page_offset: NonNegativeInt = 0
    page_limit: NonNegativeInt = 100
    dag_ids: list[str] | None = None
    states: list[DagRunState | None] | None = None

    run_after_gte: AwareDatetime | None = None
    run_after_gt: AwareDatetime | None = None
    run_after_lte: AwareDatetime | None = None
    run_after_lt: AwareDatetime | None = None

    logical_date_gte: AwareDatetime | None = None
    logical_date_gt: AwareDatetime | None = None
    logical_date_lte: AwareDatetime | None = None
    logical_date_lt: AwareDatetime | None = None

    start_date_gte: AwareDatetime | None = None
    start_date_gt: AwareDatetime | None = None
    start_date_lte: AwareDatetime | None = None
    start_date_lt: AwareDatetime | None = None

    end_date_gte: AwareDatetime | None = None
    end_date_gt: AwareDatetime | None = None
    end_date_lte: AwareDatetime | None = None
    end_date_lt: AwareDatetime | None = None

    duration_gte: float | None = None
    duration_gt: float | None = None
    duration_lte: float | None = None
    duration_lt: float | None = None

    conf_contains: str | None = None
