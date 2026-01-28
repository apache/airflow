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
from typing import Annotated, Any

from pydantic import (
    AliasPath,
    AwareDatetime,
    BeforeValidator,
    Field,
    NonNegativeInt,
    StringConstraints,
    ValidationError,
    field_validator,
    model_validator,
)

from airflow.api_fastapi.core_api.base import BaseModel, StrictBaseModel
from airflow.api_fastapi.core_api.datamodels.dag_versions import DagVersionResponse
from airflow.api_fastapi.core_api.datamodels.job import JobResponse
from airflow.api_fastapi.core_api.datamodels.trigger import TriggerResponse
from airflow.utils.state import TaskInstanceState


class TaskInstanceResponse(BaseModel):
    """TaskInstance serializer for responses."""

    id: str
    task_id: str
    dag_id: str
    run_id: str = Field(alias="dag_run_id")
    map_index: int
    logical_date: datetime | None
    run_after: datetime
    start_date: datetime | None
    end_date: datetime | None
    duration: float | None
    state: TaskInstanceState | None
    try_number: int
    max_tries: int
    task_display_name: str
    dag_display_name: str = Field(validation_alias=AliasPath("dag_run", "dag_model", "dag_display_name"))
    hostname: str | None
    unixname: str | None
    pool: str
    pool_slots: int
    queue: str | None
    priority_weight: int | None
    operator: str | None
    operator_name: str | None
    queued_dttm: datetime | None = Field(alias="queued_when")
    scheduled_dttm: datetime | None = Field(alias="scheduled_when")
    pid: int | None
    executor: str | None
    executor_config: Annotated[str, BeforeValidator(str)]
    note: str | None
    rendered_map_index: str | None
    rendered_fields: dict = Field(
        validation_alias=AliasPath("rendered_task_instance_fields", "rendered_fields"),
        default_factory=dict,
    )
    trigger: TriggerResponse | None
    queued_by_job: JobResponse | None = Field(alias="triggerer_job")
    dag_version: DagVersionResponse | None


class TaskInstanceCollectionResponse(BaseModel):
    """Task Instance Collection serializer for responses."""

    task_instances: Iterable[TaskInstanceResponse]
    total_entries: int


class TaskDependencyResponse(BaseModel):
    """Task Dependency serializer for responses."""

    name: str
    reason: str


class TaskDependencyCollectionResponse(BaseModel):
    """Task scheduling dependencies collection serializer for responses."""

    dependencies: list[TaskDependencyResponse]


class TaskInstancesBatchBody(StrictBaseModel):
    """Task Instance body for get batch."""

    dag_ids: list[str] | None = None
    dag_run_ids: list[str] | None = None
    task_ids: list[str] | None = None
    state: list[TaskInstanceState | None] | None = None

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

    pool: list[str] | None = None
    queue: list[str] | None = None
    executor: list[str] | None = None
    page_offset: NonNegativeInt = 0
    page_limit: NonNegativeInt = 100
    order_by: str | None = None


class ClearTaskInstancesBody(StrictBaseModel):
    """Request body for Clear Task Instances endpoint."""

    dry_run: bool = True
    start_date: AwareDatetime | None = None
    end_date: AwareDatetime | None = None
    only_failed: bool = True
    only_running: bool = False
    reset_dag_runs: bool = True
    task_ids: list[str | tuple[str, int]] | None = Field(
        default=None,
        description="A list of `task_id` or [`task_id`, `map_index`]. "
        "If only the `task_id` is provided for a mapped task, all of its map indices will be targeted.",
    )
    dag_run_id: str | None = None
    include_upstream: bool = False
    include_downstream: bool = False
    include_future: bool = False
    include_past: bool = False
    run_on_latest_version: bool = Field(
        default=False,
        description="(Experimental) Run on the latest bundle version of the dag after "
        "clearing the task instances.",
    )
    prevent_running_task: bool = False

    @model_validator(mode="before")
    @classmethod
    def validate_model(cls, data: Any) -> Any:
        """Validate clear task instance form."""
        if data.get("only_failed") and data.get("only_running"):
            raise ValidationError("only_failed and only_running both are set to True")
        if data.get("start_date") and data.get("end_date"):
            if data.get("start_date") > data.get("end_date"):
                raise ValidationError("end_date is sooner than start_date")
        if data.get("start_date") and data.get("end_date") and data.get("dag_run_id"):
            raise ValidationError("Exactly one of dag_run_id or (start_date and end_date) must be provided")
        if data.get("start_date") and data.get("dag_run_id"):
            raise ValidationError("Exactly one of dag_run_id or start_date must be provided")
        if data.get("end_date") and data.get("dag_run_id"):
            raise ValidationError("Exactly one of dag_run_id or end_date must be provided")
        if isinstance(data.get("task_ids"), list) and len(data.get("task_ids")) < 1:
            raise ValidationError("task_ids list should have at least 1 element.")
        return data


class PatchTaskInstanceBody(StrictBaseModel):
    """Request body for Clear Task Instances endpoint."""

    new_state: TaskInstanceState | None = None
    note: Annotated[str, StringConstraints(max_length=1000)] | None = None
    include_upstream: bool = False
    include_downstream: bool = False
    include_future: bool = False
    include_past: bool = False

    @field_validator("new_state", mode="before")
    @classmethod
    def validate_new_state(cls, ns: str | None) -> str:
        """Validate new_state."""
        valid_states = [
            vs.name.lower()
            for vs in (TaskInstanceState.SUCCESS, TaskInstanceState.FAILED, TaskInstanceState.SKIPPED)
        ]
        if ns is None:
            raise ValueError("'new_state' should not be empty")
        ns = ns.lower()
        if ns not in valid_states:
            raise ValueError(f"'{ns}' is not one of {valid_states}")
        return ns


class BulkTaskInstanceBody(PatchTaskInstanceBody, StrictBaseModel):
    """Request body for bulk update, and delete task instances."""

    task_id: str
    map_index: int | None = None
    dag_id: str | None = None
    dag_run_id: str | None = None
