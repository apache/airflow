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

from datetime import datetime
from typing import Annotated, Any

from pydantic import (
    AliasPath,
    AwareDatetime,
    BeforeValidator,
    ConfigDict,
    Field,
    NonNegativeInt,
    ValidationError,
    model_validator,
)

from airflow.api_fastapi.core_api.base import BaseModel
from airflow.api_fastapi.core_api.datamodels.job import JobResponse
from airflow.api_fastapi.core_api.datamodels.trigger import TriggerResponse
from airflow.utils.state import TaskInstanceState


class TaskInstanceResponse(BaseModel):
    """TaskInstance serializer for responses."""

    model_config = ConfigDict(populate_by_name=True, from_attributes=True)

    id: str
    task_id: str
    dag_id: str
    run_id: str = Field(alias="dag_run_id")
    map_index: int
    logical_date: datetime
    start_date: datetime | None
    end_date: datetime | None
    duration: float | None
    state: TaskInstanceState | None
    try_number: int
    max_tries: int
    task_display_name: str
    hostname: str | None
    unixname: str | None
    pool: str
    pool_slots: int
    queue: str | None
    priority_weight: int | None
    operator: str | None
    queued_dttm: datetime | None = Field(alias="queued_when")
    pid: int | None
    executor: str | None
    executor_config: Annotated[str, BeforeValidator(str)]
    note: str | None
    rendered_map_index: str | None
    rendered_fields: dict = Field(
        validation_alias=AliasPath("rendered_task_instance_fields", "rendered_fields"),
        default={},
    )
    trigger: TriggerResponse | None
    queued_by_job: JobResponse | None = Field(alias="triggerer_job")


class TaskInstanceCollectionResponse(BaseModel):
    """Task Instance Collection serializer for responses."""

    task_instances: list[TaskInstanceResponse]
    total_entries: int


class TaskDependencyResponse(BaseModel):
    """Task Dependency serializer for responses."""

    name: str
    reason: str


class TaskDependencyCollectionResponse(BaseModel):
    """Task scheduling dependencies collection serializer for responses."""

    dependencies: list[TaskDependencyResponse]


class TaskInstancesBatchBody(BaseModel):
    """Task Instance body for get batch."""

    dag_ids: list[str] | None = None
    dag_run_ids: list[str] | None = None
    task_ids: list[str] | None = None
    state: list[TaskInstanceState | None] | None = None
    logical_date_gte: AwareDatetime | None = None
    logical_date_lte: AwareDatetime | None = None
    start_date_gte: AwareDatetime | None = None
    start_date_lte: AwareDatetime | None = None
    end_date_gte: AwareDatetime | None = None
    end_date_lte: AwareDatetime | None = None
    duration_gte: float | None = None
    duration_lte: float | None = None
    pool: list[str] | None = None
    queue: list[str] | None = None
    executor: list[str] | None = None
    page_offset: NonNegativeInt = 0
    page_limit: NonNegativeInt = 100
    order_by: str | None = None


class TaskInstanceHistoryResponse(BaseModel):
    """TaskInstanceHistory serializer for responses."""

    model_config = ConfigDict(populate_by_name=True, from_attributes=True)

    task_id: str
    dag_id: str

    # todo: this should not be aliased; it's ambiguous with dag run's "id" - airflow 3.0
    run_id: str = Field(alias="dag_run_id")

    map_index: int
    start_date: datetime | None
    end_date: datetime | None
    duration: float | None
    state: TaskInstanceState | None
    try_number: int
    max_tries: int
    task_display_name: str
    hostname: str | None
    unixname: str | None
    pool: str
    pool_slots: int
    queue: str | None
    priority_weight: int | None
    operator: str | None
    queued_dttm: datetime | None = Field(alias="queued_when")
    pid: int | None
    executor: str | None
    executor_config: Annotated[str, BeforeValidator(str)]


class TaskInstanceHistoryCollectionResponse(BaseModel):
    """TaskInstanceHistory Collection serializer for responses."""

    task_instances: list[TaskInstanceHistoryResponse]
    total_entries: int


class ClearTaskInstancesBody(BaseModel):
    """Request body for Clear Task Instances endpoint."""

    dry_run: bool = True
    start_date: AwareDatetime | None = None
    end_date: AwareDatetime | None = None
    only_failed: bool = True
    only_running: bool = False
    reset_dag_runs: bool = False
    task_ids: list[str] | None = None
    dag_run_id: str | None = None
    include_upstream: bool = False
    include_downstream: bool = False
    include_future: bool = False
    include_past: bool = False

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


class TaskInstanceReferenceResponse(BaseModel):
    """Task Instance Reference serializer for responses."""

    task_id: str
    dag_run_id: str = Field(validation_alias="run_id")
    dag_id: str


class TaskInstanceReferenceCollectionResponse(BaseModel):
    """Task Instance Reference collection serializer for responses."""

    task_instances: list[TaskInstanceReferenceResponse]
    total_entries: int
