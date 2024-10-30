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
from typing import Annotated

from pydantic import AliasPath, BaseModel, BeforeValidator, ConfigDict, Field

from airflow.api_fastapi.core_api.serializers.job import JobResponse
from airflow.api_fastapi.core_api.serializers.trigger import TriggerResponse
from airflow.utils.state import TaskInstanceState


class TaskInstanceResponse(BaseModel):
    """TaskInstance serializer for responses."""

    model_config = ConfigDict(populate_by_name=True)

    task_id: str
    dag_id: str
    run_id: str = Field(alias="dag_run_id")
    map_index: int
    execution_date: datetime = Field(alias="logical_date")
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
