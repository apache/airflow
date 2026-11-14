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
from uuid import UUID

from pydantic import Field

from airflow.api_fastapi.common.region import OmitsMissingRegion, RegionId, RegionIndex
from airflow.api_fastapi.core_api.base import BaseModel
from airflow.utils.state import TaskInstanceState


class ExecutionTaskResponse(OmitsMissingRegion, BaseModel):
    """A task try together with the coordinates that address it exactly."""

    id: UUID
    dag_id: str
    run_id: str = Field(alias="dag_run_id")
    task_id: str
    task_display_name: str
    region_id: RegionId = None
    region_index: RegionIndex = None
    map_index: int
    try_number: int
    state: TaskInstanceState | None
    start_date: datetime | None
    end_date: datetime | None
    duration: float | None
    dag_version_id: UUID | None
    operator: str | None
    note: str | None = None


class ExecutionRegionResponse(BaseModel):
    """Immutable region structure for interpreting task coordinates."""

    id: UUID
    node_id: str
    parent_region_id: UUID | None
    parent_region_index: int | None
    forked_from_region_id: UUID | None
    resumes_from_index: int


class ExecutionCollectionResponse(BaseModel):
    """A page of task executions with their region ancestry."""

    task_instances: list[ExecutionTaskResponse]
    regions: list[ExecutionRegionResponse]
    total_entries: int
