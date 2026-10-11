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
from typing import Literal
from uuid import UUID

from airflow.api_fastapi.core_api.base import BaseModel
from airflow.utils.state import TaskInstanceState


class LightGridTaskInstanceSummary(BaseModel):
    """Task Instance Summary model for the Grid UI."""

    task_id: str
    task_display_name: str
    state: TaskInstanceState | None
    child_states: dict[TaskInstanceState | Literal["none"], int] | None
    min_start_date: datetime | None
    max_end_date: datetime | None
    dag_version_number: int | None = None
    has_note: bool = False
    loop_iterations_count: int | None = None


class GridTISummaries(BaseModel):
    """DAG Run model for the Grid UI."""

    run_id: str
    dag_id: str
    task_instances: list[LightGridTaskInstanceSummary]


class LoopIterationSummary(BaseModel):
    """Execution state for one existing iteration."""

    index: int
    state: TaskInstanceState | None = None
    start_date: datetime | None = None
    end_date: datetime | None = None


class LoopInvocationResponse(BaseModel):
    """One invocation of a loop in a Dag run."""

    region_id: UUID


class LoopSummaryResponse(BaseModel):
    """Runtime state of one loop invocation."""

    dag_id: str
    run_id: str
    group_id: str
    max_iterations: int
    iterations_ran: int
    status: Literal["running", "stopped_early", "ran_to_cap", "failed", "skipped", "removed"]
    stopped_at_iteration: int | None = None
    failed_at_iteration: int | None = None
    exit_criteria_doc: str | None = None
    exit_criteria_name: str | None = None
    reason: Literal["cap_reached", "iteration_failed"] | None = None
    reason_task_id: str | None = None
    loop_region_id: UUID | None = None
    loop_regions: list[LoopInvocationResponse] = []
    iterations: list[LoopIterationSummary]


class LoopRunSummary(BaseModel):
    """A loop invocation in a recent DAG run."""

    run_id: str
    run_after: datetime
    logical_date: datetime | None = None
    max_iterations: int
    iterations_ran: int
    status: Literal["running", "stopped_early", "ran_to_cap", "failed", "skipped", "removed"]
    reason: Literal["cap_reached", "iteration_failed"] | None = None
    loop_region_id: UUID | None = None


class LoopHistoryResponse(BaseModel):
    """Loop invocations across recent DAG runs."""

    dag_id: str
    group_id: str
    runs: list[LoopRunSummary]
