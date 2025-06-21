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

from pydantic import BaseModel, Field

from airflow.utils.state import DagRunState, TaskInstanceState
from airflow.utils.types import DagRunType


class GridTaskInstanceSummary(BaseModel):
    """Task Instance Summary model for the Grid UI."""

    task_id: str
    try_number: int
    start_date: datetime | None
    end_date: datetime | None
    queued_dttm: datetime | None
    child_states: dict[str, int] | None
    task_count: int
    state: TaskInstanceState | None
    note: str | None


class LightGridTaskInstanceSummary(BaseModel):
    """Task Instance Summary model for the Grid UI."""

    task_id: str
    state: TaskInstanceState | None


class GridDAGRunwithTIs(BaseModel):
    """DAG Run model for the Grid UI."""

    run_id: str = Field(serialization_alias="dag_run_id", validation_alias="run_id")
    queued_at: datetime | None
    start_date: datetime | None
    end_date: datetime | None
    run_after: datetime
    state: DagRunState
    run_type: DagRunType
    logical_date: datetime | None
    data_interval_start: datetime | None
    data_interval_end: datetime | None
    note: str | None
    task_instances: list[GridTaskInstanceSummary]


class GridTISummaries(BaseModel):
    """DAG Run model for the Grid UI."""

    run_id: str
    dag_id: str
    task_instances: list[LightGridTaskInstanceSummary]


class GridResponse(BaseModel):
    """Response model for the Grid UI."""

    dag_runs: list[GridDAGRunwithTIs]
