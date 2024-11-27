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

from pydantic import BaseModel


class GridTaskInstanceSummary(BaseModel):
    """Task Instance Summary model for the Grid UI."""

    task_id: str
    try_number: int
    start_date: datetime | None
    end_date: datetime | None
    queued_dttm: datetime | None
    states: dict[str, int] | None
    task_count: int
    overall_state: str | None


class GridDAGRun(BaseModel):
    """DAG Run model for the Grid UI."""

    run_id: str
    queued_at: datetime | None
    start_date: datetime | None
    end_date: datetime | None
    state: str
    run_type: str
    data_interval_start: datetime | None
    data_interval_end: datetime | None
    version_number: UUID | None


class GridDAGRunwithTIs(GridDAGRun):
    """DAG Run model for the Grid UI with Task Instances."""

    task_instances: list[GridTaskInstanceSummary] | None


class GridResponse(BaseModel):
    """Response model for the Grid UI."""

    dag_runs: list[GridDAGRunwithTIs]
