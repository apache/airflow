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

from pydantic import Field

from airflow.api_fastapi.core_api.base import BaseModel


class DagScheduleOverviewEntry(BaseModel):
    """
    Aggregate, time-of-day statistics for a single Dag from its recent successful runs.

    ``start_*`` and ``end_*`` fields are expressed as integer seconds since
    midnight (0..86399) in UTC. ``duration_mean_seconds`` and
    ``duration_median_seconds`` are total wall-clock seconds for a run.
    """

    dag_id: str
    dag_display_name: str
    recent_runs_count: int = Field(ge=0)
    oldest_logical_date: datetime | None
    newest_logical_date: datetime | None
    start_mean_seconds: int | None = Field(default=None, ge=0, le=86399)
    start_median_seconds: int | None = Field(default=None, ge=0, le=86399)
    end_mean_seconds: int | None = Field(default=None, ge=0, le=86399)
    end_median_seconds: int | None = Field(default=None, ge=0, le=86399)
    duration_mean_seconds: float | None
    duration_median_seconds: float | None


class DagScheduleOverviewCollectionResponse(BaseModel):
    """Response model for the cross-Dag schedule overview endpoint."""

    total_entries: int
    entries: list[DagScheduleOverviewEntry]
