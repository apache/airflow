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
from typing import Generic, Literal, TypeVar

from pydantic import computed_field

from airflow._shared.timezones import timezone
from airflow.api_fastapi.core_api.base import BaseModel
from airflow.utils.state import TaskInstanceState
from airflow.utils.types import DagRunType


class BaseEdgeResponse(BaseModel):
    """Base Edge serializer for responses."""

    source_id: str
    target_id: str


class BaseNodeResponse(BaseModel):
    """Base Node serializer for responses."""

    id: str
    label: str
    type: Literal[
        "join",
        "task",
        "asset-condition",
        "asset",
        "asset-alias",
        "asset-name-ref",
        "asset-uri-ref",
        "dag",
        "sensor",
        "trigger",
    ]


E = TypeVar("E", bound=BaseEdgeResponse)
N = TypeVar("N", bound=BaseNodeResponse)


class GridNodeResponse(BaseModel):
    """Base Node serializer for responses."""

    id: str
    label: str
    children: list[GridNodeResponse] | None = None
    is_mapped: bool | None
    setup_teardown_type: Literal["setup", "teardown"] | None = None


class GridRunsResponse(BaseModel):
    """Base Node serializer for responses."""

    dag_id: str
    run_id: str
    queued_at: datetime | None
    start_date: datetime | None
    end_date: datetime | None
    run_after: datetime
    state: TaskInstanceState | None
    run_type: DagRunType

    @computed_field
    def duration(self) -> float:
        if self.start_date:
            end_date = self.end_date or timezone.utcnow()
            return (end_date - self.start_date).total_seconds()
        return 0


class BaseGraphResponse(BaseModel, Generic[E, N]):
    """Base Graph serializer for responses."""

    edges: list[E]
    nodes: list[N]
