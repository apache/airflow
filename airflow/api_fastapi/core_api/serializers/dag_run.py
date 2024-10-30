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
from enum import Enum

from pydantic import BaseModel, Field

from airflow.utils.state import DagRunState
from airflow.utils.types import DagRunTriggeredByType, DagRunType


class DAGRunPatchStates(str, Enum):
    """Enum for DAG Run states when updating a DAG Run."""

    QUEUED = DagRunState.QUEUED
    SUCCESS = DagRunState.SUCCESS
    FAILED = DagRunState.FAILED


class DAGRunPatchBody(BaseModel):
    """DAG Run Serializer for PATCH requests."""

    state: DAGRunPatchStates


class DAGRunResponse(BaseModel):
    """DAG Run serializer for responses."""

    dag_run_id: str | None = Field(alias="run_id")
    dag_id: str
    logical_date: datetime | None
    start_date: datetime | None
    end_date: datetime | None
    data_interval_start: datetime | None
    data_interval_end: datetime | None
    last_scheduling_decision: datetime | None
    run_type: DagRunType
    state: DagRunState
    external_trigger: bool
    triggered_by: DagRunTriggeredByType
    conf: dict
    note: str | None
