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

from pydantic import Field

from airflow.api_fastapi.common.types import UtcDateTime
from airflow.api_fastapi.core_api.base import BaseModel, StrictBaseModel
from airflow.utils.state import DagRunState


class TriggerDAGRunPayload(StrictBaseModel):
    """Schema for Trigger DAG Run API request."""

    logical_date: UtcDateTime | None = None
    conf: dict = Field(default_factory=dict)
    reset_dag_run: bool = False


class DagRunStateResponse(BaseModel):
    """Schema for DAG Run State response."""

    state: DagRunState


class DagRunResponse(BaseModel):
    """Schema for DAG Run response."""

    dag_id: str
    run_id: str
    logical_date: UtcDateTime
    start_date: UtcDateTime | None = None
    end_date: UtcDateTime | None = None
    state: DagRunState
    data_interval_start: UtcDateTime | None = None
    data_interval_end: UtcDateTime | None = None
