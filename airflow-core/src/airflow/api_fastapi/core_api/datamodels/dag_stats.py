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

from pydantic import AliasPath, Field

from airflow.api_fastapi.core_api.base import BaseModel
from airflow.utils.state import DagRunState


class DagStatsStateResponse(BaseModel):
    """DagStatsState serializer for responses."""

    state: DagRunState
    count: int


class DagStatsResponse(BaseModel):
    """DAG Stats serializer for responses."""

    dag_id: str
    dag_display_name: str = Field(validation_alias=AliasPath("dag_model", "dag_display_name"))
    stats: list[DagStatsStateResponse]


class DagStatsCollectionResponse(BaseModel):
    """DAG Stats Collection serializer for responses."""

    dags: list[DagStatsResponse]
    total_entries: int
