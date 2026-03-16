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

from airflow.api_fastapi.core_api.base import BaseModel


class PartitionedDagRunResponse(BaseModel):
    """Single partitioned Dag run item."""

    id: int
    partition_key: str
    created_at: str | None = None
    total_received: int
    total_required: int
    dag_id: str | None = None
    state: str | None = None
    created_dag_run_id: str | None = None


class PartitionedDagRunCollectionResponse(BaseModel):
    """Collection of partitioned Dag runs."""

    partitioned_dag_runs: list[PartitionedDagRunResponse]
    total: int
    asset_expressions: dict[str, dict | None] | None = None


class PartitionedDagRunAssetResponse(BaseModel):
    """Asset info within a partitioned Dag run detail."""

    asset_id: int
    asset_name: str
    asset_uri: str
    received: bool


class PartitionedDagRunDetailResponse(BaseModel):
    """Detail of a single partitioned Dag run."""

    id: int
    dag_id: str
    partition_key: str
    created_at: str | None = None
    updated_at: str | None = None
    created_dag_run_id: str | None = None
    assets: list[PartitionedDagRunAssetResponse]
    total_required: int
    total_received: int
    asset_expression: dict | None = None
