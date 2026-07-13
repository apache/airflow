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
from airflow.api_fastapi.core_api.datamodels.common import MaybeAssetExpression
from airflow.api_fastapi.core_api.datamodels.dags import DAGResponse
from airflow.api_fastapi.core_api.datamodels.hitl import HITLDetail
from airflow.api_fastapi.core_api.datamodels.ui.dag_runs import DAGRunLightResponse
from airflow.utils.state import DagRunState


class DAGWithLatestDagRunsResponse(DAGResponse):
    """DAG with latest dag runs response serializer."""

    asset_expression: MaybeAssetExpression
    latest_dag_runs: list[DAGRunLightResponse]
    pending_actions: list[HITLDetail]
    is_favorite: bool


class DAGWithLatestDagRunsCollectionResponse(BaseModel):
    """DAG with latest dag runs collection response serializer."""

    total_entries: int
    dags: list[DAGWithLatestDagRunsResponse]


class DAGRunStateCountsResponse(BaseModel):
    """Per-Dag counts of DagRuns grouped by state."""

    dag_id: str
    state_counts: dict[DagRunState, int]


class DAGsRunStateCountsCollectionResponse(BaseModel):
    """Collection of per-Dag DagRun-state counts for the Dag list page."""

    dags: list[DAGRunStateCountsResponse]
    state_count_limit: int
