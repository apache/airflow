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

from airflow.api_fastapi.core_api.serializers.dag_run import DAGRunResponse
from airflow.api_fastapi.core_api.serializers.dags import DAGCollectionResponse, DAGResponse
from airflow.api_fastapi.core_api.serializers.optional import OptionalModel


class RecentDAGRunResponse(DAGRunResponse, OptionalModel):
    """Recent DAG Run response serializer."""

    execution_date: datetime


class RecentDAGResponse(DAGResponse, OptionalModel):
    """Recent DAG Runs response serializer."""

    latest_dag_runs: list[RecentDAGRunResponse]


class RecentDAGCollectionResponse(DAGCollectionResponse):
    """Recent DAG Runs collection response serializer."""

    # override parent's fields to use RecentDAGResponse instead of DAGResponse
    dags: list[RecentDAGResponse]  # type: ignore
