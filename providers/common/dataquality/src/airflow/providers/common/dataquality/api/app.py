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
"""Read-only query API for data quality results."""

from __future__ import annotations

from typing import Annotated

from fastapi import Depends, FastAPI, HTTPException, Query

from airflow.api_fastapi.auth.managers.models.resource_details import DagAccessEntity
from airflow.api_fastapi.core_api.security import requires_access_dag
from airflow.providers.common.dataquality.api.models import (
    PaginatedResponse,
    RuleHistoryRecordModel,
    TaskDQRunModel,
)
from airflow.providers.common.dataquality.backends import get_backend_from_config
from airflow.providers.common.dataquality.backends.object_storage import ObjectStorageResultsBackend


def _get_backend() -> ObjectStorageResultsBackend:
    backend = get_backend_from_config()
    if backend is None:
        raise HTTPException(
            status_code=503,
            detail="No data quality results backend configured; set '[common.dataquality] results_path'.",
        )
    return backend


BackendDep = Annotated[ObjectStorageResultsBackend, Depends(_get_backend)]

dataquality_app = FastAPI(
    title="Data Quality",
    description="Read-only query API over data quality check results.",
)


@dataquality_app.get("/health")
async def health() -> dict[str, str]:
    """Liveness check."""
    return {"status": "ok"}


@dataquality_app.get(
    "/v1/dags/{dag_id}/tasks/{task_id}/runs",
    dependencies=[Depends(requires_access_dag(method="GET", access_entity=DagAccessEntity.TASK_INSTANCE))],
)
async def task_runs(
    dag_id: str,
    task_id: str,
    backend: BackendDep,
    limit: Annotated[int, Query(gt=0, le=200)] = 50,
    before: Annotated[str | None, Query()] = None,
) -> PaginatedResponse[TaskDQRunModel]:
    """
    Recent data quality runs for one task, newest first.

    ``before`` is the opaque ``next_cursor`` from the previous page; omit it for the first page.
    """
    return PaginatedResponse[TaskDQRunModel](**backend.read_task_runs(dag_id, task_id, limit, before))


@dataquality_app.get(
    "/v1/dags/{dag_id}/tasks/{task_id}/rules/{rule_uid}/history",
    dependencies=[Depends(requires_access_dag(method="GET", access_entity=DagAccessEntity.TASK_INSTANCE))],
)
async def task_rule_history(
    dag_id: str,
    task_id: str,
    rule_uid: str,
    backend: BackendDep,
    limit: Annotated[int, Query(gt=0, le=1000)] = 100,
    before: Annotated[str | None, Query()] = None,
) -> PaginatedResponse[RuleHistoryRecordModel]:
    """
    Recent results for one rule produced by one task, newest first.

    ``before`` is the opaque ``next_cursor`` from the previous page; omit it for the first page.
    """
    return PaginatedResponse[RuleHistoryRecordModel](
        **backend.read_task_rule_history(dag_id, task_id, rule_uid, limit, before)
    )


@dataquality_app.get(
    "/v1/dags/{dag_id}/tasks/{task_id}/runs/by_run/{run_id}",
    dependencies=[Depends(requires_access_dag(method="GET", access_entity=DagAccessEntity.TASK_INSTANCE))],
)
async def run_detail_by_task_instance(
    dag_id: str,
    task_id: str,
    run_id: str,
    backend: BackendDep,
    map_index: Annotated[int, Query()] = -1,
) -> TaskDQRunModel:
    """Look up a run by the route params available to the task-instance UI plugin."""
    try:
        payload = backend.read_by_task_instance(dag_id, task_id, run_id, map_index)
    except FileNotFoundError:
        raise HTTPException(
            status_code=404, detail="No data quality run found for this task instance."
        ) from None
    return TaskDQRunModel(**payload)
