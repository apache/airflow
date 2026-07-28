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

from fastapi import Depends, HTTPException, status
from sqlalchemy import select

from airflow.api_fastapi.auth.managers.models.resource_details import DagAccessEntity
from airflow.api_fastapi.common.db.common import SessionDep
from airflow.api_fastapi.common.router import AirflowRouter
from airflow.api_fastapi.core_api.datamodels.ui.dag_runs import DagRunStatsResponse
from airflow.api_fastapi.core_api.openapi.exceptions import create_openapi_http_exception_doc
from airflow.api_fastapi.core_api.security import requires_access_dag
from airflow.api_fastapi.core_api.services.ui.dag_run import compute_duration_stats
from airflow.models.dagrun import DagRun
from airflow.utils.state import DagRunState

dag_runs_router = AirflowRouter(prefix="/dags/{dag_id}/dagRuns", tags=["DagRun"])


@dag_runs_router.get(
    "/{dag_run_id}/stats",
    responses=create_openapi_http_exception_doc([status.HTTP_404_NOT_FOUND]),
    dependencies=[Depends(requires_access_dag(method="GET", access_entity=DagAccessEntity.RUN))],
)
def get_dag_run_stats(dag_id: str, dag_run_id: str, session: SessionDep) -> DagRunStatsResponse:
    """Get duration statistics for a DAG based on its historical completed runs."""
    if not session.scalar(select(DagRun.id).filter_by(dag_id=dag_id, run_id=dag_run_id)):
        raise HTTPException(
            status.HTTP_404_NOT_FOUND,
            f"The DagRun with dag_id: `{dag_id}` and run_id: `{dag_run_id}` was not found",
        )

    durations = [
        d
        for d in session.scalars(
            select(DagRun.duration.expression)  # type: ignore[attr-defined]
            .where(
                DagRun.dag_id == dag_id,
                DagRun.state.in_([DagRunState.SUCCESS, DagRunState.FAILED]),
            )
            .order_by(DagRun.run_after.desc())
            .limit(100)
        )
        if d is not None
    ]

    return DagRunStatsResponse(duration=compute_duration_stats(durations))
