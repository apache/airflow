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
from sqlalchemy.orm import joinedload

from airflow.api_fastapi.auth.managers.models.resource_details import DagAccessEntity
from airflow.api_fastapi.common.db.common import SessionDep
from airflow.api_fastapi.common.router import AirflowRouter
from airflow.api_fastapi.core_api.datamodels.ui.deadline import DeadlineResponse
from airflow.api_fastapi.core_api.openapi.exceptions import create_openapi_http_exception_doc
from airflow.api_fastapi.core_api.security import requires_access_dag
from airflow.models.dagrun import DagRun
from airflow.models.deadline import Deadline

deadlines_router = AirflowRouter(prefix="/deadlines", tags=["Deadlines"])


@deadlines_router.get(
    "/{dag_id}/{run_id}",
    responses=create_openapi_http_exception_doc(
        [
            status.HTTP_404_NOT_FOUND,
        ]
    ),
    dependencies=[
        Depends(
            requires_access_dag(
                method="GET",
                access_entity=DagAccessEntity.RUN,
            )
        ),
    ],
)
def get_dag_run_deadlines(
    dag_id: str,
    run_id: str,
    session: SessionDep,
) -> list[DeadlineResponse]:
    """Get all deadlines for a specific DAG run."""
    dag_run = session.scalar(select(DagRun).where(DagRun.dag_id == dag_id, DagRun.run_id == run_id))
    if not dag_run:
        raise HTTPException(
            status.HTTP_404_NOT_FOUND,
            f"No DAG run found for dag_id={dag_id} run_id={run_id}",
        )

    deadlines = (
        session.scalars(
            select(Deadline)
            .where(Deadline.dagrun_id == dag_run.id)
            .options(joinedload(Deadline.deadline_alert))
            .order_by(Deadline.deadline_time.asc())
        )
        .unique()
        .all()
    )

    return [
        DeadlineResponse(
            id=d.id,
            deadline_time=d.deadline_time,
            missed=d.missed,
            created_at=d.created_at,
            alert_name=d.deadline_alert.name if d.deadline_alert else None,
            alert_description=d.deadline_alert.description if d.deadline_alert else None,
        )
        for d in deadlines
    ]
