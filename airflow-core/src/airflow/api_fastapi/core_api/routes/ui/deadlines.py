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

from typing import Annotated

from fastapi import Depends, HTTPException, status
from sqlalchemy import select
from sqlalchemy.orm import joinedload

from airflow.api_fastapi.auth.managers.models.resource_details import DagAccessEntity
from airflow.api_fastapi.common.db.common import SessionDep, paginated_select
from airflow.api_fastapi.common.parameters import QueryLimit, QueryOffset, SortParam
from airflow.api_fastapi.common.router import AirflowRouter
from airflow.api_fastapi.core_api.datamodels.ui.deadline import DealineCollectionResponse
from airflow.api_fastapi.core_api.openapi.exceptions import create_openapi_http_exception_doc
from airflow.api_fastapi.core_api.security import requires_access_dag
from airflow.models.dagrun import DagRun
from airflow.models.deadline import Deadline
from airflow.models.deadline_alert import DeadlineAlert

deadlines_router = AirflowRouter(prefix="/dags/{dag_id}/dagRuns/{dag_run_id}/deadlines", tags=["Deadlines"])


@deadlines_router.get(
    "",
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
    dag_run_id: str,
    session: SessionDep,
    limit: QueryLimit,
    offset: QueryOffset,
    order_by: Annotated[
        SortParam,
        Depends(
            SortParam(
                ["id", "deadline_time", "created_at"],
                Deadline,
                to_replace={
                    "alert_name": DeadlineAlert.name,
                },
            ).dynamic_depends(default="deadline_time")
        ),
    ],
) -> DealineCollectionResponse:
    """Get all deadlines for a specific DAG run."""
    dag_run = session.scalar(select(DagRun).where(DagRun.dag_id == dag_id, DagRun.run_id == dag_run_id))

    if not dag_run:
        raise HTTPException(
            status.HTTP_404_NOT_FOUND,
            f"No DAG run found for dag_id={dag_id} dag_run_id={dag_run_id}",
        )

    query = (
        select(Deadline)
        .join(Deadline.dagrun)
        .outerjoin(Deadline.deadline_alert)
        .where(Deadline.dagrun_id == dag_run.id)
        .where(DagRun.dag_id == dag_id)
        .options(joinedload(Deadline.deadline_alert))
    )

    deadlines_select, total_entries = paginated_select(
        statement=query,
        filters=None,
        order_by=order_by,
        offset=offset,
        limit=limit,
        session=session,
    )

    deadlines = session.scalars(deadlines_select)

    return DealineCollectionResponse(deadlines=deadlines, total_entries=total_entries)
