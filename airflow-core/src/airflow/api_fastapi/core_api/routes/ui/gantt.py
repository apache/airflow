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
from sqlalchemy import or_, select

from airflow.api_fastapi.auth.managers.models.resource_details import DagAccessEntity
from airflow.api_fastapi.common.db.common import SessionDep
from airflow.api_fastapi.common.router import AirflowRouter
from airflow.api_fastapi.core_api.datamodels.ui.gantt import GanttResponse, GanttTaskInstance
from airflow.api_fastapi.core_api.openapi.exceptions import create_openapi_http_exception_doc
from airflow.api_fastapi.core_api.security import requires_access_dag
from airflow.models.taskinstance import TaskInstance
from airflow.utils.state import TaskInstanceState

gantt_router = AirflowRouter(prefix="/gantt", tags=["Gantt"])


@gantt_router.get(
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
                access_entity=DagAccessEntity.TASK_INSTANCE,
            )
        ),
        Depends(
            requires_access_dag(
                method="GET",
                access_entity=DagAccessEntity.RUN,
            )
        ),
    ],
)
def get_gantt_data(
    dag_id: str,
    run_id: str,
    session: SessionDep,
) -> GanttResponse:
    """Get all task instance tries for Gantt chart."""
    # Pending retries retain timing for backoff; only the archived attempt belongs on the chart.
    current_tis = select(
        TaskInstance.task_id.label("task_id"),
        TaskInstance.task_display_name.label("task_display_name"),  # type: ignore[attr-defined]
        TaskInstance.try_number.label("try_number"),
        TaskInstance.state.label("state"),
        TaskInstance.scheduled_dttm.label("scheduled_dttm"),
        TaskInstance.queued_dttm.label("queued_dttm"),
        TaskInstance.start_date.label("start_date"),
        TaskInstance.end_date.label("end_date"),
    ).where(
        TaskInstance.dag_id == dag_id,
        TaskInstance.run_id == run_id,
        TaskInstance.region_index == -1,
        or_(TaskInstance.state != TaskInstanceState.UP_FOR_RETRY, TaskInstance.state.is_(None)),
    )

    query = current_tis.order_by(TaskInstance.task_id, TaskInstance.try_number)

    results = session.execute(query).fetchall()

    if not results:
        raise HTTPException(
            status.HTTP_404_NOT_FOUND,
            f"No task instances for dag_id={dag_id} run_id={run_id}",
        )

    task_instances = [
        GanttTaskInstance(
            task_id=row.task_id,
            task_display_name=row.task_display_name,
            try_number=row.try_number,
            state=row.state,
            scheduled_dttm=row.scheduled_dttm,
            queued_dttm=row.queued_dttm,
            start_date=row.start_date,
            end_date=row.end_date,
        )
        for row in results
    ]

    return GanttResponse(dag_id=dag_id, run_id=run_id, task_instances=task_instances)
