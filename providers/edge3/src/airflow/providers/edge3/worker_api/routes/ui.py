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

from fastapi import Depends, HTTPException
from sqlalchemy import select

from airflow.api_fastapi.auth.managers.models.resource_details import AccessView
from airflow.api_fastapi.common.db.common import SessionDep  # noqa: TC001
from airflow.api_fastapi.common.router import AirflowRouter
from airflow.api_fastapi.core_api.security import GetUserDep, requires_access_view
from airflow.providers.edge3.models.edge_job import EdgeJobModel
from airflow.providers.edge3.models.edge_worker import EdgeWorkerModel, exit_maintenance, request_maintenance
from airflow.providers.edge3.worker_api.datamodels_ui import (
    Job,
    JobCollectionResponse,
    MaintenanceRequest,
    Worker,
    WorkerCollectionResponse,
)

ui_router = AirflowRouter(tags=["UI"])


@ui_router.get(
    "/worker",
    dependencies=[
        Depends(requires_access_view(access_view=AccessView.JOBS)),
    ],
)
def worker(
    session: SessionDep,
) -> WorkerCollectionResponse:
    """Return Edge Workers."""
    query = select(EdgeWorkerModel).order_by(EdgeWorkerModel.worker_name)
    workers: list[EdgeWorkerModel] = session.scalars(query)

    result = [
        Worker(
            worker_name=w.worker_name,
            queues=w.queues,
            state=w.state,
            jobs_active=w.jobs_active,
            sysinfo=w.sysinfo_json or {},
            maintenance_comments=w.maintenance_comment,
            first_online=w.first_online,
            last_heartbeat=w.last_update,
        )
        for w in workers
    ]
    return WorkerCollectionResponse(
        workers=result,
        total_entries=len(result),
    )


@ui_router.get(
    "/jobs",
    dependencies=[
        Depends(requires_access_view(access_view=AccessView.JOBS)),
    ],
)
def jobs(
    session: SessionDep,
) -> JobCollectionResponse:
    """Return Edge Jobs."""
    query = select(EdgeJobModel).order_by(EdgeJobModel.queued_dttm)
    jobs: list[EdgeJobModel] = session.scalars(query)

    result = [
        Job(
            dag_id=j.dag_id,
            task_id=j.task_id,
            run_id=j.run_id,
            map_index=j.map_index,
            try_number=j.try_number,
            state=j.state,
            queue=j.queue,
            queued_dttm=j.queued_dttm,
            edge_worker=j.edge_worker,
            last_update=j.last_update,
        )
        for j in jobs
    ]
    return JobCollectionResponse(
        jobs=result,
        total_entries=len(result),
    )


@ui_router.post(
    "/worker/{worker_name}/maintenance",
    dependencies=[
        Depends(requires_access_view(access_view=AccessView.JOBS)),
    ],
)
def request_worker_maintenance(
    worker_name: str,
    maintenance_request: MaintenanceRequest,
    session: SessionDep,
    user: GetUserDep,
) -> None:
    """Put a worker into maintenance mode."""
    # Check if worker exists first
    worker_query = select(EdgeWorkerModel).where(EdgeWorkerModel.worker_name == worker_name)
    worker = session.scalar(worker_query)
    if not worker:
        raise HTTPException(status_code=404, detail=f"Worker {worker_name} not found")

    # Format the comment with timestamp and username (username will be added by plugin layer)
    formatted_comment = f"[{datetime.now().strftime('%Y-%m-%d %H:%M')}] - {user.get_name()} put node into maintenance mode\nComment: {maintenance_request.maintenance_comment}"

    try:
        request_maintenance(worker_name, formatted_comment, session=session)
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@ui_router.delete(
    "/worker/{worker_name}/maintenance",
    dependencies=[
        Depends(requires_access_view(access_view=AccessView.JOBS)),
    ],
)
def exit_worker_maintenance(
    worker_name: str,
    session: SessionDep,
) -> None:
    """Exit a worker from maintenance mode."""
    # Check if worker exists first
    worker_query = select(EdgeWorkerModel).where(EdgeWorkerModel.worker_name == worker_name)
    worker = session.scalar(worker_query)
    if not worker:
        raise HTTPException(status_code=404, detail=f"Worker {worker_name} not found")

    try:
        exit_maintenance(worker_name, session=session)
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))
