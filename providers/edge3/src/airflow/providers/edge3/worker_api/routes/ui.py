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
from typing import TYPE_CHECKING, Annotated

from fastapi import Depends, HTTPException, Query, status

from airflow.exceptions import AirflowOptionalProviderFeatureException

try:
    from sqlalchemy import select
except ImportError:
    select = None

from airflow.api_fastapi.auth.managers.models.resource_details import AccessView
from airflow.api_fastapi.common.db.common import SessionDep  # noqa: TC001
from airflow.api_fastapi.common.router import AirflowRouter
from airflow.api_fastapi.core_api.security import GetUserDep, requires_access_view
from airflow.providers.edge3.models.edge_job import EdgeJobModel
from airflow.providers.edge3.models.edge_worker import (
    EdgeWorkerModel,
    EdgeWorkerState,
    add_worker_queues,
    change_maintenance_comment,
    exit_maintenance,
    remove_worker,
    remove_worker_queues,
    request_maintenance,
    request_shutdown,
)
from airflow.providers.edge3.worker_api.datamodels_ui import (
    Job,
    JobCollectionResponse,
    MaintenanceRequest,
    Worker,
    WorkerCollectionResponse,
)
from airflow.utils.state import TaskInstanceState

if TYPE_CHECKING:
    from sqlalchemy.engine import ScalarResult

ui_router = AirflowRouter(tags=["UI"])


@ui_router.get(
    "/worker",
    dependencies=[
        Depends(requires_access_view(access_view=AccessView.JOBS)),
    ],
)
def worker(
    session: SessionDep,
    worker_name_pattern: str | None = None,
    queue_name_pattern: str | None = None,
    state: Annotated[list[EdgeWorkerState] | None, Query()] = None,
) -> WorkerCollectionResponse:
    """Return Edge Workers."""
    if select is None:
        raise AirflowOptionalProviderFeatureException(
            "sqlalchemy is required for Edge Worker UI operations. "
            "Install it with: pip install 'apache-airflow-providers-edge3[sqlalchemy]'"
        )
    query = select(EdgeWorkerModel)
    if worker_name_pattern:
        query = query.where(EdgeWorkerModel.worker_name.ilike(f"%{worker_name_pattern}%"))
    if queue_name_pattern:
        query = query.where(EdgeWorkerModel._queues.ilike(f"%'{queue_name_pattern}%"))
    if state:
        query = query.where(EdgeWorkerModel.state.in_(state))
    query = query.order_by(EdgeWorkerModel.worker_name)
    workers: ScalarResult[EdgeWorkerModel] = session.scalars(query)

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
    if select is None:
        raise AirflowOptionalProviderFeatureException(
            "sqlalchemy is required for Edge Worker UI operations. "
            "Install it with: pip install 'apache-airflow-providers-edge3[sqlalchemy]'"
        )
    query = select(EdgeJobModel).order_by(EdgeJobModel.queued_dttm)
    jobs: ScalarResult[EdgeJobModel] = session.scalars(query)

    result = [
        Job(
            dag_id=j.dag_id,
            task_id=j.task_id,
            run_id=j.run_id,
            map_index=j.map_index,
            try_number=j.try_number,
            state=TaskInstanceState(j.state),
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
        raise HTTPException(status.HTTP_404_NOT_FOUND, detail=f"Worker {worker_name} not found")
    if not maintenance_request.maintenance_comment:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, detail="Maintenance comment is required")

    # Format the comment with timestamp and username (username will be added by plugin layer)
    formatted_comment = f"[{datetime.now().strftime('%Y-%m-%d %H:%M')}] - {user.get_name()} put node into maintenance mode\nComment: {maintenance_request.maintenance_comment}"

    try:
        request_maintenance(worker_name, formatted_comment, session=session)
    except Exception as e:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, detail=str(e))


@ui_router.patch(
    "/worker/{worker_name}/maintenance",
    dependencies=[
        Depends(requires_access_view(access_view=AccessView.JOBS)),
    ],
)
def update_worker_maintenance(
    worker_name: str,
    maintenance_request: MaintenanceRequest,
    session: SessionDep,
    user: GetUserDep,
) -> None:
    """Update maintenance comments for a worker."""
    # Check if worker exists first
    worker_query = select(EdgeWorkerModel).where(EdgeWorkerModel.worker_name == worker_name)
    worker = session.scalar(worker_query)
    if not worker:
        raise HTTPException(status.HTTP_404_NOT_FOUND, detail=f"Worker {worker_name} not found")
    if not maintenance_request.maintenance_comment:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, detail="Maintenance comment is required")

    # Format the comment with timestamp and username (username will be added by plugin layer)
    first_line = worker.maintenance_comment.split("\n", 1)[0] if worker.maintenance_comment else ""
    formatted_comment = f"{first_line}\n[{datetime.now().strftime('%Y-%m-%d %H:%M')}] - {user.get_name()} updated comment:\n{maintenance_request.maintenance_comment}"

    try:
        change_maintenance_comment(worker_name, formatted_comment, session=session)
    except Exception as e:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, detail=str(e))


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
        raise HTTPException(status.HTTP_404_NOT_FOUND, detail=f"Worker {worker_name} not found")

    try:
        exit_maintenance(worker_name, session=session)
    except Exception as e:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, detail=str(e))


@ui_router.post(
    "/worker/{worker_name}/shutdown",
    dependencies=[
        Depends(requires_access_view(access_view=AccessView.JOBS)),
    ],
)
def request_worker_shutdown(
    worker_name: str,
    session: SessionDep,
) -> None:
    """Request shutdown of a worker."""
    # Check if worker exists first
    worker_query = select(EdgeWorkerModel).where(EdgeWorkerModel.worker_name == worker_name)
    worker = session.scalar(worker_query)
    if not worker:
        raise HTTPException(status.HTTP_404_NOT_FOUND, detail=f"Worker {worker_name} not found")

    try:
        request_shutdown(worker_name, session=session)
    except Exception as e:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, detail=str(e))


@ui_router.delete(
    "/worker/{worker_name}",
    dependencies=[
        Depends(requires_access_view(access_view=AccessView.JOBS)),
    ],
)
def delete_worker(
    worker_name: str,
    session: SessionDep,
) -> None:
    """Delete a worker record from the system."""
    # Check if worker exists first
    worker_query = select(EdgeWorkerModel).where(EdgeWorkerModel.worker_name == worker_name)
    worker = session.scalar(worker_query)
    if not worker:
        raise HTTPException(status.HTTP_404_NOT_FOUND, detail=f"Worker {worker_name} not found")

    try:
        remove_worker(worker_name, session=session)
    except Exception as e:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, detail=str(e))


@ui_router.put(
    "/worker/{worker_name}/queues/{queue_name}",
    dependencies=[
        Depends(requires_access_view(access_view=AccessView.JOBS)),
    ],
)
def add_worker_queue(
    worker_name: str,
    queue_name: str,
    session: SessionDep,
) -> None:
    """Add a queue to a worker."""
    # Check if worker exists first
    worker_query = select(EdgeWorkerModel).where(EdgeWorkerModel.worker_name == worker_name)
    worker = session.scalar(worker_query)
    if not worker:
        raise HTTPException(status.HTTP_404_NOT_FOUND, detail=f"Worker {worker_name} not found")

    try:
        add_worker_queues(worker_name, [queue_name], session=session)
    except Exception as e:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, detail=str(e))


@ui_router.delete(
    "/worker/{worker_name}/queues/{queue_name}",
    dependencies=[
        Depends(requires_access_view(access_view=AccessView.JOBS)),
    ],
)
def remove_worker_queue(
    worker_name: str,
    queue_name: str,
    session: SessionDep,
) -> None:
    """Remove a queue from a worker."""
    # Check if worker exists first
    worker_query = select(EdgeWorkerModel).where(EdgeWorkerModel.worker_name == worker_name)
    worker = session.scalar(worker_query)
    if not worker:
        raise HTTPException(status.HTTP_404_NOT_FOUND, detail=f"Worker {worker_name} not found")

    try:
        remove_worker_queues(worker_name, [queue_name], session=session)
    except Exception as e:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, detail=str(e))
