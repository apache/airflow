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

import json
from datetime import datetime
from typing import Annotated

from sqlalchemy import select

from airflow.providers.edge.models.edge_worker import EdgeWorkerModel, EdgeWorkerState, set_metrics
from airflow.providers.edge.worker_api.auth import jwt_token_authorization_rest
from airflow.providers.edge.worker_api.datamodels import (
    WorkerQueueUpdateBody,
    WorkerSetStateReturn,
    WorkerStateBody,
)
from airflow.providers.edge.worker_api.routes._v2_compat import (
    AirflowRouter,
    Body,
    Depends,
    HTTPException,
    Path,
    SessionDep,
    create_openapi_http_exception_doc,
    status,
)
from airflow.stats import Stats
from airflow.utils import timezone

worker_router = AirflowRouter(
    tags=["Worker"],
    prefix="/worker",
    responses=create_openapi_http_exception_doc(
        [
            status.HTTP_400_BAD_REQUEST,
            status.HTTP_403_FORBIDDEN,
        ]
    ),
)


def _assert_version(sysinfo: dict[str, str | int]) -> None:
    """Check if the Edge Worker version matches the central API site."""
    from airflow import __version__ as airflow_version
    from airflow.providers.edge import __version__ as edge_provider_version

    # Note: In future, more stable versions we might be more liberate, for the
    #       moment we require exact version match for Edge Worker and core version
    if "airflow_version" in sysinfo:
        airflow_on_worker = sysinfo["airflow_version"]
        if airflow_on_worker != airflow_version:
            raise HTTPException(
                status.HTTP_400_BAD_REQUEST,
                f"Edge Worker runs on Airflow {airflow_on_worker} "
                f"and the core runs on {airflow_version}. Rejecting access due to difference.",
            )
    else:
        raise HTTPException(
            status.HTTP_400_BAD_REQUEST, "Edge Worker does not specify the version it is running on."
        )

    if "edge_provider_version" in sysinfo:
        provider_on_worker = sysinfo["edge_provider_version"]
        if provider_on_worker != edge_provider_version:
            raise HTTPException(
                status.HTTP_400_BAD_REQUEST,
                f"Edge Worker runs on Edge Provider {provider_on_worker} "
                f"and the core runs on {edge_provider_version}. Rejecting access due to difference.",
            )
    else:
        raise HTTPException(
            status.HTTP_400_BAD_REQUEST, "Edge Worker does not specify the provider version it is running on."
        )


_worker_name_doc = Path(title="Worker Name", description="Hostname or instance name of the worker")
_worker_state_doc = Body(
    title="Worker State",
    description="State of the worker with details",
    examples=[
        {
            "state": "running",
            "jobs_active": 3,
            "queues": ["large_node", "wisconsin_site"],
            "sysinfo": {
                "concurrency": 4,
                "airflow_version": "2.10.0",
                "edge_provider_version": "1.0.0",
            },
        }
    ],
)
_worker_queue_doc = Body(
    title="Changes in worker queues",
    description="Changes to be applied to current queues of worker",
    examples=[{"new_queues": ["new_queue"], "remove_queues": ["old_queue"]}],
)


def redefine_state_if_maintenance(
    worker_state: EdgeWorkerState, body_state: EdgeWorkerState
) -> EdgeWorkerState:
    """Redefine the state of the worker based on maintenance request."""
    if (
        worker_state == EdgeWorkerState.MAINTENANCE_REQUEST
        and body_state
        not in (
            EdgeWorkerState.MAINTENANCE_PENDING,
            EdgeWorkerState.MAINTENANCE_MODE,
        )
        or worker_state == EdgeWorkerState.OFFLINE_MAINTENANCE
        and body_state == EdgeWorkerState.STARTING
    ):
        return EdgeWorkerState.MAINTENANCE_REQUEST

    if worker_state == EdgeWorkerState.MAINTENANCE_EXIT:
        if body_state == EdgeWorkerState.MAINTENANCE_PENDING:
            return EdgeWorkerState.RUNNING
        if body_state == EdgeWorkerState.MAINTENANCE_MODE:
            return EdgeWorkerState.IDLE

    return body_state


@worker_router.post("/{worker_name}", dependencies=[Depends(jwt_token_authorization_rest)])
def register(
    worker_name: Annotated[str, _worker_name_doc],
    body: Annotated[WorkerStateBody, _worker_state_doc],
    session: SessionDep,
) -> datetime:
    """Register a new worker to the backend."""
    _assert_version(body.sysinfo)
    query = select(EdgeWorkerModel).where(EdgeWorkerModel.worker_name == worker_name)
    worker: EdgeWorkerModel = session.scalar(query)
    if not worker:
        worker = EdgeWorkerModel(worker_name=worker_name, state=body.state, queues=body.queues)
    worker.state = redefine_state_if_maintenance(worker.state, body.state)
    worker.queues = body.queues
    worker.sysinfo = json.dumps(body.sysinfo)
    worker.last_update = timezone.utcnow()
    session.add(worker)
    return worker.last_update


@worker_router.patch("/{worker_name}", dependencies=[Depends(jwt_token_authorization_rest)])
def set_state(
    worker_name: Annotated[str, _worker_name_doc],
    body: Annotated[WorkerStateBody, _worker_state_doc],
    session: SessionDep,
) -> WorkerSetStateReturn:
    """Set state of worker and returns the current assigned queues."""
    query = select(EdgeWorkerModel).where(EdgeWorkerModel.worker_name == worker_name)
    worker: EdgeWorkerModel = session.scalar(query)
    worker.state = redefine_state_if_maintenance(worker.state, body.state)
    worker.jobs_active = body.jobs_active
    worker.sysinfo = json.dumps(body.sysinfo)
    worker.last_update = timezone.utcnow()
    session.commit()
    Stats.incr(f"edge_worker.heartbeat_count.{worker_name}", 1, 1)
    Stats.incr("edge_worker.heartbeat_count", 1, 1, tags={"worker_name": worker_name})
    set_metrics(
        worker_name=worker_name,
        state=body.state,
        jobs_active=body.jobs_active,
        concurrency=int(body.sysinfo.get("concurrency", -1)),
        free_concurrency=int(body.sysinfo["free_concurrency"]),
        queues=worker.queues,
    )
    _assert_version(body.sysinfo)  #  Exception only after worker state is in the DB
    return WorkerSetStateReturn(state=worker.state, queues=worker.queues)


@worker_router.patch(
    "/queues/{worker_name}",
    dependencies=[Depends(jwt_token_authorization_rest)],
)
def update_queues(
    worker_name: Annotated[str, _worker_name_doc],
    body: Annotated[WorkerQueueUpdateBody, _worker_queue_doc],
    session: SessionDep,
) -> None:
    query = select(EdgeWorkerModel).where(EdgeWorkerModel.worker_name == worker_name)
    worker: EdgeWorkerModel = session.scalar(query)
    if body.new_queues:
        worker.add_queues(body.new_queues)
    if body.remove_queues:
        worker.remove_queues(body.remove_queues)
    session.add(worker)
