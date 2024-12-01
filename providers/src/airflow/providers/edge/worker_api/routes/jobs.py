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

from ast import literal_eval
from typing import Annotated

from sqlalchemy import select, update

from airflow.providers.edge.models.edge_job import EdgeJobModel
from airflow.providers.edge.worker_api.auth import jwt_token_authorization_rest
from airflow.providers.edge.worker_api.datamodels import (
    EdgeJobFetched,
    WorkerApiDocs,
    WorkerQueuesBody,
)
from airflow.providers.edge.worker_api.routes._v2_compat import (
    AirflowRouter,
    Body,
    Depends,
    SessionDep,
    create_openapi_http_exception_doc,
    status,
)
from airflow.utils import timezone
from airflow.utils.sqlalchemy import with_row_locks
from airflow.utils.state import TaskInstanceState

jobs_router = AirflowRouter(tags=["Jobs"], prefix="/jobs")


@jobs_router.get(
    "/fetch/{worker_name}",
    dependencies=[Depends(jwt_token_authorization_rest)],
    responses=create_openapi_http_exception_doc(
        [
            status.HTTP_400_BAD_REQUEST,
            status.HTTP_403_FORBIDDEN,
        ]
    ),
)
def fetch(
    worker_name: str,
    body: Annotated[
        WorkerQueuesBody,
        Body(
            title="Log data chunks",
            description="The queues and capacity from which the worker can fetch jobs.",
        ),
    ],
    session: SessionDep,
) -> EdgeJobFetched | None:
    """Fetch a job to execute on the edge worker."""
    query = (
        select(EdgeJobModel)
        .where(
            EdgeJobModel.state == TaskInstanceState.QUEUED,
            EdgeJobModel.concurrency_slots <= body.free_concurrency,
        )
        .order_by(EdgeJobModel.queued_dttm)
    )
    if body.queues:
        query = query.where(EdgeJobModel.queue.in_(body.queues))
    query = query.limit(1)
    query = with_row_locks(query, of=EdgeJobModel, session=session, skip_locked=True)
    job: EdgeJobModel = session.scalar(query)
    if not job:
        return None
    job.state = TaskInstanceState.RUNNING
    job.edge_worker = worker_name
    job.last_update = timezone.utcnow()
    session.commit()
    return EdgeJobFetched(
        dag_id=job.dag_id,
        task_id=job.task_id,
        run_id=job.run_id,
        map_index=job.map_index,
        try_number=job.try_number,
        command=literal_eval(job.command),
        concurrency_slots=job.concurrency_slots,
    )


@jobs_router.patch(
    "/state/{dag_id}/{task_id}/{run_id}/{try_number}/{map_index}/{state}",
    dependencies=[Depends(jwt_token_authorization_rest)],
    responses=create_openapi_http_exception_doc(
        [
            status.HTTP_400_BAD_REQUEST,
            status.HTTP_403_FORBIDDEN,
        ]
    ),
)
def state(
    dag_id: Annotated[str, WorkerApiDocs.dag_id],
    task_id: Annotated[str, WorkerApiDocs.task_id],
    run_id: Annotated[str, WorkerApiDocs.run_id],
    try_number: Annotated[int, WorkerApiDocs.try_number],
    map_index: Annotated[int, WorkerApiDocs.map_index],
    state: Annotated[TaskInstanceState, WorkerApiDocs.state],
    session: SessionDep,
) -> None:
    """Update the state of a job running on the edge worker."""
    query = (
        update(EdgeJobModel)
        .where(
            EdgeJobModel.dag_id == dag_id,
            EdgeJobModel.task_id == task_id,
            EdgeJobModel.run_id == run_id,
            EdgeJobModel.map_index == map_index,
            EdgeJobModel.try_number == try_number,
        )
        .values(state=state, last_update=timezone.utcnow())
    )
    session.execute(query)
