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

from functools import cache
from pathlib import Path
from typing import TYPE_CHECKING, Annotated

from airflow.configuration import conf
from airflow.models.taskinstance import TaskInstance
from airflow.models.taskinstancekey import TaskInstanceKey
from airflow.providers.edge.models.edge_logs import EdgeLogsModel
from airflow.providers.edge.worker_api.auth import jwt_token_authorization_rest
from airflow.providers.edge.worker_api.datamodels import PushLogsBody, WorkerApiDocs
from airflow.providers.edge.worker_api.routes._v2_compat import (
    AirflowRouter,
    Body,
    Depends,
    SessionDep,
    create_openapi_http_exception_doc,
    status,
)
from airflow.utils.session import NEW_SESSION, provide_session

logs_router = AirflowRouter(tags=["Logs"], prefix="/logs")


@cache
@provide_session
def _logfile_path(task: TaskInstanceKey, session=NEW_SESSION) -> str:
    """Elaborate the (relative) path and filename to expect from task execution."""
    from airflow.utils.log.file_task_handler import FileTaskHandler

    ti = TaskInstance.get_task_instance(
        dag_id=task.dag_id,
        run_id=task.run_id,
        task_id=task.task_id,
        map_index=task.map_index,
        session=session,
    )
    if TYPE_CHECKING:
        assert ti
        assert isinstance(ti, TaskInstance)
    return FileTaskHandler(".")._render_filename(ti, task.try_number)


@logs_router.get(
    "/logfile_path/{dag_id}/{task_id}/{run_id}/{try_number}/{map_index}",
    dependencies=[Depends(jwt_token_authorization_rest)],
    responses=create_openapi_http_exception_doc(
        [
            status.HTTP_400_BAD_REQUEST,
            status.HTTP_403_FORBIDDEN,
        ]
    ),
)
def logfile_path(
    dag_id: Annotated[str, WorkerApiDocs.dag_id],
    task_id: Annotated[str, WorkerApiDocs.task_id],
    run_id: Annotated[str, WorkerApiDocs.run_id],
    try_number: Annotated[int, WorkerApiDocs.try_number],
    map_index: Annotated[int, WorkerApiDocs.map_index],
) -> str:
    """Elaborate the path and filename to expect from task execution."""
    task = TaskInstanceKey(
        dag_id=dag_id, task_id=task_id, run_id=run_id, try_number=try_number, map_index=map_index
    )
    return _logfile_path(task)


@logs_router.post(
    "/push/{dag_id}/{task_id}/{run_id}/{try_number}/{map_index}",
    dependencies=[Depends(jwt_token_authorization_rest)],
    responses=create_openapi_http_exception_doc(
        [
            status.HTTP_400_BAD_REQUEST,
            status.HTTP_403_FORBIDDEN,
        ]
    ),
)
def push_logs(
    dag_id: Annotated[str, WorkerApiDocs.dag_id],
    task_id: Annotated[str, WorkerApiDocs.task_id],
    run_id: Annotated[str, WorkerApiDocs.run_id],
    try_number: Annotated[int, WorkerApiDocs.try_number],
    map_index: Annotated[int, WorkerApiDocs.map_index],
    body: Annotated[
        PushLogsBody,
        Body(
            title="Log data chunks",
            description="The worker remote has no access to log sink and with this can send log chunks to the central site.",
        ),
    ],
    session: SessionDep,
) -> None:
    """Push an incremental log chunk from Edge Worker to central site."""
    log_chunk = EdgeLogsModel(
        dag_id=dag_id,
        task_id=task_id,
        run_id=run_id,
        map_index=map_index,
        try_number=try_number,
        log_chunk_time=body.log_chunk_time,
        log_chunk_data=body.log_chunk_data,
    )
    session.add(log_chunk)
    # Write logs to local file to make them accessible
    task = TaskInstanceKey(
        dag_id=dag_id, task_id=task_id, run_id=run_id, try_number=try_number, map_index=map_index
    )
    base_log_folder = conf.get("logging", "base_log_folder", fallback="NOT AVAILABLE")
    logfile_path = Path(base_log_folder, _logfile_path(task))
    if not logfile_path.exists():
        new_folder_permissions = int(
            conf.get("logging", "file_task_handler_new_folder_permissions", fallback="0o775"), 8
        )
        logfile_path.parent.mkdir(parents=True, exist_ok=True, mode=new_folder_permissions)
    with logfile_path.open("a") as logfile:
        logfile.write(body.log_chunk_data)
