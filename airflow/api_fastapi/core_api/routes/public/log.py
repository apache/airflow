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

import textwrap
from collections.abc import Iterable

from fastapi import HTTPException, Request, status
from itsdangerous import BadSignature, URLSafeSerializer
from pydantic import PositiveInt
from sqlalchemy.orm import joinedload
from sqlalchemy.sql import select
from starlette.responses import StreamingResponse

from airflow.api_fastapi.common.db.common import SessionDep
from airflow.api_fastapi.common.headers import HeaderAcceptJsonOrText
from airflow.api_fastapi.common.router import AirflowRouter
from airflow.api_fastapi.common.types import Mimetype
from airflow.api_fastapi.core_api.datamodels.log import TaskInstancesLogResponse
from airflow.api_fastapi.core_api.openapi.exceptions import create_openapi_http_exception_doc
from airflow.exceptions import TaskNotFound
from airflow.models import TaskInstance, Trigger
from airflow.models.taskinstancehistory import TaskInstanceHistory
from airflow.utils.log.log_reader import TaskLogReader

task_instances_log_router = AirflowRouter(
    tags=["Task Instance"], prefix="/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances"
)

text_example_response_for_get_log = {
    Mimetype.TEXT: {
        "schema": {
            "type": "string",
            "example": textwrap.dedent(
                """\
    content
    """
            ),
        }
    }
}


@task_instances_log_router.get(
    "/{task_id}/logs/{try_number}",
    responses={
        **create_openapi_http_exception_doc([status.HTTP_404_NOT_FOUND]),
        status.HTTP_200_OK: {
            "description": "Successful Response",
            "content": text_example_response_for_get_log,
        },
    },
    response_model=TaskInstancesLogResponse,
)
def get_log(
    dag_id: str,
    dag_run_id: str,
    task_id: str,
    try_number: PositiveInt,
    accept: HeaderAcceptJsonOrText,
    request: Request,
    session: SessionDep,
    full_content: bool = False,
    map_index: int = -1,
    token: str | None = None,
):
    """Get logs for a specific task instance."""
    if not token:
        metadata = {}
    else:
        try:
            metadata = URLSafeSerializer(request.app.state.secret_key).loads(token)
        except BadSignature:
            raise HTTPException(
                status.HTTP_400_BAD_REQUEST, "Bad Signature. Please use only the tokens provided by the API."
            )

    if metadata.get("download_logs") and metadata["download_logs"]:
        full_content = True

    if full_content:
        metadata["download_logs"] = True
    else:
        metadata["download_logs"] = False

    task_log_reader = TaskLogReader()

    if not task_log_reader.supports_read:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "Task log handler does not support read logs.")

    query = (
        select(TaskInstance)
        .where(
            TaskInstance.task_id == task_id,
            TaskInstance.dag_id == dag_id,
            TaskInstance.run_id == dag_run_id,
            TaskInstance.map_index == map_index,
        )
        .join(TaskInstance.dag_run)
        .options(joinedload(TaskInstance.trigger).joinedload(Trigger.triggerer_job))
    )
    ti = session.scalar(query)
    if ti is None:
        query = select(TaskInstanceHistory).where(
            TaskInstanceHistory.task_id == task_id,
            TaskInstanceHistory.dag_id == dag_id,
            TaskInstanceHistory.run_id == dag_run_id,
            TaskInstanceHistory.map_index == map_index,
            TaskInstanceHistory.try_number == try_number,
        )
        ti = session.scalar(query)

    if ti is None:
        metadata["end_of_log"] = True
        raise HTTPException(status.HTTP_404_NOT_FOUND, "TaskInstance not found")

    dag = request.app.state.dag_bag.get_dag(dag_id)
    if dag:
        try:
            ti.task = dag.get_task(ti.task_id)
        except TaskNotFound:
            pass

    log_streams: list[Iterable[str]]
    log_stream: Iterable[str]
    if accept == Mimetype.JSON or accept == Mimetype.ANY:  # default
        hosts, log_streams, metadata = task_log_reader.read_log_chunks(ti, try_number, metadata)
        # we must have token here, so we can safely ignore it
        token = URLSafeSerializer(request.app.state.secret_key).dumps(metadata)  # type: ignore[assignment]
        host = f"{hosts[0] or ''}\n"
        log_stream = log_streams[0]
        return TaskInstancesLogResponse(
            continuation_token=token, content=host + "\n".join(log for log in log_stream)
        ).model_dump()
    # text/plain. Stream
    log_stream = task_log_reader.read_log_stream(ti, try_number, metadata)
    return StreamingResponse(log_stream, media_type=Mimetype.TEXT.value)
