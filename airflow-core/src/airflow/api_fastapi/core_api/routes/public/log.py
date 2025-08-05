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

import contextlib
import textwrap

from fastapi import Depends, HTTPException, Request, status
from fastapi.responses import StreamingResponse
from itsdangerous import BadSignature, URLSafeSerializer
from pydantic import NonNegativeInt, PositiveInt
from sqlalchemy.orm import joinedload
from sqlalchemy.sql import select

from airflow.api_fastapi.common.dagbag import DagBagDep
from airflow.api_fastapi.common.db.common import SessionDep
from airflow.api_fastapi.common.headers import HeaderAcceptJsonOrNdjson
from airflow.api_fastapi.common.router import AirflowRouter
from airflow.api_fastapi.common.types import Mimetype
from airflow.api_fastapi.core_api.datamodels.log import ExternalLogUrlResponse, TaskInstancesLogResponse
from airflow.api_fastapi.core_api.openapi.exceptions import create_openapi_http_exception_doc
from airflow.api_fastapi.core_api.security import DagAccessEntity, requires_access_dag
from airflow.exceptions import TaskNotFound
from airflow.models import TaskInstance, Trigger
from airflow.models.taskinstancehistory import TaskInstanceHistory
from airflow.utils.log.log_reader import TaskLogReader

task_instances_log_router = AirflowRouter(
    tags=["Task Instance"], prefix="/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances"
)

ndjson_example_response_for_get_log = {
    Mimetype.NDJSON: {
        "schema": {
            "type": "string",
            "example": textwrap.dedent(
                """\
    {"content": "content"}
    {"content": "content"}
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
            "content": ndjson_example_response_for_get_log,
        },
    },
    dependencies=[Depends(requires_access_dag("GET", DagAccessEntity.TASK_LOGS))],
    response_model=TaskInstancesLogResponse,
    response_model_exclude_unset=True,
)
def get_log(
    dag_id: str,
    dag_run_id: str,
    task_id: str,
    try_number: NonNegativeInt,
    accept: HeaderAcceptJsonOrNdjson,
    request: Request,
    dag_bag: DagBagDep,
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

    metadata["download_logs"] = full_content

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
            TaskInstance.try_number == try_number,
        )
        .join(TaskInstance.dag_run)
        .options(joinedload(TaskInstance.trigger).joinedload(Trigger.triggerer_job))
        .options(joinedload(TaskInstance.dag_model))
    )
    ti = session.scalar(query)
    if ti is None:
        query = (
            select(TaskInstanceHistory)
            .where(
                TaskInstanceHistory.task_id == task_id,
                TaskInstanceHistory.dag_id == dag_id,
                TaskInstanceHistory.run_id == dag_run_id,
                TaskInstanceHistory.map_index == map_index,
                TaskInstanceHistory.try_number == try_number,
            )
            .options(joinedload(TaskInstanceHistory.dag_run))
            # we need to joinedload the dag_run, since FileTaskHandler._render_filename needs ti.dag_run
        )
        ti = session.scalar(query)

    if ti is None:
        metadata["end_of_log"] = True
        raise HTTPException(status.HTTP_404_NOT_FOUND, "TaskInstance not found")

    dag = dag_bag.get_dag_for_run(ti.dag_run, session=session)
    if dag:
        with contextlib.suppress(TaskNotFound):
            ti.task = dag.get_task(ti.task_id)

    if accept == Mimetype.NDJSON:  # only specified application/x-ndjson will return streaming response
        # LogMetadata(TypedDict) is used as type annotation for log_reader; added ignore to suppress mypy error
        log_stream = task_log_reader.read_log_stream(ti, try_number, metadata)  # type: ignore[arg-type]
        headers = None
        if not metadata.get("end_of_log", False):
            headers = {
                "Airflow-Continuation-Token": URLSafeSerializer(request.app.state.secret_key).dumps(metadata)
            }
        return StreamingResponse(media_type="application/x-ndjson", content=log_stream, headers=headers)

    # application/json, or something else we don't understand.
    # Return JSON format, which will be more easily for users to debug.

    # LogMetadata(TypedDict) is used as type annotation for log_reader; added ignore to suppress mypy error
    structured_log_stream, out_metadata = task_log_reader.read_log_chunks(ti, try_number, metadata)  # type: ignore[arg-type]
    encoded_token = None
    if not out_metadata.get("end_of_log", False):
        encoded_token = URLSafeSerializer(request.app.state.secret_key).dumps(out_metadata)
    return TaskInstancesLogResponse.model_construct(
        continuation_token=encoded_token, content=list(structured_log_stream)
    )


@task_instances_log_router.get(
    "/{task_id}/externalLogUrl/{try_number}",
    responses=create_openapi_http_exception_doc([status.HTTP_400_BAD_REQUEST, status.HTTP_404_NOT_FOUND]),
    dependencies=[Depends(requires_access_dag("GET", DagAccessEntity.TASK_INSTANCE))],
)
def get_external_log_url(
    dag_id: str,
    dag_run_id: str,
    task_id: str,
    try_number: PositiveInt,
    session: SessionDep,
    map_index: int = -1,
) -> ExternalLogUrlResponse:
    """Get external log URL for a specific task instance."""
    task_log_reader = TaskLogReader()

    if not task_log_reader.supports_external_link:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "Task log handler does not support external logs.")

    # Fetch the task instance
    query = (
        select(TaskInstance)
        .where(
            TaskInstance.task_id == task_id,
            TaskInstance.dag_id == dag_id,
            TaskInstance.run_id == dag_run_id,
            TaskInstance.map_index == map_index,
        )
        .options(joinedload(TaskInstance.dag_model))
    )
    ti = session.scalar(query)

    if ti is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "TaskInstance not found")

    url = task_log_reader.log_handler.get_external_log_url(ti, try_number)
    return ExternalLogUrlResponse(url=url)
