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

from math import ceil
from typing import TYPE_CHECKING

from flask import Response, request
from itsdangerous.exc import BadSignature
from itsdangerous.url_safe import URLSafeSerializer
from sqlalchemy import select
from sqlalchemy.orm import joinedload

from airflow.api_connexion import security
from airflow.api_connexion.exceptions import BadRequest, NotFound
from airflow.api_connexion.schemas.log_schema import LogResponseObject, logs_schema
from airflow.auth.managers.models.resource_details import DagAccessEntity
from airflow.exceptions import TaskNotFound
from airflow.models import TaskInstance, Trigger
from airflow.providers.amazon.aws.hooks.s3 import S3Hook, provide_bucket_name, unify_bucket_name_and_key
from airflow.utils.airflow_flask_app import get_airflow_app
from airflow.utils.log.log_reader import TaskLogReader
from airflow.utils.session import NEW_SESSION, provide_session
from airflow.utils.state import TaskInstanceState

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

    from airflow.api_connexion.types import APIResponse


@security.requires_access_dag("GET", DagAccessEntity.TASK_LOGS)
@provide_session
def get_log(
    *,
    dag_id: str,
    dag_run_id: str,
    task_id: str,
    task_try_number: int,
    full_content: bool = False,
    map_index: int = -1,
    token: str | None = None,
    session: Session = NEW_SESSION,
) -> APIResponse:
    """Get logs for specific task instance."""
    key = get_airflow_app().config["SECRET_KEY"]
    metadata = {}
    if token:
        try:
            metadata = URLSafeSerializer(key).loads(token)
        except BadSignature:
            raise BadRequest("Bad Signature. Please use only the tokens provided by the API.")

    metadata["download_logs"] = full_content or metadata.get("download_logs", False)

    task_log_reader = TaskLogReader()

    if not task_log_reader.supports_read:
        raise BadRequest("Task log handler does not support read logs.")

    ti = session.scalar(
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
    if ti is None:
        metadata["end_of_log"] = True
        raise NotFound(title="TaskInstance not found")

    dag = get_airflow_app().dag_bag.get_dag(dag_id)
    if dag:
        ti.task = dag.get_task(ti.task_id)
        if ti.task is None:
            raise NotFound("Task not found in DAG")

    return_type = request.accept_mimetypes.best_match(["text/plain", "application/json"])

    if return_type in ["application/json", None]:  # default
        logs, metadata = task_log_reader.read_log_chunks(
            ti, task_try_number, metadata
        )
        logs = logs[0] if task_try_number is not None else logs
        token = URLSafeSerializer(key).dumps(metadata)
        return logs_schema.dump(LogResponseObject(continuation_token=token, content=logs))
    else:  # text/plain streaming
        logs = task_log_reader.read_log_stream(ti, task_try_number, metadata)
        return Response(logs, headers={"Content-Type": return_type})


@security.requires_access_dag("GET", DagAccessEntity.TASK_LOGS)
@provide_session
@unify_bucket_name_and_key
@provide_bucket_name
def get_log_pages(
    *,
    dag_id: str,
    dag_run_id: str,
    task_id: str,
    bucket_name: str,
    key: str,
    session: Session = NEW_SESSION,
) -> APIResponse:
    """Get total number of log pages for a specific task instance."""
    task_log_reader = TaskLogReader()

    if not task_log_reader.supports_read:
        raise BadRequest("Task log handler does not support read logs.")

    query = (
        select(TaskInstance)
        .where(
            TaskInstance.task_id == task_id,
            TaskInstance.dag_id == dag_id,
            TaskInstance.run_id == dag_run_id,
        )
        .join(TaskInstance.dag_run)
        .options(joinedload(TaskInstance.trigger).joinedload(Trigger.triggerer_job))
    )

    ti = session.scalar(query)

    # Check if the task instance state is terminal
    if ti.state is None:
        raise BadRequest("TaskInstance state is None")

    # Maybe add more?
    if ti.state not in {TaskInstanceState.SUCCESS, TaskInstanceState.FAILED, TaskInstanceState.UP_FOR_RETRY}:
        return {"total_pages": 1}

    # Fetch s3 log content length, change this to be generic in the future
    s3_hook = S3Hook()
    log_key = f"{dag_id}/{dag_run_id}/{task_id}/{key}"  # Updated to use key parameter
    page_size_kb = 100  # Hardcoded page size in KB for now
    page_size_bytes = page_size_kb * 1024

    try:
        content_length = s3_hook.get_content_length(key=log_key, bucket_name=bucket_name)
        total_pages = ceil(content_length / page_size_bytes)
    except Exception:
        raise BadRequest("Error fetching log content length")

    return {"total_pages": total_pages}
