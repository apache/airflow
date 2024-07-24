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
from airflow.models import TaskInstance, Trigger
from airflow.models.taskinstancehistory import TaskInstanceHistory
from airflow.providers.amazon.aws.hooks.s3 import S3Hook, provide_bucket_name, unify_bucket_name_and_key
from airflow.utils.airflow_flask_app import get_airflow_app
from airflow.utils.log.log_reader import TaskLogReader
from airflow.utils.session import NEW_SESSION, provide_session
from airflow.utils.state import TaskInstanceState

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

    from airflow.api_connexion.types import APIResponse


def format_log_entry(host, message):
    """Ensure each log entry ends with a newline for consistent formatting."""
    return (host, message if message.endswith("\n") else message + "\n")


def fetch_all_logs(task_log_reader, task_instance, try_number, metadata, initial_offset, limit):
    """Fetch all logs and handle pagination if necessary."""
    full_log = []
    current_offset = initial_offset
    total_data_read = 0

    while True:
        logs, new_metadata = task_log_reader.read_log_chunks(
            task_instance, try_number, metadata, offset=current_offset, limit=limit
        )
        for sublist in logs:
            for host, message in sublist:
                full_log.append(format_log_entry(host, message))
                total_data_read += len(message)  # Assume length of message contributes to offset

        # Check if the end of log has been reached or if we have read enough data
        if new_metadata.get("end_of_log", False) or total_data_read >= limit:
            metadata.update(new_metadata)
            break

        # Update current offset for the next read
        current_offset += total_data_read

    return full_log, metadata


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
    offset: int = 0,
    limit: int = 100,
    token: str | None = None,
    session: Session = NEW_SESSION,
) -> APIResponse:
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

    if offset < 0 or limit <= 0:
        raise BadRequest("Offset and Limit must be non-negative, and limit must be greater than 0")

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
            TaskInstanceHistory.try_number == task_try_number,
        )
        ti = session.scalar(query)

    if ti is None:
        metadata["end_of_log"] = True
        raise NotFound(title="TaskInstance not found")

    full_log, metadata = fetch_all_logs(task_log_reader, ti, task_try_number, metadata, offset, limit)
    return_type = request.accept_mimetypes.best_match(["text/plain", "application/json"])

    if return_type in ["application/json", None]:
        content = f"[{', '.join(f'({repr(host)}, {repr(message.strip())})' for host, message in full_log)}]"
        response_data = logs_schema.dump(
            LogResponseObject(continuation_token=URLSafeSerializer(key).dumps(metadata), content=content)
        )
        return response_data
    else:
        log_stream = "".join(f"{host}\n{message}" for host, message in full_log)
        return Response(log_stream, mimetype="text/plain", headers={"Content-Type": "text/plain"})


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
