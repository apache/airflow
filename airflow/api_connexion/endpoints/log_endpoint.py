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

from typing import Any

from flask import Response, request
from itsdangerous.exc import BadSignature
from itsdangerous.url_safe import URLSafeSerializer
from sqlalchemy.orm.session import Session

from airflow.api_connexion import security
from airflow.api_connexion.exceptions import BadRequest, NotFound
from airflow.api_connexion.schemas.log_schema import LogResponseObject, logs_schema
from airflow.api_connexion.types import APIResponse
from airflow.exceptions import TaskNotFound
from airflow.models import TaskInstance
from airflow.security import permissions
from airflow.utils.airflow_flask_app import get_airflow_app
from airflow.utils.log.log_reader import TaskLogReader
from airflow.utils.session import NEW_SESSION, provide_session


@security.requires_access(
    [
        (permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG),
        (permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG_RUN),
        (permissions.ACTION_CAN_READ, permissions.RESOURCE_TASK_INSTANCE),
    ],
)
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
    """Get logs for specific task instance"""
    key = get_airflow_app().config["SECRET_KEY"]
    if not token:
        metadata = {}
    else:
        try:
            metadata = URLSafeSerializer(key).loads(token)
        except BadSignature:
            raise BadRequest("Bad Signature. Please use only the tokens provided by the API.")

    if metadata.get("download_logs") and metadata["download_logs"]:
        full_content = True

    if full_content:
        metadata["download_logs"] = True
    else:
        metadata["download_logs"] = False

    task_log_reader = TaskLogReader()
    if not task_log_reader.supports_read:
        raise BadRequest("Task log handler does not support read logs.")
    ti = (
        session.query(TaskInstance)
        .filter(
            TaskInstance.task_id == task_id,
            TaskInstance.dag_id == dag_id,
            TaskInstance.run_id == dag_run_id,
            TaskInstance.map_index == map_index,
        )
        .join(TaskInstance.dag_run)
        .one_or_none()
    )
    if ti is None:
        metadata["end_of_log"] = True
        raise NotFound(title="TaskInstance not found")

    dag = get_airflow_app().dag_bag.get_dag(dag_id)
    if dag:
        try:
            ti.task = dag.get_task(ti.task_id)
        except TaskNotFound:
            pass

    return_type = request.accept_mimetypes.best_match(["text/plain", "application/json"])

    # return_type would be either the above two or None
    logs: Any
    if return_type == "application/json" or return_type is None:  # default
        logs, metadata = task_log_reader.read_log_chunks(ti, task_try_number, metadata)
        logs = logs[0] if task_try_number is not None else logs
        # we must have token here, so we can safely ignore it
        token = URLSafeSerializer(key).dumps(metadata)  # type: ignore[assignment]
        return logs_schema.dump(LogResponseObject(continuation_token=token, content=logs))
    # text/plain. Stream
    logs = task_log_reader.read_log_stream(ti, task_try_number, metadata)

    return Response(logs, headers={"Content-Type": return_type})
