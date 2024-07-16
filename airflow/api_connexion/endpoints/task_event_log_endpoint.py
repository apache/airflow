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

from typing import TYPE_CHECKING

from sqlalchemy import select

from airflow.api_connexion import security
from airflow.api_connexion.parameters import check_limit, format_parameters
from airflow.api_connexion.schemas.task_event_log_schema import (
    TaskEventLogCollection,
    task_event_log_collection_schema,
)
from airflow.auth.managers.models.resource_details import DagAccessEntity
from airflow.models.taskinstance import TaskEventLog
from airflow.utils.db import get_query_count
from airflow.utils.session import NEW_SESSION, provide_session

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

    from airflow.api_connexion.types import APIResponse


@security.requires_access_dag("GET", DagAccessEntity.TASK_EVENT_LOG)
@format_parameters({"limit": check_limit})
@provide_session
def get_task_event_logs(
    *,
    dag_id: str | None = None,
    task_id: str | None = None,
    run_id: str | None = None,
    map_index: int | None = None,
    try_number: int | None = None,
    limit: int,
    offset: int | None = None,
    session: Session = NEW_SESSION,
) -> APIResponse:
    """Get all log entries from event log."""
    query = select(TaskEventLog)

    if dag_id:
        query = query.where(TaskEventLog.dag_id == dag_id)
    if task_id:
        query = query.where(TaskEventLog.task_id == task_id)
    if run_id:
        query = query.where(TaskEventLog.run_id == run_id)
    if map_index:
        query = query.where(TaskEventLog.map_index == map_index)
    if try_number:
        query = query.where(TaskEventLog.try_number == try_number)

    total_entries = get_query_count(query, session=session)

    query = query.order_by(TaskEventLog.id)
    logs = session.scalars(query.offset(offset).limit(limit)).all()
    return task_event_log_collection_schema.dump(
        TaskEventLogCollection(data=logs, total_entries=total_entries)
    )
