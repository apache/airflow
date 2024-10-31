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

from fastapi import Depends, HTTPException
from sqlalchemy.orm import Session, joinedload
from sqlalchemy.sql import select
from typing_extensions import Annotated

from airflow.api_fastapi.common.db.common import get_session
from airflow.api_fastapi.common.router import AirflowRouter
from airflow.api_fastapi.core_api.openapi.exceptions import create_openapi_http_exception_doc
from airflow.api_fastapi.core_api.serializers.task_instances import TaskInstanceResponse
from airflow.models.taskinstance import TaskInstance as TI

task_instances_router = AirflowRouter(
    tags=["Task Instance"], prefix="/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances"
)


@task_instances_router.get("/{task_id}", responses=create_openapi_http_exception_doc([401, 403, 404]))
async def get_task_instance(
    dag_id: str, dag_run_id: str, task_id: str, session: Annotated[Session, Depends(get_session)]
) -> TaskInstanceResponse:
    """Get task instance."""
    query = (
        select(TI)
        .where(TI.dag_id == dag_id, TI.run_id == dag_run_id, TI.task_id == task_id)
        .join(TI.dag_run)
        .options(joinedload(TI.rendered_task_instance_fields))
    )

    task_instance = session.scalar(query)

    if task_instance is None:
        raise HTTPException(
            404,
            f"The Task Instance with dag_id: `{dag_id}`, run_id: `{dag_run_id}` and task_id: `{task_id}` was not found",
        )
    if task_instance.map_index != -1:
        raise HTTPException(404, "Task instance is mapped, add the map_index value to the URL")

    return TaskInstanceResponse.model_validate(task_instance, from_attributes=True)
