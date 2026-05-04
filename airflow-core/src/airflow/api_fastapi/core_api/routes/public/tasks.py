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

from typing import cast

from fastapi import Depends, HTTPException, status

from airflow.api_fastapi.auth.managers.models.resource_details import DagAccessEntity
from airflow.api_fastapi.common.dagbag import DagBagDep, get_latest_version_of_dag
from airflow.api_fastapi.common.db.common import SessionDep
from airflow.api_fastapi.common.router import AirflowRouter
from airflow.api_fastapi.core_api.datamodels.tasks import TaskCollectionResponse, TaskResponse
from airflow.api_fastapi.core_api.openapi.exceptions import create_openapi_http_exception_doc
from airflow.api_fastapi.core_api.security import requires_access_dag
from airflow.exceptions import TaskNotFound

tasks_router = AirflowRouter(tags=["Task"], prefix="/dags/{dag_id}/tasks")

_SORTABLE_TASK_FIELDS = {
    "task_id",
    "task_display_name",
    "owner",
    "start_date",
    "end_date",
    "trigger_rule",
    "depends_on_past",
    "wait_for_downstream",
    "retries",
    "queue",
    "pool",
    "pool_slots",
    "execution_timeout",
    "retry_delay",
    "retry_exponential_backoff",
    "priority_weight",
    "weight_rule",
    "ui_color",
    "ui_fgcolor",
    "operator_name",
}


@tasks_router.get(
    "",
    responses=create_openapi_http_exception_doc(
        [
            status.HTTP_400_BAD_REQUEST,
            status.HTTP_404_NOT_FOUND,
        ]
    ),
    dependencies=[Depends(requires_access_dag(method="GET", access_entity=DagAccessEntity.TASK))],
)
def get_tasks(
    dag_id: str,
    dag_bag: DagBagDep,
    session: SessionDep,
    order_by: str = "task_id",
) -> TaskCollectionResponse:
    """Get tasks for Dag."""
    dag = get_latest_version_of_dag(dag_bag, dag_id, session)
    lstripped_order_by = order_by.lstrip("-")
    if lstripped_order_by not in _SORTABLE_TASK_FIELDS:
        raise HTTPException(
            status.HTTP_400_BAD_REQUEST,
            f"Ordering with '{lstripped_order_by}' is disallowed or "
            f"the attribute does not exist on the model",
        )
    tasks = sorted(
        dag.tasks,
        key=lambda task: (getattr(task, lstripped_order_by) is None, getattr(task, lstripped_order_by)),
        reverse=(order_by[0:1] == "-"),
    )
    return TaskCollectionResponse(
        tasks=cast("list[TaskResponse]", tasks),
        total_entries=len(tasks),
    )


@tasks_router.get(
    "/{task_id}",
    responses=create_openapi_http_exception_doc(
        [
            status.HTTP_400_BAD_REQUEST,
            status.HTTP_404_NOT_FOUND,
        ]
    ),
    dependencies=[Depends(requires_access_dag(method="GET", access_entity=DagAccessEntity.TASK))],
)
def get_task(dag_id: str, task_id, session: SessionDep, dag_bag: DagBagDep) -> TaskResponse:
    """Get simplified representation of a task."""
    dag = get_latest_version_of_dag(dag_bag, dag_id, session)
    try:
        task = dag.get_task(task_id=task_id)
    except TaskNotFound:
        raise HTTPException(status.HTTP_404_NOT_FOUND, f"Task with id {task_id} was not found")
    return cast("TaskResponse", task)
