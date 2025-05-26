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

from operator import attrgetter
from typing import cast

from fastapi import Depends, HTTPException, status

from airflow.api_fastapi.auth.managers.models.resource_details import DagAccessEntity
from airflow.api_fastapi.common.dagbag import DagBagDep
from airflow.api_fastapi.common.router import AirflowRouter
from airflow.api_fastapi.core_api.datamodels.tasks import TaskCollectionResponse, TaskResponse
from airflow.api_fastapi.core_api.openapi.exceptions import create_openapi_http_exception_doc
from airflow.api_fastapi.core_api.security import requires_access_dag
from airflow.exceptions import TaskNotFound
from airflow.models import DAG

tasks_router = AirflowRouter(tags=["Task"], prefix="/dags/{dag_id}/tasks")


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
    order_by: str = "task_id",
) -> TaskCollectionResponse:
    """Get tasks for DAG."""
    dag: DAG = dag_bag.get_dag(dag_id)
    if not dag:
        raise HTTPException(status.HTTP_404_NOT_FOUND, f"Dag with id {dag_id} was not found")
    try:
        tasks = sorted(dag.tasks, key=attrgetter(order_by.lstrip("-")), reverse=(order_by[0:1] == "-"))
    except AttributeError as err:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, str(err))
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
def get_task(dag_id: str, task_id, dag_bag: DagBagDep) -> TaskResponse:
    """Get simplified representation of a task."""
    dag: DAG = dag_bag.get_dag(dag_id)
    if not dag:
        raise HTTPException(status.HTTP_404_NOT_FOUND, f"Dag with id {dag_id} was not found")
    try:
        task = dag.get_task(task_id=task_id)
    except TaskNotFound:
        raise HTTPException(status.HTTP_404_NOT_FOUND, f"Task with id {task_id} was not found")
    return cast("TaskResponse", task)
