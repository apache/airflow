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

from fastapi import HTTPException, Request, status

from airflow.api_fastapi.common.router import AirflowRouter
from airflow.api_fastapi.common.types import get_class_ref
from airflow.api_fastapi.core_api.datamodels.tasks import TaskResponse
from airflow.api_fastapi.core_api.openapi.exceptions import create_openapi_http_exception_doc
from airflow.exceptions import TaskNotFound
from airflow.models import DAG
from airflow.models.mappedoperator import MappedOperator

tasks_router = AirflowRouter(tags=["Task"], prefix="/dags/{dag_id}/tasks")


@tasks_router.get(
    "/{task_id}",
    responses=create_openapi_http_exception_doc(
        [
            status.HTTP_400_BAD_REQUEST,
            status.HTTP_401_UNAUTHORIZED,
            status.HTTP_403_FORBIDDEN,
            status.HTTP_404_NOT_FOUND,
        ]
    ),
)
def get_task(dag_id: str, task_id, request: Request) -> TaskResponse:
    """Get simplified representation of a task."""
    dag: DAG = request.app.state.dag_bag.get_dag(dag_id)
    if not dag:
        raise HTTPException(status.HTTP_404_NOT_FOUND, f"Dag with id {dag_id} was not found")
    try:
        task = dag.get_task(task_id=task_id)
        task.__dict__.update(
            {"class_ref": get_class_ref(task), "is_mapped": isinstance(task, MappedOperator)}
        )
    except TaskNotFound:
        raise HTTPException(status.HTTP_404_NOT_FOUND, f"Task with id {task_id} was not found")
    return TaskResponse.model_validate(task, from_attributes=True)
