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
from typing import TYPE_CHECKING

from airflow.api_connexion import security
from airflow.api_connexion.exceptions import BadRequest, NotFound
from airflow.api_connexion.schemas.task_schema import (
    TaskCollection,
    task_collection_schema,
    task_schema,
)
from airflow.auth.managers.models.resource_details import DagAccessEntity
from airflow.exceptions import TaskNotFound
from airflow.utils.airflow_flask_app import get_airflow_app

if TYPE_CHECKING:
    from airflow import DAG
    from airflow.api_connexion.types import APIResponse


@security.requires_access_dag("GET", DagAccessEntity.TASK)
def get_task(*, dag_id: str, task_id: str) -> APIResponse:
    """Get simplified representation of a task."""
    dag: DAG = get_airflow_app().dag_bag.get_dag(dag_id)
    if not dag:
        raise NotFound("DAG not found")

    try:
        task = dag.get_task(task_id=task_id)
    except TaskNotFound:
        raise NotFound("Task not found")
    return task_schema.dump(task)


@security.requires_access_dag("GET", DagAccessEntity.TASK)
def get_tasks(*, dag_id: str, order_by: str = "task_id") -> APIResponse:
    """Get tasks for DAG."""
    dag: DAG = get_airflow_app().dag_bag.get_dag(dag_id)
    if not dag:
        raise NotFound("DAG not found")
    tasks = dag.tasks

    try:
        tasks = sorted(
            tasks, key=attrgetter(order_by.lstrip("-")), reverse=(order_by[0:1] == "-")
        )
    except AttributeError as err:
        raise BadRequest(detail=str(err))
    task_collection = TaskCollection(tasks=tasks, total_entries=len(tasks))
    return task_collection_schema.dump(task_collection)
