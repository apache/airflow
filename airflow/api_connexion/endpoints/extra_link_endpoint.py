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
from airflow.api_connexion.exceptions import NotFound
from airflow.auth.managers.models.resource_details import DagAccessEntity
from airflow.exceptions import TaskNotFound
from airflow.utils.airflow_flask_app import get_airflow_app
from airflow.utils.session import NEW_SESSION, provide_session

if TYPE_CHECKING:
    from sqlalchemy.orm.session import Session

    from airflow import DAG
    from airflow.api_connexion.types import APIResponse
    from airflow.models.dagbag import DagBag


@security.requires_access_dag("GET", DagAccessEntity.TASK_INSTANCE)
@provide_session
def get_extra_links(
    *,
    dag_id: str,
    dag_run_id: str,
    task_id: str,
    session: Session = NEW_SESSION,
) -> APIResponse:
    """Get extra links for task instance."""
    from airflow.models.taskinstance import TaskInstance

    dagbag: DagBag = get_airflow_app().dag_bag
    dag: DAG = dagbag.get_dag(dag_id)
    if not dag:
        raise NotFound("DAG not found", detail=f'DAG with ID = "{dag_id}" not found')

    try:
        task = dag.get_task(task_id)
    except TaskNotFound:
        raise NotFound("Task not found", detail=f'Task with ID = "{task_id}" not found')

    ti = session.scalar(
        select(TaskInstance).where(
            TaskInstance.dag_id == dag_id,
            TaskInstance.run_id == dag_run_id,
            TaskInstance.task_id == task_id,
        )
    )

    if not ti:
        raise NotFound("DAG Run not found", detail=f'DAG Run with ID = "{dag_run_id}" not found')

    all_extra_link_pairs = (
        (link_name, task.get_extra_links(ti, link_name)) for link_name in task.extra_links
    )
    all_extra_links = {link_name: link_url or None for link_name, link_url in sorted(all_extra_link_pairs)}
    return all_extra_links
