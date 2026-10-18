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

from typing import TYPE_CHECKING, Annotated, Any

from fastapi import Depends, HTTPException, status
from sqlalchemy.sql import select

from airflow import plugins_manager
from airflow.api_fastapi.common.dagbag import DagBagDep, get_dag_for_run_or_latest_version
from airflow.api_fastapi.common.db.common import SessionDep
from airflow.api_fastapi.common.router import AirflowRouter
from airflow.api_fastapi.core_api.datamodels.extra_links import ExtraLinkCollectionResponse
from airflow.api_fastapi.core_api.openapi.exceptions import create_openapi_http_exception_doc
from airflow.api_fastapi.core_api.security import DagAccessEntity, requires_access_dag
from airflow.api_fastapi.core_api.services.public.task_coordinates import TaskScopeDep
from airflow.configuration import conf
from airflow.exceptions import TaskNotFound
from airflow.models.dag import DagModel
from airflow.models.task_coordinates import TaskCoordinateResolver
from airflow.models.taskinstance import TaskInstance

if TYPE_CHECKING:
    from airflow.serialization.serialized_objects import SerializedOperator

extra_links_router = AirflowRouter(
    tags=["Extra Links"], prefix="/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances/{task_id}/links"
)


def _get_try_number(try_number: int | None = None) -> int | None:
    return try_number


TryNumberDep = Annotated[int | None, Depends(_get_try_number)]


def _find_operator_link(task: SerializedOperator, link_name: str) -> Any:
    """
    Resolve a link name to the link object that will render it.

    Mirrors the lookup order of ``get_extra_links`` so the object inspected for team
    ownership is the one actually used, which matters when an operator link and a
    plugin link share a name.
    """
    return task.operator_extra_link_dict.get(link_name) or task.global_operator_extra_link_dict.get(link_name)


@extra_links_router.get(
    "",
    responses=create_openapi_http_exception_doc(
        [status.HTTP_400_BAD_REQUEST, status.HTTP_404_NOT_FOUND, status.HTTP_409_CONFLICT]
    ),
    dependencies=[Depends(requires_access_dag("GET", DagAccessEntity.TASK_INSTANCE))],
    tags=["Task Instance"],
)
def get_extra_links(
    dag_id: str,
    dag_run_id: str,
    task_id: str,
    session: SessionDep,
    dag_bag: DagBagDep,
    scope: TaskScopeDep,
    try_number: TryNumberDep,
) -> ExtraLinkCollectionResponse:
    """Get extra links for task instance."""
    query = select(TaskInstance).where(
        TaskInstance.dag_id == dag_id,
        TaskInstance.run_id == dag_run_id,
        TaskInstance.task_id == task_id,
        TaskInstance.region_id == scope.region_id,
        TaskInstance.region_index == scope.region_index,
    )
    if try_number is not None:
        query = query.where(TaskInstance.try_number == try_number).execution_options(
            include_all_attempts=True
        )
    ti = session.scalar(query)

    if not ti:
        raise HTTPException(
            status.HTTP_404_NOT_FOUND,
            "TaskInstance not found",
        )

    try:
        if ti.dag_version_id is None:
            task = get_dag_for_run_or_latest_version(dag_bag, ti.dag_run, dag_id, session).get_task(task_id)
        else:
            task = TaskCoordinateResolver(dag_bag, session).get_task(
                dag_id, dag_run_id, task_id, dag_version_id=ti.dag_version_id
            )
    except (TaskNotFound, ValueError):
        raise HTTPException(status.HTTP_404_NOT_FOUND, f"Task with ID = {task_id} not found")

    link_names: list[str] = task.extra_links
    if conf.getboolean("core", "multi_team"):
        dag_team_name = DagModel.get_team_name(dag_id)
        link_names = [
            link_name
            for link_name in link_names
            if plugins_manager.is_extra_link_visible_to_team(
                _find_operator_link(task, link_name), dag_team_name
            )
        ]

    all_extra_link_pairs = (
        (link_name, task.get_extra_links(ti, link_name))
        for link_name in link_names  # type: ignore[arg-type]
    )
    all_extra_links = {link_name: link_url or None for link_name, link_url in sorted(all_extra_link_pairs)}

    return ExtraLinkCollectionResponse(
        extra_links=all_extra_links,
        total_entries=len(all_extra_links),
    )
