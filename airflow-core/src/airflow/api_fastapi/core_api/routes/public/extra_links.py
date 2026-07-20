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

from fastapi import Depends, HTTPException, status
from sqlalchemy.sql import select

from airflow.api_fastapi.common.dagbag import DagBagDep, get_dag_for_run_or_latest_version
from airflow.api_fastapi.common.db.common import SessionDep
from airflow.api_fastapi.common.router import AirflowRouter
from airflow.api_fastapi.core_api.datamodels.extra_links import ExtraLinkCollectionResponse
from airflow.api_fastapi.core_api.openapi.exceptions import create_openapi_http_exception_doc
from airflow.api_fastapi.core_api.security import DagAccessEntity, requires_access_dag
from airflow.exceptions import TaskNotFound
from airflow.models import DagRun

extra_links_router = AirflowRouter(
    tags=["Extra Links"], prefix="/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances/{task_id}/links"
)


@extra_links_router.get(
    "",
    responses=create_openapi_http_exception_doc([status.HTTP_404_NOT_FOUND]),
    dependencies=[Depends(requires_access_dag("GET", DagAccessEntity.TASK_INSTANCE))],
    tags=["Task Instance"],
)
def get_extra_links(
    dag_id: str,
    dag_run_id: str,
    task_id: str,
    session: SessionDep,
    dag_bag: DagBagDep,
    map_index: int = -1,
    try_number: int | None = None,
) -> ExtraLinkCollectionResponse:
    """Get extra links for task instance."""
    from airflow.models.taskinstance import TaskInstance
    from airflow.models.taskinstancehistory import TaskInstanceHistory

    dag_run = session.scalar(select(DagRun).where(DagRun.dag_id == dag_id, DagRun.run_id == dag_run_id))

    ti = session.scalar(
        select(TaskInstance).where(
            TaskInstance.dag_id == dag_id,
            TaskInstance.run_id == dag_run_id,
            TaskInstance.task_id == task_id,
            TaskInstance.map_index == map_index,
        )
    )

    if not ti:
        raise HTTPException(
            status.HTTP_404_NOT_FOUND,
            "TaskInstance not found",
        )

    dag = get_dag_for_run_or_latest_version(dag_bag, dag_run, dag_id, session)

    try:
        task = dag.get_task(task_id)
    except TaskNotFound:
        raise HTTPException(status.HTTP_404_NOT_FOUND, f"Task with ID = {task_id} not found")

    # Resolve which object to use for link generation. For the current try we use
    # the live TI; for past tries we fetch the immutable TaskInstanceHistory record,
    # which also validates that the requested try_number actually exists.
    if try_number is not None and try_number != ti.try_number:
        tih = session.scalar(
            select(TaskInstanceHistory).where(
                TaskInstanceHistory.dag_id == dag_id,
                TaskInstanceHistory.task_id == task_id,
                TaskInstanceHistory.run_id == dag_run_id,
                TaskInstanceHistory.map_index == map_index,
                TaskInstanceHistory.try_number == try_number,
            )
        )
        if not tih:
            raise HTTPException(
                status.HTTP_404_NOT_FOUND,
                f"TaskInstanceHistory not found for try_number={try_number}",
            )
        ti_for_links = tih
    else:
        ti_for_links = ti

    all_extra_link_pairs = (
        (link_name, task.get_extra_links(ti_for_links, link_name))
        for link_name in task.extra_links  # type: ignore[arg-type]
    )
    all_extra_links = {link_name: link_url or None for link_name, link_url in sorted(all_extra_link_pairs)}

    return ExtraLinkCollectionResponse(
        extra_links=all_extra_links,
        total_entries=len(all_extra_links),
    )
