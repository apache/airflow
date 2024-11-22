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

from fastapi import Depends, HTTPException, Request, status
from sqlalchemy.orm import Session
from sqlalchemy.sql import select
from typing_extensions import Annotated

from airflow.api_fastapi.common.db.common import get_session
from airflow.api_fastapi.common.router import AirflowRouter
from airflow.api_fastapi.core_api.datamodels.extra_links import ExtraLinksResponse
from airflow.api_fastapi.core_api.openapi.exceptions import create_openapi_http_exception_doc
from airflow.exceptions import TaskNotFound

if TYPE_CHECKING:
    from sqlalchemy.orm.session import Session

    from airflow.models import DAG


extra_links_router = AirflowRouter(
    tags=["Extra Links"], prefix="/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances/{task_id}/links"
)


@extra_links_router.get(
    "",
    responses=create_openapi_http_exception_doc(
        [status.HTTP_401_UNAUTHORIZED, status.HTTP_403_FORBIDDEN, status.HTTP_404_NOT_FOUND]
    ),
)
async def get_extra_links(
    dag_id: str,
    dag_run_id: str,
    task_id: str,
    session: Annotated[Session, Depends(get_session)],
    request: Request,
) -> ExtraLinksResponse:
    """Get extra links for task instance."""
    from airflow.models.taskinstance import TaskInstance

    dag: DAG = request.app.state.dag_bag.get_dag(dag_id)
    if not dag:
        raise HTTPException(status.HTTP_404_NOT_FOUND, f"DAG with ID = {dag_id} not found")

    try:
        task = dag.get_task(task_id)
    except TaskNotFound:
        raise HTTPException(status.HTTP_404_NOT_FOUND, f"Task with ID = {task_id} not found")

    ti = session.scalar(
        select(TaskInstance).where(
            TaskInstance.dag_id == dag_id,
            TaskInstance.run_id == dag_run_id,
            TaskInstance.task_id == task_id,
        )
    )

    if not ti:
        raise HTTPException(
            status.HTTP_404_NOT_FOUND,
            f"DAG Run with ID = {dag_run_id} not found",
        )

    all_extra_link_pairs = (
        (link_name, task.get_extra_links(ti, link_name)) for link_name in task.extra_links
    )
    all_extra_links = {link_name: link_url or None for link_name, link_url in sorted(all_extra_link_pairs)}
    return ExtraLinksResponse.model_validate(all_extra_links)
