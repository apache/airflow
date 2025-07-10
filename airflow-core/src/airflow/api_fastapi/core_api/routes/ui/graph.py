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

from fastapi import Depends
from sqlalchemy import select
from starlette import status

from airflow.api_fastapi.auth.managers.models.resource_details import DagAccessEntity
from airflow.api_fastapi.common.db.common import SessionDep
from airflow.api_fastapi.common.router import AirflowRouter
from airflow.api_fastapi.core_api.openapi.exceptions import create_openapi_http_exception_doc
from airflow.api_fastapi.core_api.routes.ui.grid import _get_latest_serdag
from airflow.api_fastapi.core_api.security import requires_access_dag
from airflow.models import TaskInstance
from airflow.models.serialized_dag import SerializedDagModel

graph_router = AirflowRouter(prefix="/graph", tags=["Graph"])


@graph_router.get(
    "/group_ids/{dag_id}",
    responses=create_openapi_http_exception_doc([status.HTTP_400_BAD_REQUEST, status.HTTP_404_NOT_FOUND]),
    dependencies=[
        Depends(requires_access_dag(method="GET", access_entity=DagAccessEntity.TASK_INSTANCE)),
        Depends(requires_access_dag(method="GET", access_entity=DagAccessEntity.RUN)),
    ],
    response_model_exclude_none=True,
)
def get_group_ids(
    dag_id: str,
    session: SessionDep,
    run_id: str | None = None,
) -> list[str]:
    """Return dag structure for grid view."""
    dag = None
    if run_id:
        versions_sq = select(TaskInstance.dag_version_id).where(
            TaskInstance.run_id == run_id,
            TaskInstance.dag_id == dag_id,
        )
        serdag_query = (
            select(SerializedDagModel)
            .where(SerializedDagModel.dag_version_id.in_(versions_sq))
            .order_by(SerializedDagModel.created_at.desc())
            .limit(1)
        )

        serdag = session.scalar(serdag_query)
        if serdag:
            dag = serdag.dag
    if not dag:
        latest_serdag = _get_latest_serdag(dag_id, session)
        dag = latest_serdag.dag

    group_ids = list(dag.task_group_dict.keys())
    return group_ids
