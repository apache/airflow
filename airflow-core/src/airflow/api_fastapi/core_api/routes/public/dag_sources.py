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

from fastapi import Depends, HTTPException, Response, status

from airflow.api_fastapi.auth.managers.models.resource_details import DagAccessEntity
from airflow.api_fastapi.common.db.common import SessionDep
from airflow.api_fastapi.common.headers import HeaderAcceptJsonOrText
from airflow.api_fastapi.common.router import AirflowRouter
from airflow.api_fastapi.common.types import Mimetype
from airflow.api_fastapi.core_api.datamodels.dag_sources import DAGSourceResponse
from airflow.api_fastapi.core_api.openapi.exceptions import create_openapi_http_exception_doc
from airflow.api_fastapi.core_api.security import requires_access_dag
from airflow.models.dag_version import DagVersion

dag_sources_router = AirflowRouter(tags=["DagSource"], prefix="/dagSources")


@dag_sources_router.get(
    "/{dag_id}",
    responses={
        **create_openapi_http_exception_doc(
            [
                status.HTTP_400_BAD_REQUEST,
                status.HTTP_404_NOT_FOUND,
                status.HTTP_406_NOT_ACCEPTABLE,
            ]
        ),
        "200": {
            "description": "Successful Response",
            "content": {
                Mimetype.TEXT: {"schema": {"type": "string", "example": "dag code"}},
            },
        },
    },
    response_model=DAGSourceResponse,
    dependencies=[Depends(requires_access_dag(method="GET", access_entity=DagAccessEntity.CODE))],
)
def get_dag_source(
    accept: HeaderAcceptJsonOrText,
    dag_id: str,
    session: SessionDep,
    version_number: int | None = None,
):
    """Get source code using file token."""
    dag_version = DagVersion.get_version(dag_id, version_number, session=session)
    if not dag_version:
        raise HTTPException(
            status.HTTP_404_NOT_FOUND,
            f"The source code of the DAG {dag_id}, version_number {version_number} was not found",
        )
    if not dag_version.dag_code:
        raise HTTPException(
            status.HTTP_404_NOT_FOUND,
            detail=f"Code not found. dag_id='{dag_id}' version_number='{version_number}'",
        )
    dag_source_model = DAGSourceResponse(
        dag_id=dag_id,
        content=dag_version.dag_code.source_code,
        version_number=dag_version.version_number,
    )

    if accept == Mimetype.TEXT:
        return Response(dag_source_model.content, media_type=Mimetype.TEXT)
    return dag_source_model
