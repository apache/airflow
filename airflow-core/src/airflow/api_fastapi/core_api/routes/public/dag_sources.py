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
from sqlalchemy import select

from airflow.api_fastapi.app import get_auth_manager
from airflow.api_fastapi.auth.managers.models.resource_details import DagAccessEntity
from airflow.api_fastapi.common.db.common import SessionDep
from airflow.api_fastapi.common.headers import HeaderAcceptJsonOrText
from airflow.api_fastapi.common.router import AirflowRouter
from airflow.api_fastapi.common.types import Mimetype
from airflow.api_fastapi.core_api.datamodels.dag_sources import DAGSourceResponse
from airflow.api_fastapi.core_api.openapi.exceptions import create_openapi_http_exception_doc
from airflow.api_fastapi.core_api.security import GetUserDep, requires_access_dag
from airflow.models import DagModel
from airflow.models.dag_version import DagVersion

REDACTED_SOURCE = "REDACTED - you do not have read permission on all Dags in the file"
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
    user: GetUserDep,
    version_number: int | None = None,
):
    """Get source code using file token."""
    dag_version = DagVersion.get_version(dag_id, version_number, session=session)
    if not dag_version:
        raise HTTPException(
            status.HTTP_404_NOT_FOUND,
            f"The source code of the Dag {dag_id}, version_number {version_number} was not found",
        )
    if not dag_version.dag_code:
        raise HTTPException(
            status.HTTP_404_NOT_FOUND,
            detail=f"Code not found. dag_id='{dag_id}' version_number='{version_number}'",
        )

    # Per-file authorization overlay on top of the ``DagAccessEntity.CODE``
    # check above: a single source file may define multiple Dags, and the
    # caller having CODE access to ``dag_id`` does not imply they may read
    # every other Dag co-located in the file. Match the file by
    # ``(relative_fileloc, bundle_name)`` -- the same keying
    # ``import_error.py`` uses for its equivalent check -- and redact the
    # response when any co-located Dag is not in the caller's readable set.
    content = dag_version.dag_code.source_code
    dag_model = dag_version.dag_model
    if dag_model is not None and dag_model.relative_fileloc:
        file_dag_ids = set(
            session.scalars(
                select(DagModel.dag_id).where(
                    DagModel.relative_fileloc == dag_model.relative_fileloc,
                    DagModel.bundle_name == dag_model.bundle_name,
                )
            ).all()
        )
        if file_dag_ids:
            readable_dag_ids = get_auth_manager().get_authorized_dag_ids(user=user)
            if not file_dag_ids.issubset(readable_dag_ids):
                content = REDACTED_SOURCE

    dag_source_model = DAGSourceResponse(
        dag_id=dag_id,
        content=content,
        version_number=dag_version.version_number,
        dag_display_name=dag_version.dag_model.dag_display_name,
    )

    if accept == Mimetype.TEXT:
        return Response(dag_source_model.content, media_type=Mimetype.TEXT)
    return dag_source_model
