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

from http import HTTPStatus
from typing import TYPE_CHECKING, Sequence

from flask import Response, request
from sqlalchemy import select

from airflow.api_connexion import security
from airflow.api_connexion.exceptions import NotFound, PermissionDenied
from airflow.api_connexion.schemas.dag_source_schema import dag_source_schema
from airflow.auth.managers.models.resource_details import DagAccessEntity, DagDetails
from airflow.models.dag import DagModel
from airflow.models.dag_version import DagVersion
from airflow.utils.api_migration import mark_fastapi_migration_done
from airflow.utils.session import NEW_SESSION, provide_session
from airflow.www.extensions.init_auth_manager import get_auth_manager

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

    from airflow.auth.managers.models.batch_apis import IsAuthorizedDagRequest


@mark_fastapi_migration_done
@security.requires_access_dag("GET", DagAccessEntity.CODE)
@provide_session
def get_dag_source(
    *,
    dag_id: str,
    version_name: str | None = None,
    version_number: int | None = None,
    session: Session = NEW_SESSION,
) -> Response:
    """Get source code from DagCode."""
    dag_version = DagVersion.get_version(dag_id, version_number, version_name, session=session)
    if not dag_version:
        raise NotFound(
            f"The source code of the DAG {dag_id}, version {version_name} and version_number {version_number} was not found"
        )
    path = dag_version.dag_code.fileloc
    dag_ids = session.scalars(select(DagModel.dag_id).where(DagModel.fileloc == path)).all()
    requests: Sequence[IsAuthorizedDagRequest] = [
        {
            "method": "GET",
            "details": DagDetails(id=dag_id[0]),
        }
        for dag_id in dag_ids
    ]

    # Check if user has read access to all the DAGs defined in the file
    if not get_auth_manager().batch_is_authorized_dag(requests):
        raise PermissionDenied()
    dag_source = dag_version.dag_code.source_code
    version_number = dag_version.version_number
    version_name = dag_version.version_name

    return_type = request.accept_mimetypes.best_match(["text/plain", "application/json"])
    if return_type == "text/plain":
        return Response(dag_source, headers={"Content-Type": return_type})
    if return_type == "application/json":
        content = dag_source_schema.dumps(
            {
                "content": dag_source,
                "dag_id": dag_id,
                "version_name": version_name,
                "version_number": version_number,
            }
        )
        return Response(content, headers={"Content-Type": return_type})
    return Response("Not Allowed Accept Header", status=HTTPStatus.NOT_ACCEPTABLE)
