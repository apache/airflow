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

from flask import Response, current_app, request
from itsdangerous import BadSignature, URLSafeSerializer

from airflow.api_connexion import security
from airflow.api_connexion.exceptions import NotFound, PermissionDenied
from airflow.api_connexion.schemas.dag_source_schema import dag_source_schema
from airflow.auth.managers.models.resource_details import DagAccessEntity, DagDetails
from airflow.models.dag import DagModel
from airflow.models.dagcode import DagCode
from airflow.utils.session import NEW_SESSION, provide_session
from airflow.www.extensions.init_auth_manager import get_auth_manager

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

    from airflow.auth.managers.models.batch_apis import IsAuthorizedDagRequest


@security.requires_access_dag("GET", DagAccessEntity.CODE)
@provide_session
def get_dag_source(*, file_token: str, session: Session = NEW_SESSION) -> Response:
    """Get source code using file token."""
    secret_key = current_app.config["SECRET_KEY"]
    auth_s = URLSafeSerializer(secret_key)
    try:
        path = auth_s.loads(file_token)
        dag_ids = session.query(DagModel.dag_id).filter(DagModel.fileloc == path).all()
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
        dag_source = DagCode.code(path, session=session)
    except (BadSignature, FileNotFoundError):
        raise NotFound("Dag source not found")

    return_type = request.accept_mimetypes.best_match(["text/plain", "application/json"])
    if return_type == "text/plain":
        return Response(dag_source, headers={"Content-Type": return_type})
    if return_type == "application/json":
        content = dag_source_schema.dumps({"content": dag_source})
        return Response(content, headers={"Content-Type": return_type})
    return Response("Not Allowed Accept Header", status=HTTPStatus.NOT_ACCEPTABLE)
