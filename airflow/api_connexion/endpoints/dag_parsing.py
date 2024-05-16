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

from flask import Response, current_app
from itsdangerous import BadSignature, URLSafeSerializer
from sqlalchemy import exc, select

from airflow.api_connexion import security
from airflow.api_connexion.exceptions import NotFound, PermissionDenied
from airflow.auth.managers.models.resource_details import DagDetails
from airflow.models.dag import DagModel
from airflow.models.dagbag import DagPriorityParsingRequest
from airflow.utils.session import NEW_SESSION, provide_session
from airflow.www.extensions.init_auth_manager import get_auth_manager

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

    from airflow.auth.managers.models.batch_apis import IsAuthorizedDagRequest


@security.requires_access_dag("PUT")
@provide_session
def reparse_dag_file(*, file_token: str, session: Session = NEW_SESSION) -> Response:
    """Request re-parsing a DAG file."""
    secret_key = current_app.config["SECRET_KEY"]
    auth_s = URLSafeSerializer(secret_key)
    try:
        path = auth_s.loads(file_token)
    except BadSignature:
        raise NotFound("File not found")

    requests: Sequence[IsAuthorizedDagRequest] = [
        {"method": "PUT", "details": DagDetails(id=dag_id)}
        for dag_id in session.scalars(select(DagModel.dag_id).where(DagModel.fileloc == path))
    ]
    if not requests:
        raise NotFound("File not found")

    # Check if user has read access to all the DAGs defined in the file
    if not get_auth_manager().batch_is_authorized_dag(requests):
        raise PermissionDenied()

    parsing_request = DagPriorityParsingRequest(fileloc=path)
    session.add(parsing_request)
    try:
        session.commit()
    except exc.IntegrityError:
        session.rollback()
        return Response("Duplicate request", HTTPStatus.CREATED)
    return Response(status=HTTPStatus.CREATED)
