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

from collections.abc import Sequence
from typing import TYPE_CHECKING

from fastapi import HTTPException, Request, status
from itsdangerous import BadSignature, URLSafeSerializer
from sqlalchemy import select

from airflow.api_fastapi.common.db.common import SessionDep
from airflow.api_fastapi.common.router import AirflowRouter
from airflow.api_fastapi.core_api.openapi.exceptions import create_openapi_http_exception_doc
from airflow.auth.managers.models.resource_details import DagDetails
from airflow.models.dag import DagModel
from airflow.models.dagbag import DagPriorityParsingRequest

if TYPE_CHECKING:
    from airflow.auth.managers.models.batch_apis import IsAuthorizedDagRequest

dag_parsing_router = AirflowRouter(tags=["DAG Parsing"], prefix="/parseDagFile/{file_token}")


@dag_parsing_router.put(
    "",
    responses=create_openapi_http_exception_doc([status.HTTP_404_NOT_FOUND]),
    status_code=status.HTTP_201_CREATED,
)
def reparse_dag_file(
    file_token: str,
    session: SessionDep,
    request: Request,
) -> None:
    """Request re-parsing a DAG file."""
    secret_key = request.app.state.secret_key
    auth_s = URLSafeSerializer(secret_key)
    try:
        path = auth_s.loads(file_token)
    except BadSignature:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "File not found")

    requests: Sequence[IsAuthorizedDagRequest] = [
        {"method": "PUT", "details": DagDetails(id=dag_id)}
        for dag_id in session.scalars(select(DagModel.dag_id).where(DagModel.fileloc == path))
    ]
    if not requests:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "File not found")

    parsing_request = DagPriorityParsingRequest(fileloc=path)
    session.add(parsing_request)
