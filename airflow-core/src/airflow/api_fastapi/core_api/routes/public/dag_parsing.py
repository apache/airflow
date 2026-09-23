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

from fastapi import Depends, Request, status
from itsdangerous import URLSafeSerializer

from airflow.api_fastapi.common.db.common import SessionDep
from airflow.api_fastapi.common.router import AirflowRouter
from airflow.api_fastapi.core_api.openapi.exceptions import create_openapi_http_exception_doc
from airflow.api_fastapi.core_api.security import requires_access_dag_from_file_token
from airflow.api_fastapi.logging.decorators import action_logging
from airflow.models.dagbag import DagPriorityParsingRequest

dag_parsing_router = AirflowRouter(tags=["DAG Parsing"], prefix="/parseDagFile/{file_token}")


@dag_parsing_router.put(
    "",
    responses=create_openapi_http_exception_doc([status.HTTP_404_NOT_FOUND]),
    status_code=status.HTTP_201_CREATED,
    dependencies=[Depends(requires_access_dag_from_file_token(method="PUT")), Depends(action_logging())],
)
def reparse_dag_file(file_token: str, session: SessionDep, request: Request) -> None:
    """Request re-parsing a Dag file."""
    payload = URLSafeSerializer(request.app.state.secret_key).loads(file_token)
    parsing_request = DagPriorityParsingRequest(
        bundle_name=payload["bundle_name"], relative_fileloc=payload["relative_fileloc"]
    )
    session.add(parsing_request)
