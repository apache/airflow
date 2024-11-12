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

from typing import Annotated

from fastapi import Depends, Header, HTTPException, Request, Response, status
from itsdangerous import BadSignature, URLSafeSerializer
from sqlalchemy.orm import Session

from airflow.api_fastapi.common.db.common import get_session
from airflow.api_fastapi.common.router import AirflowRouter
from airflow.api_fastapi.core_api.datamodels.dag_sources import DAGSourceResponse
from airflow.api_fastapi.core_api.openapi.exceptions import create_openapi_http_exception_doc
from airflow.models.dagcode import DagCode

dag_sources_router = AirflowRouter(tags=["DagSource"], prefix="/dagSources")

mime_type_text = "text/plain"
mime_type_json = "application/json"
mime_type_any = "*/*"


@dag_sources_router.get(
    "/{file_token}",
    responses={
        **create_openapi_http_exception_doc(
            [
                status.HTTP_400_BAD_REQUEST,
                status.HTTP_401_UNAUTHORIZED,
                status.HTTP_403_FORBIDDEN,
                status.HTTP_404_NOT_FOUND,
                status.HTTP_406_NOT_ACCEPTABLE,
            ]
        ),
        "200": {
            "description": "Successful Response",
            "content": {
                mime_type_text: {"schema": {"type": "string", "example": "dag code"}},
            },
        },
    },
    response_model=DAGSourceResponse,
)
def get_dag_source(
    file_token: str,
    session: Annotated[Session, Depends(get_session)],
    request: Request,
    accept: Annotated[str, Header()] = mime_type_any,
):
    """Get source code using file token."""
    auth_s = URLSafeSerializer(request.app.state.secret_key)

    try:
        path = auth_s.loads(file_token)
        dag_source_model = DAGSourceResponse(
            content=DagCode.code(path, session=session),
        )
    except (BadSignature, FileNotFoundError):
        raise HTTPException(status.HTTP_404_NOT_FOUND, "DAG source not found")

    if accept.startswith(mime_type_text):
        return Response(dag_source_model.content, media_type=mime_type_text)
    if accept.startswith(mime_type_json) or accept.startswith(mime_type_any):
        return dag_source_model
    raise HTTPException(status.HTTP_406_NOT_ACCEPTABLE, "Content not available for Accept header")
