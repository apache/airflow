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

from fastapi import Depends, Header, HTTPException, Request, Response
from itsdangerous import BadSignature, URLSafeSerializer
from sqlalchemy.orm import Session
from typing_extensions import Annotated

from airflow.api_fastapi.common.db.common import get_session
from airflow.api_fastapi.common.router import AirflowRouter
from airflow.api_fastapi.core_api.openapi.exceptions import create_openapi_http_exception_doc
from airflow.api_fastapi.core_api.serializers.dag_sources import DAGSourceResponse
from airflow.models.dagcode import DagCode

dag_sources_router = AirflowRouter(tags=["DagSource"], prefix="/dagSources")

mime_type_text = "text/plain"
mime_type_json = "application/json"
mime_type_any = "*/*"


@dag_sources_router.get(
    "/{file_token}",
    responses={
        **create_openapi_http_exception_doc([400, 401, 403, 404, 406]),
        "200": {
            "description": "Successful Response",
            "content": {
                mime_type_text: {"schema": {"type": "string", "example": "dag code"}},
            },
        },
    },
    response_model=DAGSourceResponse,
)
async def get_dag_source(
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
        raise HTTPException(404, "DAG source not found")

    if accept.startswith(mime_type_text):
        return Response(dag_source_model.content, media_type=mime_type_text)
    if accept.startswith(mime_type_json) or accept.startswith(mime_type_any):
        return dag_source_model
    raise HTTPException(406, "Content not available for Accept header")
