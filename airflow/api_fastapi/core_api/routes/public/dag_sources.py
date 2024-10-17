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

from fastapi import Depends, HTTPException, Request, Response
from itsdangerous import BadSignature, URLSafeSerializer
from sqlalchemy.orm import Session
from typing_extensions import Annotated

from airflow.api_fastapi.common.db.common import get_session
from airflow.api_fastapi.common.router import AirflowRouter
from airflow.api_fastapi.core_api.openapi.exceptions import create_openapi_http_exception_doc
from airflow.api_fastapi.core_api.serializers.dag_sources import DAGSourceModel
from airflow.models.dagcode import DagCode

dag_sources_router = AirflowRouter(tags=["DagSource"], prefix="/dagSources")
mime_type_text = "text/plain"
mime_type_json = "application/json"
supported_mime_types = [mime_type_text, mime_type_json]


def _get_matching_mime_type(request: Request) -> str | None:
    """Return the matching MIME type in the request's ACCEPT header."""
    accept_header = request.headers.get("Accept", "").lower()
    for mime_type in supported_mime_types:
        if mime_type in accept_header:
            return mime_type
    return None


@dag_sources_router.get(
    "/{file_token}",
    responses=create_openapi_http_exception_doc([400, 401, 403, 404, 422]),
)
async def get_dag_source(
    file_token: str,
    session: Annotated[Session, Depends(get_session)],
    request: Request,
) -> Response:
    """Get source code using file token."""
    auth_s = URLSafeSerializer(request.app.state.secret_key)

    try:
        path = auth_s.loads(file_token)
        dag_source = DagCode.code(path, session=session)
    except (BadSignature, FileNotFoundError):
        raise HTTPException(404, "DAG source not found")

    return_type = _get_matching_mime_type(request)

    if return_type == mime_type_text:
        return Response(dag_source, media_type=return_type)

    if return_type == mime_type_json:
        content = DAGSourceModel(content=dag_source).model_dump_json()
        return Response(content, media_type=return_type)

    raise HTTPException(406, "Content not available for Accept header")
