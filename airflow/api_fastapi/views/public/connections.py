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

from fastapi import Depends, HTTPException
from sqlalchemy import select
from sqlalchemy.orm import Session
from typing_extensions import Annotated

from airflow.api_fastapi.db.common import get_session
from airflow.api_fastapi.openapi.exceptions import create_openapi_http_exception_doc
from airflow.api_fastapi.serializers.connections import ConnectionResponse
from airflow.api_fastapi.views.router import AirflowRouter
from airflow.models import Connection

connections_router = AirflowRouter(tags=["Connection"])


@connections_router.delete(
    "/connections/{connection_id}",
    status_code=204,
    responses=create_openapi_http_exception_doc([401, 403, 404]),
)
async def delete_connection(
    connection_id: str,
    session: Annotated[Session, Depends(get_session)],
):
    """Delete a connection entry."""
    connection = session.scalar(select(Connection).filter_by(conn_id=connection_id))

    if connection is None:
        raise HTTPException(404, f"The Connection with connection_id: `{connection_id}` was not found")

    session.delete(connection)


@connections_router.get(
    "/connections/{connection_id}",
    status_code=200,
    responses=create_openapi_http_exception_doc([401, 403, 404]),
)
async def get_connection(
    connection_id: str,
    session: Annotated[Session, Depends(get_session)],
) -> ConnectionResponse:
    """Get a connection entry."""
    connection = session.scalar(select(Connection).filter_by(conn_id=connection_id))

    if connection is None:
        raise HTTPException(404, f"The Connection with connection_id: `{connection_id}` was not found")

    return ConnectionResponse.model_validate(connection, from_attributes=True)
