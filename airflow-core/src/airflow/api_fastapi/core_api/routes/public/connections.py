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

import os
from typing import Annotated

from fastapi import Depends, HTTPException, Query, status
from fastapi.exceptions import RequestValidationError
from pydantic import ValidationError
from sqlalchemy import select

from airflow.api_fastapi.common.db.common import SessionDep, paginated_select
from airflow.api_fastapi.common.parameters import (
    QueryConnectionIdPatternSearch,
    QueryLimit,
    QueryOffset,
    SortParam,
)
from airflow.api_fastapi.common.router import AirflowRouter
from airflow.api_fastapi.core_api.datamodels.common import (
    BulkBody,
    BulkResponse,
)
from airflow.api_fastapi.core_api.datamodels.connections import (
    ConnectionBody,
    ConnectionCollectionResponse,
    ConnectionResponse,
    ConnectionTestResponse,
)
from airflow.api_fastapi.core_api.openapi.exceptions import create_openapi_http_exception_doc
from airflow.api_fastapi.core_api.security import (
    ReadableConnectionsFilterDep,
    requires_access_connection,
    requires_access_connection_bulk,
)
from airflow.api_fastapi.core_api.services.public.connections import (
    BulkConnectionService,
    update_orm_from_pydantic,
)
from airflow.api_fastapi.logging.decorators import action_logging
from airflow.configuration import conf
from airflow.models import Connection
from airflow.secrets.environment_variables import CONN_ENV_PREFIX
from airflow.utils.db import create_default_connections as db_create_default_connections
from airflow.utils.strings import get_random_string

connections_router = AirflowRouter(tags=["Connection"], prefix="/connections")


@connections_router.delete(
    "/{connection_id}",
    status_code=status.HTTP_204_NO_CONTENT,
    responses=create_openapi_http_exception_doc([status.HTTP_404_NOT_FOUND]),
    dependencies=[Depends(requires_access_connection(method="DELETE")), Depends(action_logging())],
)
def delete_connection(
    connection_id: str,
    session: SessionDep,
):
    """Delete a connection entry."""
    connection = session.scalar(select(Connection).filter_by(conn_id=connection_id))

    if connection is None:
        raise HTTPException(
            status.HTTP_404_NOT_FOUND, f"The Connection with connection_id: `{connection_id}` was not found"
        )

    session.delete(connection)


@connections_router.get(
    "/{connection_id}",
    responses=create_openapi_http_exception_doc([status.HTTP_404_NOT_FOUND]),
    dependencies=[Depends(requires_access_connection(method="GET"))],
)
def get_connection(
    connection_id: str,
    session: SessionDep,
) -> ConnectionResponse:
    """Get a connection entry."""
    connection = session.scalar(select(Connection).filter_by(conn_id=connection_id))

    if connection is None:
        raise HTTPException(
            status.HTTP_404_NOT_FOUND, f"The Connection with connection_id: `{connection_id}` was not found"
        )

    return connection


@connections_router.get(
    "",
    responses=create_openapi_http_exception_doc([status.HTTP_404_NOT_FOUND]),
    dependencies=[Depends(requires_access_connection(method="GET"))],
)
def get_connections(
    limit: QueryLimit,
    offset: QueryOffset,
    order_by: Annotated[
        SortParam,
        Depends(
            SortParam(
                ["conn_id", "conn_type", "description", "host", "port", "id"],
                Connection,
                {"connection_id": "conn_id"},
            ).dynamic_depends()
        ),
    ],
    readable_connections_filter: ReadableConnectionsFilterDep,
    session: SessionDep,
    connection_id_pattern: QueryConnectionIdPatternSearch,
) -> ConnectionCollectionResponse:
    """Get all connection entries."""
    connection_select, total_entries = paginated_select(
        statement=select(Connection),
        filters=[connection_id_pattern, readable_connections_filter],
        order_by=order_by,
        offset=offset,
        limit=limit,
        session=session,
    )

    connections = session.scalars(connection_select)

    return ConnectionCollectionResponse(
        connections=connections,
        total_entries=total_entries,
    )


@connections_router.post(
    "",
    status_code=status.HTTP_201_CREATED,
    responses=create_openapi_http_exception_doc(
        [status.HTTP_409_CONFLICT]
    ),  # handled by global exception handler
    dependencies=[Depends(requires_access_connection(method="POST")), Depends(action_logging())],
)
def post_connection(
    post_body: ConnectionBody,
    session: SessionDep,
) -> ConnectionResponse:
    """Create connection entry."""
    connection = Connection(**post_body.model_dump(by_alias=True))
    session.add(connection)
    return connection


@connections_router.patch(
    "", dependencies=[Depends(requires_access_connection_bulk()), Depends(action_logging())]
)
def bulk_connections(
    request: BulkBody[ConnectionBody],
    session: SessionDep,
) -> BulkResponse:
    """Bulk create, update, and delete connections."""
    return BulkConnectionService(session=session, request=request).handle_request()


@connections_router.patch(
    "/{connection_id}",
    responses=create_openapi_http_exception_doc(
        [
            status.HTTP_400_BAD_REQUEST,
            status.HTTP_404_NOT_FOUND,
        ]
    ),
    dependencies=[Depends(requires_access_connection(method="PUT")), Depends(action_logging())],
)
def patch_connection(
    connection_id: str,
    patch_body: ConnectionBody,
    session: SessionDep,
    update_mask: list[str] | None = Query(None),
) -> ConnectionResponse:
    """Update a connection entry."""
    if patch_body.connection_id != connection_id:
        raise HTTPException(
            status.HTTP_400_BAD_REQUEST,
            "The connection_id in the request body does not match the URL parameter",
        )

    connection = session.scalar(select(Connection).filter_by(conn_id=connection_id).limit(1))

    if connection is None:
        raise HTTPException(
            status.HTTP_404_NOT_FOUND, f"The Connection with connection_id: `{connection_id}` was not found"
        )

    try:
        ConnectionBody(**patch_body.model_dump())
    except ValidationError as e:
        raise RequestValidationError(errors=e.errors())

    update_orm_from_pydantic(connection, patch_body, update_mask)
    return connection


@connections_router.post("/test", dependencies=[Depends(requires_access_connection(method="POST"))])
def test_connection(
    test_body: ConnectionBody,
    session: SessionDep,
) -> ConnectionTestResponse:
    """
    Test an API connection.

    This method first creates an in-memory transient conn_id & exports that to an env var,
    as some hook classes tries to find out the `conn` from their __init__ method & errors out if not found.
    It also deletes the conn id env connection after the test.
    """
    if conf.get("core", "test_connection", fallback="Disabled").lower().strip() != "enabled":
        raise HTTPException(
            status.HTTP_403_FORBIDDEN,
            "Testing connections is disabled in Airflow configuration. "
            "Contact your deployment admin to enable it.",
        )

    conn_env_var: str | None = None
    try:
        conn = None
        if test_body.connection_id:
            conn = session.scalar(select(Connection).filter_by(conn_id=test_body.connection_id))

        if conn is None:
            effective_conn_id = get_random_string()
            data = test_body.model_dump(by_alias=True)
            data["conn_id"] = effective_conn_id
            conn = Connection(**data)
        else:
            effective_conn_id = conn.conn_id

        conn_env_var = f"{CONN_ENV_PREFIX}{effective_conn_id.upper()}"
        os.environ[conn_env_var] = conn.get_uri()
        test_status, test_message = conn.test_connection()
        return ConnectionTestResponse.model_validate({"status": test_status, "message": test_message})
    finally:
        if conn_env_var:
            os.environ.pop(conn_env_var, None)


@connections_router.post(
    "/defaults",
    status_code=status.HTTP_204_NO_CONTENT,
    dependencies=[Depends(requires_access_connection(method="POST")), Depends(action_logging())],
)
def create_default_connections(
    session: SessionDep,
):
    """Create default connections."""
    db_create_default_connections(session)
