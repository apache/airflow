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

import uuid
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
from airflow.api_fastapi.core_api.datamodels.connection_test import (
    ConnectionTestQueuedResponse,
    ConnectionTestStatusResponse,
)
from airflow.api_fastapi.core_api.datamodels.connections import (
    ConnectionBody,
    ConnectionCollectionResponse,
    ConnectionResponse,
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
from airflow.exceptions import AirflowNotFoundException
from airflow.models import Connection
from airflow.models.connection_test import ConnectionTestRequest
from airflow.models.crypto import get_fernet
from airflow.utils.db import create_default_connections as db_create_default_connections

connections_router = AirflowRouter(tags=["Connection"], prefix="/connections")


# NOTE: /test routes must be defined BEFORE /{connection_id} routes to avoid route conflicts
@connections_router.post(
    "/test",
    dependencies=[Depends(requires_access_connection(method="POST")), Depends(action_logging())],
    responses=create_openapi_http_exception_doc([status.HTTP_403_FORBIDDEN]),
)
def test_connection(
    test_body: ConnectionBody,
    session: SessionDep,
) -> ConnectionTestQueuedResponse:
    """
    Queue a connection test for asynchronous execution on a worker.

    This endpoint queues the connection test request for execution on a worker node,
    which provides better security isolation (workers run in ephemeral environments)
    and network accessibility (workers can reach external systems that API servers cannot).

    Returns a request_id that can be used to poll for the test result via GET /connections/test/{request_id}.
    """
    if conf.get("core", "test_connection", fallback="Disabled").lower().strip() != "enabled":
        raise HTTPException(
            status.HTTP_403_FORBIDDEN,
            "Testing connections is disabled in Airflow configuration. "
            "Contact your deployment admin to enable it.",
        )

    try:
        conn = Connection.get_connection_from_secrets(test_body.connection_id)
        update_orm_from_pydantic(conn, test_body)
    except AirflowNotFoundException:
        conn = Connection(**test_body.model_dump(by_alias=True))

    # Encrypt the connection URI for secure storage
    fernet = get_fernet()
    connection_uri = conn.get_uri()
    encrypted_uri = fernet.encrypt(connection_uri.encode("utf-8")).decode("utf-8")

    # Create the test request
    test_request = ConnectionTestRequest.create_request(
        encrypted_connection_uri=encrypted_uri,
        conn_type=test_body.conn_type,
        session=session,
    )
    session.commit()

    return ConnectionTestQueuedResponse(
        request_id=str(test_request.id),
        state=test_request.state,
        message="Connection test request queued for execution on a worker.",
    )


@connections_router.get(
    "/test/{request_id}",
    dependencies=[Depends(requires_access_connection(method="GET"))],
    responses=create_openapi_http_exception_doc([status.HTTP_404_NOT_FOUND]),
)
def get_connection_test_status(
    request_id: str,
    session: SessionDep,
) -> ConnectionTestStatusResponse:
    """
    Get the status of a connection test request.

    Poll this endpoint to check if a connection test has completed and retrieve the result.
    """
    # Validate that request_id is a valid UUID format
    try:
        uuid.UUID(request_id)
    except (ValueError, TypeError):
        raise HTTPException(
            status.HTTP_404_NOT_FOUND,
            f"Connection test request with id `{request_id}` was not found.",
        )

    test_request = session.scalar(select(ConnectionTestRequest).where(ConnectionTestRequest.id == request_id))

    if test_request is None:
        raise HTTPException(
            status.HTTP_404_NOT_FOUND,
            f"Connection test request with id `{request_id}` was not found.",
        )

    return ConnectionTestStatusResponse(
        request_id=str(test_request.id),
        state=test_request.state,
        result_status=test_request.result_status,
        result_message=test_request.result_message,
        created_at=test_request.created_at,
        started_at=test_request.started_at,
        completed_at=test_request.completed_at,
    )


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
