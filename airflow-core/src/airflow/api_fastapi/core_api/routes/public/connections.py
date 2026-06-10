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

from fastapi import Depends, Header, HTTPException, Query, status
from fastapi.exceptions import RequestValidationError
from pydantic import ValidationError
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError

from airflow.api_fastapi.app import get_auth_manager
from airflow.api_fastapi.auth.managers.models.resource_details import ConnectionDetails
from airflow.api_fastapi.common.db.common import SessionDep, paginated_select
from airflow.api_fastapi.common.parameters import (
    QueryConnectionIdPatternSearch,
    QueryConnectionIdPrefixPatternSearch,
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
    AsyncConnectionTestResponse,
    ConnectionBody,
    ConnectionBodyPartial,
    ConnectionCollectionResponse,
    ConnectionResponse,
    ConnectionTestQueuedResponse,
    ConnectionTestRequestBody,
    ConnectionTestResponse,
)
from airflow.api_fastapi.core_api.openapi.exceptions import create_openapi_http_exception_doc
from airflow.api_fastapi.core_api.security import (
    GetUserDep,
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
from airflow.executors.executor_loader import ExecutorLoader
from airflow.models import Connection
from airflow.models.connection_test import ConnectionTestRequest
from airflow.secrets.environment_variables import CONN_ENV_PREFIX
from airflow.utils.db import create_default_connections as db_create_default_connections
from airflow.utils.strings import get_random_string

connections_router = AirflowRouter(tags=["Connection"], prefix="/connections")


def _ensure_test_connection_enabled() -> None:
    """Raise 403 if connection testing is not enabled in the Airflow configuration."""
    if conf.get("core", "test_connection", fallback="Disabled").lower().strip() != "enabled":
        raise HTTPException(
            status.HTTP_403_FORBIDDEN,
            "Testing connections is disabled in Airflow configuration. "
            "Contact your deployment admin to enable it.",
        )


def _ensure_executor_is_configured(executor: str | None) -> None:
    """Raise 422 if the requested executor is not in the configured executors list."""
    if executor is None:
        return
    configured = ExecutorLoader.get_executor_names(validate_teams=False)
    if not any(
        executor in (name.alias, name.module_path, name.module_path.split(".")[-1]) for name in configured
    ):
        raise HTTPException(
            status.HTTP_422_UNPROCESSABLE_ENTITY,
            f"Executor '{executor}' is not configured. "
            f"Configured executors: {[name.alias or name.module_path for name in configured]}",
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
    "/enqueue-test",
    responses=create_openapi_http_exception_doc([status.HTTP_404_NOT_FOUND]),
)
def get_connection_test(
    session: SessionDep,
    user: GetUserDep,
    connection_test_token: Annotated[str, Header(alias="Airflow-Connection-Test-Token")],
) -> AsyncConnectionTestResponse:
    """Poll for the status of an enqueued connection test by its token (passed as a header)."""
    connection_test = session.scalar(select(ConnectionTestRequest).filter_by(token=connection_test_token))

    if connection_test is None:
        raise HTTPException(
            status.HTTP_404_NOT_FOUND,
            f"No connection test found for token: `{connection_test_token}`",
        )

    if not get_auth_manager().is_authorized_connection(
        method="GET",
        details=ConnectionDetails(conn_id=connection_test.connection_id, team_name=connection_test.team_name),
        user=user,
    ):
        raise HTTPException(
            status.HTTP_404_NOT_FOUND,
            f"No connection test found for token: `{connection_test_token}`",
        )

    return AsyncConnectionTestResponse(
        token=connection_test.token,
        connection_id=connection_test.connection_id,
        state=connection_test.state,
        result_message=connection_test.result_message,
        created_at=connection_test.created_at,
    )


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
                ["conn_id", "conn_type", "description", "host", "port", "id", "team_name"],
                Connection,
                {"connection_id": "conn_id"},
            ).dynamic_depends()
        ),
    ],
    readable_connections_filter: ReadableConnectionsFilterDep,
    session: SessionDep,
    connection_id_pattern: QueryConnectionIdPatternSearch,
    connection_id_prefix_pattern: QueryConnectionIdPrefixPatternSearch,
) -> ConnectionCollectionResponse:
    """Get all connection entries."""
    connection_select, total_entries = paginated_select(
        statement=select(Connection),
        filters=[connection_id_pattern, connection_id_prefix_pattern, readable_connections_filter],
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

    if update_mask:
        fields_to_update = patch_body.model_fields_set & set(update_mask)
        try:
            ConnectionBodyPartial(**patch_body.model_dump(include=fields_to_update))
        except ValidationError as e:
            raise RequestValidationError(errors=e.errors())

    update_orm_from_pydantic(connection, patch_body, update_mask)
    return connection


@connections_router.post("/test", dependencies=[Depends(requires_access_connection(method="POST"))])
def test_connection(test_body: ConnectionBody) -> ConnectionTestResponse:
    """
    Test an API connection.

    This method first creates an in-memory transient conn_id & exports that to an env var,
    as some hook classes tries to find out the `conn` from their __init__ method & errors out if not found.
    It also deletes the conn id env connection after the test.
    """
    _ensure_test_connection_enabled()

    transient_conn_id = get_random_string()
    conn_env_var = f"{CONN_ENV_PREFIX}{transient_conn_id.upper()}"
    try:
        # Try to get existing connection and merge with provided values
        try:
            existing_conn = Connection.get_connection_from_secrets(test_body.connection_id)
            existing_conn.conn_id = transient_conn_id
            update_orm_from_pydantic(existing_conn, test_body)
            conn = existing_conn
        except AirflowNotFoundException:
            data = test_body.model_dump(by_alias=True)
            data["conn_id"] = transient_conn_id
            conn = Connection(**data)

        os.environ[conn_env_var] = conn.get_uri()
        test_status, test_message = conn.test_connection()
        return ConnectionTestResponse.model_validate({"status": test_status, "message": test_message})
    finally:
        os.environ.pop(conn_env_var, None)


@connections_router.post(
    "/enqueue-test",
    status_code=status.HTTP_202_ACCEPTED,
    responses=create_openapi_http_exception_doc(
        [
            status.HTTP_403_FORBIDDEN,
            status.HTTP_409_CONFLICT,
            status.HTTP_422_UNPROCESSABLE_ENTITY,
        ]
    ),
    dependencies=[Depends(action_logging())],
)
def enqueue_connection_test(
    test_body: ConnectionTestRequestBody,
    session: SessionDep,
    user: GetUserDep,
) -> ConnectionTestQueuedResponse:
    """Enqueue a connection test for deferred execution on a worker; returns a polling token."""
    _ensure_test_connection_enabled()
    _ensure_executor_is_configured(test_body.executor)

    existing = session.scalar(select(Connection).filter_by(conn_id=test_body.connection_id))
    if existing is not None:
        effective_team = existing.team_name
        if test_body.team_name is not None and test_body.team_name != effective_team:
            raise HTTPException(
                status.HTTP_403_FORBIDDEN,
                f"team_name `{test_body.team_name}` does not match the team of connection "
                f"`{test_body.connection_id}`.",
            )
    else:
        effective_team = test_body.team_name

    if not get_auth_manager().is_authorized_connection(
        method="POST",
        details=ConnectionDetails(conn_id=test_body.connection_id, team_name=effective_team),
        user=user,
    ):
        raise HTTPException(
            status.HTTP_403_FORBIDDEN,
            f"You are not authorized to test connection `{test_body.connection_id}`.",
        )

    connection_test = ConnectionTestRequest(
        connection_id=test_body.connection_id,
        conn_type=test_body.conn_type,
        host=test_body.host,
        login=test_body.login,
        password=test_body.password,
        schema=test_body.schema_,
        port=test_body.port,
        extra=test_body.extra,
        commit_on_success=test_body.commit_on_success,
        executor=test_body.executor,
        queue=test_body.queue,
        team_name=effective_team,
    )
    session.add(connection_test)
    try:
        session.flush()
    except IntegrityError:
        raise HTTPException(
            status.HTTP_409_CONFLICT,
            f"An active connection test already exists for connection_id `{test_body.connection_id}`.",
        )

    return ConnectionTestQueuedResponse(
        token=connection_test.token,
        connection_id=connection_test.connection_id,
        state=connection_test.state,
    )


@connections_router.post(
    "/defaults",
    status_code=status.HTTP_204_NO_CONTENT,
    dependencies=[Depends(requires_access_connection(method="POST")), Depends(action_logging())],
)
def create_default_connections(
    session: SessionDep,
):
    """Create default connections."""
    db_create_default_connections(session=session)
