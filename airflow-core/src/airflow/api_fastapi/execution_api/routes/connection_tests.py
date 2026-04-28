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

from uuid import UUID

from cadwyn import VersionedAPIRouter
from fastapi import HTTPException, Response, Security, status

from airflow.api_fastapi.auth.tokens import JWTGenerator
from airflow.api_fastapi.common.db.common import SessionDep
from airflow.api_fastapi.execution_api.datamodels.connection_test import (
    ConnectionTestConnectionResponse,
    ConnectionTestResultBody,
)
from airflow.api_fastapi.execution_api.datamodels.token import TIToken
from airflow.api_fastapi.execution_api.deps import DepContainer
from airflow.api_fastapi.execution_api.security import CurrentTIToken, ExecutionAPIRoute, require_auth
from airflow.models.connection_test import (
    TERMINAL_STATES,
    ConnectionTestRequest,
    ConnectionTestState,
)

router = VersionedAPIRouter()

ct_id_router = VersionedAPIRouter(
    route_class=ExecutionAPIRoute,
    dependencies=[
        Security(require_auth, scopes=["ct:self"]),
    ],
)


@ct_id_router.get(
    "/{connection_test_id}/connection",
    responses={
        status.HTTP_404_NOT_FOUND: {"description": "Connection test not found"},
    },
)
def get_connection_test_connection(
    connection_test_id: UUID,
    session: SessionDep,
) -> ConnectionTestConnectionResponse:
    """Return the connection data stored in a test request (called by workers)."""
    ct = session.get(ConnectionTestRequest, connection_test_id)
    if ct is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail={
                "reason": "not_found",
                "message": f"Connection test {connection_test_id} not found",
            },
        )

    return ConnectionTestConnectionResponse(
        conn_id=ct.connection_id,
        conn_type=ct.conn_type,
        host=ct.host,
        login=ct.login,
        password=ct.password,
        schema=ct.schema,
        port=ct.port,
        extra=ct.extra,
    )


@ct_id_router.patch(
    "/{connection_test_id}",
    status_code=status.HTTP_204_NO_CONTENT,
    dependencies=[Security(require_auth, scopes=["token:execution", "token:workload"])],
    responses={
        status.HTTP_404_NOT_FOUND: {"description": "Connection test not found"},
        status.HTTP_409_CONFLICT: {"description": "Connection test already in a terminal state"},
    },
)
def patch_connection_test(
    connection_test_id: UUID,
    body: ConnectionTestResultBody,
    response: Response,
    session: SessionDep,
    services=DepContainer,
    token: TIToken = CurrentTIToken,
) -> None:
    """Update the result of a connection test (called by workers)."""
    ct = session.get(ConnectionTestRequest, connection_test_id, with_for_update=True)
    if ct is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail={
                "reason": "not_found",
                "message": f"Connection test {connection_test_id} not found",
            },
        )

    if ct.state in TERMINAL_STATES:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail={
                "reason": "conflict",
                "message": f"Connection test {connection_test_id} is already in terminal state: {ct.state}",
            },
        )

    ct.state = body.state
    ct.result_message = body.result_message

    if body.state == ConnectionTestState.SUCCESS and ct.commit_on_success:
        ct.commit_to_connection_table(session=session)

    # JWTReissueMiddleware also writes Refreshed-API-Token but skips workload tokens, so we set it here for the workload→execution swap.
    if token.claims.scope == "workload":
        generator: JWTGenerator = services.get(JWTGenerator)
        execution_token = generator.generate(extras={"sub": str(connection_test_id), "scope": "execution"})
        response.headers["Refreshed-API-Token"] = execution_token


router.include_router(ct_id_router)
