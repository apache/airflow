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

"""Execution API routes for worker-side connection test execution."""

from __future__ import annotations

from typing import Annotated

import structlog
from cadwyn import VersionedAPIRouter
from fastapi import Body, HTTPException, Query, status

from airflow.api_fastapi.common.db.common import SessionDep
from airflow.api_fastapi.execution_api.datamodels.connection_test import (
    ConnectionTestPendingResponse,
    ConnectionTestResultPayload,
    ConnectionTestRunningPayload,
    ConnectionTestWorkload,
)
from airflow.models.connection_test import ConnectionTestRequest, ConnectionTestState

router = VersionedAPIRouter()

log = structlog.get_logger(__name__)


@router.get(
    "/pending",
    status_code=status.HTTP_200_OK,
)
def get_pending_connection_tests(
    session: SessionDep,
    hostname: Annotated[str, Query(description="Worker hostname requesting work")],
    limit: Annotated[int, Query(description="Maximum number of requests to return", ge=1, le=100)] = 10,
) -> ConnectionTestPendingResponse:
    """
    Get pending connection test requests for worker execution.

    Workers call this endpoint to fetch pending connection tests to execute.
    """
    log.debug("Worker requesting pending connection tests", hostname=hostname, limit=limit)

    pending_requests = ConnectionTestRequest.get_pending_requests(session, limit=limit)

    workloads = []
    for request in pending_requests:
        request.mark_running(hostname)
        workloads.append(
            ConnectionTestWorkload(
                request_id=str(request.id),
                encrypted_connection_uri=request.encrypted_connection_uri,
                conn_type=request.conn_type,
                timeout=request.timeout,
            )
        )

    session.commit()
    return ConnectionTestPendingResponse(requests=workloads)


@router.patch(
    "/{request_id}/state",
    status_code=status.HTTP_204_NO_CONTENT,
    responses={
        status.HTTP_404_NOT_FOUND: {"description": "Connection test request not found"},
        status.HTTP_409_CONFLICT: {"description": "Invalid state transition"},
    },
)
def update_connection_test_state(
    request_id: str,
    payload: Annotated[ConnectionTestRunningPayload | ConnectionTestResultPayload, Body()],
    session: SessionDep,
) -> None:
    """
    Update the state of a connection test request.

    Workers call this endpoint to report the result of a connection test.
    """
    log.debug("Updating connection test state", request_id=request_id, payload=payload)

    test_request = session.get(ConnectionTestRequest, request_id)

    if test_request is None:
        raise HTTPException(
            status.HTTP_404_NOT_FOUND,
            f"Connection test request with id `{request_id}` was not found.",
        )

    if isinstance(payload, ConnectionTestRunningPayload):
        if test_request.state not in (ConnectionTestState.PENDING.value, ConnectionTestState.RUNNING.value):
            raise HTTPException(
                status.HTTP_409_CONFLICT,
                f"Cannot transition from state `{test_request.state}` to `running`.",
            )
        test_request.mark_running(payload.hostname)
    elif isinstance(payload, ConnectionTestResultPayload):
        if test_request.state != ConnectionTestState.RUNNING.value:
            raise HTTPException(
                status.HTTP_409_CONFLICT,
                f"Cannot report result when in state `{test_request.state}`. Expected `running`.",
            )
        if payload.state == "success":
            test_request.mark_success(payload.result_message)
        else:
            test_request.mark_failed(payload.result_message)

    log.info(
        "Connection test state updated",
        request_id=request_id,
        new_state=test_request.state,
    )
