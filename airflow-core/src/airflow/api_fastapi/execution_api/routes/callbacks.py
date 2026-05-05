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
from uuid import UUID

import structlog
from cadwyn import VersionedAPIRouter
from fastapi import Body, HTTPException, Response, Security, status
from structlog.contextvars import bind_contextvars

from airflow.api_fastapi.auth.tokens import JWTGenerator
from airflow.api_fastapi.common.db.common import SessionDep
from airflow.api_fastapi.execution_api.datamodels.callback import CallbackTerminalStatePayload
from airflow.api_fastapi.execution_api.datamodels.token import TIToken
from airflow.api_fastapi.execution_api.deps import DepContainer
from airflow.api_fastapi.execution_api.security import CurrentTIToken, ExecutionAPIRoute, require_auth
from airflow.models.callback import Callback
from airflow.utils.state import CallbackState

log = structlog.get_logger(__name__)

router = VersionedAPIRouter(route_class=ExecutionAPIRoute)


def _require_self(token: TIToken, callback_id: UUID) -> None:
    """Mirror the ``ti:self`` enforcement from security.py for callback routes."""
    if str(token.id) != str(callback_id):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Token subject does not match callback id",
        )


@router.post(
    "/{callback_id}/run",
    status_code=status.HTTP_204_NO_CONTENT,
    dependencies=[Security(require_auth, scopes=["token:execution", "token:workload"])],
    responses={
        status.HTTP_403_FORBIDDEN: {"description": "Token subject does not match callback id"},
        status.HTTP_404_NOT_FOUND: {"description": "Callback not found"},
        status.HTTP_409_CONFLICT: {"description": "Callback is not in a state that can be marked running"},
    },
)
def callback_run(
    callback_id: UUID,
    response: Response,
    session: SessionDep,
    services=DepContainer,
    token: TIToken = CurrentTIToken,
) -> Response:
    """
    Mark a callback as RUNNING.

    Mirrors ``PATCH /task-instances/{id}/run``: this is the single endpoint that
    accepts a workload-scoped token and atomically (a) transitions the callback
    from QUEUED to RUNNING and (b) issues a fresh execution-scoped token via the
    ``Refreshed-API-Token`` response header. All subsequent supervisor calls hit
    execution-only routes.
    """
    bind_contextvars(callback_id=str(callback_id))
    _require_self(token, callback_id)

    callback = session.get(Callback, callback_id)
    if callback is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail={"reason": "not_found", "message": "Callback not found"},
        )

    # Allow QUEUED → RUNNING transition; treat RUNNING as idempotent so a retried
    # supervisor start does not 409. Anything else (PENDING / SCHEDULED / terminal) rejects.
    if callback.state == CallbackState.RUNNING:
        log.info("Duplicate start request received from %s", callback.id)
    elif callback.state == CallbackState.QUEUED:
        callback.state = CallbackState.RUNNING
        session.add(callback)
    else:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail={
                "reason": "invalid_state",
                "message": "Callback was not in a state where it could be marked running",
                "current_state": callback.state,
            },
        )

    if token.claims.scope == "workload":
        generator: JWTGenerator = services.get(JWTGenerator)
        execution_token = generator.generate(extras={"sub": str(callback_id), "scope": "execution"})
        response.headers["Refreshed-API-Token"] = execution_token

    response.status_code = status.HTTP_204_NO_CONTENT
    return response


@router.patch(
    "/{callback_id}/state",
    status_code=status.HTTP_204_NO_CONTENT,
    responses={
        status.HTTP_403_FORBIDDEN: {"description": "Token subject does not match callback id"},
        status.HTTP_404_NOT_FOUND: {"description": "Callback not found"},
        status.HTTP_409_CONFLICT: {"description": "Callback is not in RUNNING state"},
    },
)
def callback_update_state(
    callback_id: UUID,
    payload: Annotated[CallbackTerminalStatePayload, Body()],
    session: SessionDep,
    token: TIToken = CurrentTIToken,
) -> Response:
    """Mark a RUNNING callback as SUCCESS or FAILED."""
    bind_contextvars(callback_id=str(callback_id))
    _require_self(token, callback_id)

    callback = session.get(Callback, callback_id)
    if callback is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail={"reason": "not_found", "message": "Callback not found"},
        )

    if callback.state != CallbackState.RUNNING:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail={
                "reason": "invalid_state",
                "message": "Callback was not in RUNNING state",
                "current_state": callback.state,
            },
        )

    callback.state = CallbackState(payload.state)
    if payload.output is not None:
        callback.output = payload.output
    session.add(callback)

    return Response(status_code=status.HTTP_204_NO_CONTENT)
