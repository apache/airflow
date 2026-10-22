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
from sqlalchemy import select

from airflow._shared.timezones import timezone
from airflow.api_fastapi.common.db.common import SessionDep
from airflow.api_fastapi.core_api.openapi.exceptions import create_openapi_http_exception_doc
from airflow.api_fastapi.execution_api.datamodels.callback import (
    CallbackContextResponse,
    CallbackTerminalStatePayload,
    DagCallbackContext,
    EmailCallbackContext,
    TaskCallbackContext,
)
from airflow.api_fastapi.execution_api.deps import DepContainer
from airflow.api_fastapi.execution_api.security import (
    ExecutionAPIRoute,
    issue_execution_token,
    require_auth,
)
from airflow.callbacks.callback_requests import DagCallbackRequest, EmailRequest, TaskCallbackRequest
from airflow.models.callback import Callback
from airflow.models.dagrun import DagRun
from airflow.utils.helpers import prune_dict
from airflow.utils.state import CallbackState

router = VersionedAPIRouter(
    route_class=ExecutionAPIRoute,
    dependencies=[
        Security(require_auth, scopes=["cb:self"]),
    ],
)


def _get_callback(callback_id: UUID, session: SessionDep, *, for_update: bool = False) -> Callback:
    callback = session.get(Callback, callback_id, with_for_update=for_update)
    if callback is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail={
                "reason": "not_found",
                "message": f"Callback {callback_id} not found",
            },
        )
    return callback


def _get_identity_claims(callback: Callback, session: SessionDep) -> dict[str, str]:
    """Return the Dag, run, and bundle a callback runs for, which resolve its team."""
    run_id = None
    if dag_run_id := callback.data.get("dag_run_id"):
        run_id = session.scalar(select(DagRun.run_id).where(DagRun.id == dag_run_id))
    return prune_dict(
        {"dag_id": callback.get_dag_id(), "run_id": run_id, "bundle_name": callback.bundle_name},
    )


@router.patch(
    "/{callback_id}/run",
    status_code=status.HTTP_204_NO_CONTENT,
    dependencies=[Security(require_auth, scopes=["token:callback"])],
    responses=create_openapi_http_exception_doc(
        [
            (status.HTTP_404_NOT_FOUND, "Callback not found"),
            (
                status.HTTP_409_CONFLICT,
                "The callback token was already exchanged, or the callback's start deadline passed",
            ),
        ]
    ),
)
def run_callback(
    callback_id: UUID,
    response: Response,
    session: SessionDep,
    services=DepContainer,
) -> None:
    """Exchange a single-use callback token for a short-lived execution token."""
    callback = _get_callback(callback_id, session, for_update=True)

    if callback.state != CallbackState.QUEUED:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail={
                "reason": "invalid_state",
                "message": (
                    f"Callback {callback_id} is in state {callback.state}; its token can only be "
                    "exchanged once while QUEUED."
                ),
                "previous_state": callback.state,
            },
        )

    now = timezone.utcnow()
    if (start_deadline := callback.start_deadline) is not None and now >= start_deadline:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail={
                "reason": "start_deadline_passed",
                "message": f"Callback {callback_id} had to start by {start_deadline.isoformat()}",
            },
        )

    callback.state = CallbackState.RUNNING
    callback.mark_started(now)

    issue_execution_token(
        services, response, sub=str(callback_id), claims=_get_identity_claims(callback, session)
    )


@router.get(
    "/{callback_id}/context",
    dependencies=[Security(require_auth, scopes=["token:execution"])],
    responses=create_openapi_http_exception_doc(
        [
            (status.HTTP_404_NOT_FOUND, "Callback not found, or it has no Dag-attribute context"),
            (status.HTTP_409_CONFLICT, "The callback is not running"),
        ]
    ),
)
def get_callback_context(callback_id: UUID, session: SessionDep) -> CallbackContextResponse:
    """Return the context of the Dag success, failure, task, or email callback this token runs."""
    callback = _get_callback(callback_id, session)

    if (request := callback.get_callback_request()) is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail={
                "reason": "no_context",
                "message": f"Callback {callback_id} does not run a Dag or task attribute",
            },
        )
    if callback.state != CallbackState.RUNNING:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail={
                "reason": "invalid_state",
                "message": f"Callback {callback_id} is in state {callback.state}, not running",
                "previous_state": callback.state,
            },
        )

    context: DagCallbackContext | TaskCallbackContext | EmailCallbackContext
    if isinstance(request, DagCallbackRequest):
        run_context = request.context_from_server
        context = DagCallbackContext(
            type="DagCallback",
            dag_id=request.dag_id,
            run_id=request.run_id,
            is_failure_callback=bool(request.is_failure_callback),
            msg=request.msg,
            dag_run=run_context.dag_run if run_context else None,
            last_ti=run_context.last_ti if run_context else None,
        )
    elif isinstance(request, TaskCallbackRequest):
        context = TaskCallbackContext(
            type="TaskCallback",
            ti=request.ti,
            task_callback_type=request.task_callback_type,
            msg=request.msg,
            ti_context=request.context_from_server,
        )
    elif isinstance(request, EmailRequest):
        context = EmailCallbackContext(
            type="EmailCallback",
            ti=request.ti,
            email_type=request.email_type,
            msg=request.msg,
            ti_context=request.context_from_server,
        )
    else:
        raise TypeError(f"Unknown callback request type: {type(request).__name__}")
    return CallbackContextResponse(context=context)


@router.patch(
    "/{callback_id}/state",
    status_code=status.HTTP_204_NO_CONTENT,
    dependencies=[Security(require_auth, scopes=["token:execution"])],
    responses=create_openapi_http_exception_doc(
        [
            (status.HTTP_404_NOT_FOUND, "Callback not found"),
            (status.HTTP_409_CONFLICT, "The callback is not running"),
        ]
    ),
)
def update_callback_state(
    callback_id: UUID,
    body: CallbackTerminalStatePayload,
    session: SessionDep,
) -> None:
    """Record the terminal state of a running callback, which executor events no longer overwrite."""
    callback = _get_callback(callback_id, session, for_update=True)

    if callback.state != CallbackState.RUNNING:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail={
                "reason": "invalid_state",
                "message": f"Callback {callback_id} is in state {callback.state}, not running",
                "previous_state": callback.state,
            },
        )

    callback.state = body.state
    callback.output = body.output
