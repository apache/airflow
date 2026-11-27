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

from datetime import timedelta
from typing import TYPE_CHECKING, Annotated, cast
from uuid import UUID

from cadwyn import VersionedAPIRouter
from fastapi import Body, Header, HTTPException, Response, Security, status
from sqlalchemy import update

from airflow._shared.timezones import timezone
from airflow.api_fastapi.common.db.common import SessionDep
from airflow.api_fastapi.core_api.openapi.exceptions import create_openapi_http_exception_doc
from airflow.api_fastapi.execution_api.datamodels.callback import (
    CallbackContextResponse,
    CallbackRunPayload,
    CallbackTerminalStatePayload,
)
from airflow.api_fastapi.execution_api.datamodels.taskinstance import PrevSuccessfulDagRunResponse
from airflow.api_fastapi.execution_api.deps import DepContainer
from airflow.api_fastapi.execution_api.security import (
    ExecutionAPIRoute,
    issue_execution_token,
    require_auth,
)
from airflow.configuration import conf
from airflow.executors.workloads.callback import (
    EXECUTION_TIMEOUT_KEY,
    HEARTBEAT_SUPPORTED_KEY,
    CallbackFetchMethod,
    get_callback_report_window,
)
from airflow.models.callback import Callback
from airflow.models.dagrun import DagRun
from airflow.utils.helpers import prune_dict
from airflow.utils.state import CallbackState, DagRunState

if TYPE_CHECKING:
    from sqlalchemy import CursorResult

router = VersionedAPIRouter(
    route_class=ExecutionAPIRoute,
    dependencies=[
        Security(require_auth, scopes=["cb:self"]),
    ],
)


def _get_callback(callback_id: UUID, *, session: SessionDep, for_update: bool = False) -> Callback:
    callback = session.get(Callback, callback_id, with_for_update=for_update, populate_existing=for_update)
    if callback is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail={
                "reason": "not_found",
                "message": f"Callback {callback_id} not found",
            },
        )
    return callback


def _get_identity_claims(callback: Callback, *, session: SessionDep) -> dict[str, str]:
    """Return the Dag, run, and bundle a callback runs for, which resolve its team."""
    dag_run = callback.get_dag_run(session=session)
    return prune_dict(
        {
            "dag_id": callback.get_dag_id(),
            "run_id": dag_run.run_id if dag_run else None,
            "bundle_name": callback.bundle_name,
        },
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
    body: CallbackRunPayload | None = None,
    prefer: Annotated[str | None, Header()] = None,
) -> None:
    """
    Exchange a single-use callback token for a short-lived execution token.

    An attempt that identifies itself can repeat its own exchange, as when it never got the response,
    and gets a new execution token. Any other exchange of a callback that already started is refused,
    including a second delivery of its workload.

    A deadline worker can request heartbeats with ``Prefer: callback-heartbeat``. When enabled,
    the response returns its worker timeout in seconds in ``Airflow-Callback-Heartbeat-Timeout``.
    """
    callback = _get_callback(callback_id, session=session, for_update=True)
    started_by = body.model_dump(mode="json") if body is not None else None
    heartbeat_timeout = (
        _get_callback_heartbeat_timeout()
        if (
            started_by is not None
            and prefer == "callback-heartbeat"
            and callback.fetch_method == CallbackFetchMethod.IMPORT_PATH
            and (
                callback.heartbeat_supported
                if callback.heartbeat_supported is not None
                else callback.data.get(HEARTBEAT_SUPPORTED_KEY)
            )
        )
        else None
    )
    if not callback.try_start(
        timezone.utcnow(), started_by=started_by, heartbeat_timeout=heartbeat_timeout, session=session
    ):
        callback = _get_callback(callback_id, session=session, for_update=True)
        if callback.state == CallbackState.QUEUED and callback.state_deadline is not None:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail={
                    "reason": "start_deadline_passed",
                    "message": f"Callback {callback_id} had to start by {callback.state_deadline.isoformat()}",
                },
            )
        if callback.state != CallbackState.RUNNING or started_by is None or callback.started_by != started_by:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail={
                    "reason": "invalid_state",
                    "message": (
                        f"Callback {callback_id} is in state {callback.state}; its token can only be "
                        "exchanged once while QUEUED."
                    ),
                    "previous_state": callback.state,
                    "worker_is_monitored": callback.state_deadline is not None,
                },
            )
        # A retry whose first response was lost repeats the original negotiation.
        if callback.fetch_method == CallbackFetchMethod.IMPORT_PATH and callback.state_deadline is not None:
            heartbeat_timeout = _get_callback_heartbeat_timeout()
            _renew_callback_heartbeat(callback_id, heartbeat_timeout, session=session)
        else:
            heartbeat_timeout = None

    if heartbeat_timeout is not None:
        response.headers["Airflow-Callback-Heartbeat-Timeout"] = str(int(heartbeat_timeout.total_seconds()))
    # A routed callback doesn't heartbeat, so its token must last until it reports its outcome.
    timeout = callback.data.get(EXECUTION_TIMEOUT_KEY)
    issue_execution_token(
        services,
        response,
        sub=str(callback.id),
        claims=_get_identity_claims(callback, session=session),
        min_valid_for=get_callback_report_window(timeout) if timeout else None,
    )


def _get_callback_heartbeat_timeout() -> timedelta:
    return timedelta(seconds=max(1, conf.getint("scheduler", "task_instance_heartbeat_timeout")))


@router.put(
    "/{callback_id}/heartbeat",
    status_code=status.HTTP_204_NO_CONTENT,
    dependencies=[Security(require_auth, scopes=["token:execution"])],
    responses=create_openapi_http_exception_doc(
        [
            (status.HTTP_404_NOT_FOUND, "Callback not found"),
            (status.HTTP_409_CONFLICT, "Callback has no running heartbeat to renew"),
        ]
    ),
)
def heartbeat_callback(callback_id: UUID, session: SessionDep) -> None:
    """Renew a deadline callback's worker heartbeat, independently of its execution timeout."""
    _renew_callback_heartbeat(callback_id, _get_callback_heartbeat_timeout(), session=session)


def _renew_callback_heartbeat(callback_id: UUID, timeout: timedelta, *, session: SessionDep) -> None:
    renewed = cast(
        "CursorResult",
        session.execute(
            update(Callback)
            .where(
                Callback.id == callback_id,
                Callback.fetch_method == CallbackFetchMethod.IMPORT_PATH,
                Callback.state == CallbackState.RUNNING,
                Callback.state_deadline.is_not(None),
            )
            .values(state_deadline=timezone.utcnow() + timeout)
        ),
    )
    if not renewed.rowcount:
        _get_callback(callback_id, session=session)
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail={"reason": "invalid_state"})


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
    callback = _get_callback(callback_id, session=session)

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

    return CallbackContextResponse(context=request.build_callback_context())


@router.get(
    "/{callback_id}/previous-successful-dagrun",
    dependencies=[Security(require_auth, scopes=["token:execution"])],
    responses=create_openapi_http_exception_doc([(status.HTTP_404_NOT_FOUND, "Callback not found")]),
)
def get_callback_previous_successful_dagrun(
    callback_id: UUID, session: SessionDep
) -> PrevSuccessfulDagRunResponse:
    """
    Return the last successful run of the callback's Dag before its own Dag run, for its template context.

    A callback's token names the callback, not a task instance, so it can't read this from the
    task-instance endpoint a task uses.
    """
    callback = _get_callback(callback_id, session=session)
    previous = None
    if dag_run := callback.get_dag_run(session=session):
        previous = DagRun.get_previous_dagrun(dag_run, state=DagRunState.SUCCESS, session=session)
    if previous is None:
        return PrevSuccessfulDagRunResponse()
    return PrevSuccessfulDagRunResponse.model_validate(previous)


@router.patch(
    "/{callback_id}/state",
    status_code=status.HTTP_204_NO_CONTENT,
    dependencies=[Security(require_auth, scopes=["token:execution"])],
    responses=create_openapi_http_exception_doc(
        [
            (status.HTTP_404_NOT_FOUND, "Callback not found"),
            (status.HTTP_409_CONFLICT, "The callback is not running and is not already in that state"),
        ]
    ),
)
def update_callback_state(
    callback_id: UUID,
    payload: Annotated[CallbackTerminalStatePayload, Body()],
    session: SessionDep,
) -> None:
    """Report the final state of a callback that has finished running."""
    reported_state = CallbackState(payload.state)
    # A retried report can replace the scheduler's placeholder output for the same outcome.
    stmt = (
        update(Callback)
        .where(Callback.id == callback_id, Callback.state.in_((CallbackState.RUNNING, reported_state)))
        .values(state=reported_state, state_deadline=None)
        .execution_options(synchronize_session=False)
    )
    if payload.output is not None:
        stmt = stmt.values(output=payload.output)
    if cast("CursorResult", session.execute(stmt)).rowcount:
        return
    callback = _get_callback(callback_id, session=session, for_update=True)
    raise HTTPException(
        status_code=status.HTTP_409_CONFLICT,
        detail={
            "reason": "invalid_state",
            "message": (
                f"Callback {callback_id} is in state {callback.state}; a terminal state can only "
                f"be reported for a RUNNING callback, or for one already in {reported_state}."
            ),
            "previous_state": callback.state,
        },
    )
