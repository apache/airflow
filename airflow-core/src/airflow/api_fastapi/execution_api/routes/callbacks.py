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
from fastapi import HTTPException, Security, status

from airflow.api_fastapi.common.db.common import SessionDep
from airflow.api_fastapi.execution_api.datamodels.callback import CallbackRunResponse
from airflow.api_fastapi.execution_api.security import ExecutionAPIRoute, require_auth
from airflow.models.callback import Callback
from airflow.utils.state import CallbackState

# ``cb:self`` makes the server enforce that the presented workload token was minted for *this*
# callback id (see ``require_auth``). Combined with ``token:workload``, a worker cannot reach this
# route without a validly-signed token whose subject is the callback it is claiming.
router = VersionedAPIRouter(
    route_class=ExecutionAPIRoute,
    dependencies=[
        Security(require_auth, scopes=["cb:self", "token:workload"]),
    ],
)


@router.post(
    "/{callback_id}/run",
    responses={
        status.HTTP_404_NOT_FOUND: {"description": "Callback not found"},
        status.HTTP_409_CONFLICT: {"description": "Callback is not in a claimable state"},
    },
)
def run_callback(
    callback_id: UUID,
    session: SessionDep,
) -> CallbackRunResponse:
    """
    Atomically claim a callback for execution and transition it to RUNNING.

    The worker calls this *before* importing and invoking the callback. Its purpose is twofold:
    the ``Security`` dependency forces the API server to validate the worker's token before any
    callback code runs, and the ``QUEUED -> RUNNING`` transition is single-shot, so a redelivered
    or replayed message that reaches a callback already RUNNING or terminal is refused rather than
    executed a second time.
    """
    callback = session.get(Callback, callback_id, with_for_update=True)
    if callback is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail={"reason": "not_found", "message": f"Callback {callback_id} not found"},
        )

    if callback.state != CallbackState.QUEUED:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail={
                "reason": "conflict",
                "message": (
                    f"Callback {callback_id} is in state {callback.state}; it can only be claimed "
                    "while QUEUED."
                ),
            },
        )

    callback.state = CallbackState.RUNNING

    return CallbackRunResponse(id=callback.id, state=CallbackState.RUNNING)
