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

from airflow.api_fastapi.common.db.common import SessionDep
from airflow.api_fastapi.core_api.openapi.exceptions import create_openapi_http_exception_doc
from airflow.api_fastapi.execution_api.deps import DepContainer
from airflow.api_fastapi.execution_api.security import (
    ExecutionAPIRoute,
    issue_execution_token,
    require_auth,
)
from airflow.models.callback import Callback
from airflow.utils.state import CallbackState

router = VersionedAPIRouter(
    route_class=ExecutionAPIRoute,
    dependencies=[
        Security(require_auth, scopes=["cb:self", "token:callback"]),
    ],
)


@router.patch(
    "/{callback_id}/run",
    status_code=status.HTTP_204_NO_CONTENT,
    responses=create_openapi_http_exception_doc(
        [
            (status.HTTP_404_NOT_FOUND, "Callback not found"),
            (status.HTTP_409_CONFLICT, "The callback token was already exchanged"),
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
    callback = session.get(Callback, callback_id, with_for_update=True)
    if callback is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail={
                "reason": "not_found",
                "message": f"Callback {callback_id} not found",
            },
        )

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

    callback.state = CallbackState.RUNNING

    issue_execution_token(services, response, sub=str(callback_id))
