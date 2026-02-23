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

from fastapi import APIRouter, HTTPException, status

from airflow.api_fastapi.common.db.common import SessionDep
from airflow.api_fastapi.execution_api.datamodels.connection_test import ConnectionTestResultBody
from airflow.models.connection_test import TERMINAL_STATES, ConnectionTest

router = APIRouter()


@router.patch(
    "/{connection_test_id}",
    status_code=status.HTTP_204_NO_CONTENT,
    responses={
        status.HTTP_404_NOT_FOUND: {"description": "Connection test not found"},
        status.HTTP_409_CONFLICT: {"description": "Connection test already in a terminal state"},
    },
)
def patch_connection_test(
    connection_test_id: UUID,
    body: ConnectionTestResultBody,
    session: SessionDep,
) -> None:
    """Update the result of a connection test (called by workers)."""
    ct = session.get(ConnectionTest, connection_test_id, with_for_update=True)
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
