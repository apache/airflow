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

import logging

from fastapi import APIRouter, Depends, HTTPException, Path, status

from airflow.api_fastapi.execution_api.datamodels.connection import ConnectionResponse
from airflow.api_fastapi.execution_api.deps import JWTBearerDep
from airflow.exceptions import AirflowNotFoundException
from airflow.models.connection import Connection


async def has_connection_access(
    connection_id: str = Path(),
    token=JWTBearerDep,
) -> bool:
    """Check if the task has access to the connection."""
    # TODO: Placeholder for actual implementation

    log.debug(
        "Checking access for task instance with key '%s' to connection '%s'",
        token.id,
        connection_id,
    )
    return True


router = APIRouter(
    responses={status.HTTP_404_NOT_FOUND: {"description": "Connection not found"}},
    dependencies=[Depends(has_connection_access)],
)

log = logging.getLogger(__name__)


@router.get(
    "/{connection_id}",
    responses={
        status.HTTP_401_UNAUTHORIZED: {"description": "Unauthorized"},
        status.HTTP_403_FORBIDDEN: {"description": "Task does not have access to the connection"},
    },
)
def get_connection(connection_id: str) -> ConnectionResponse:
    """Get an Airflow connection."""
    try:
        connection = Connection.get_connection_from_secrets(connection_id)
    except AirflowNotFoundException:
        raise HTTPException(
            status.HTTP_404_NOT_FOUND,
            detail={
                "reason": "not_found",
                "message": f"Connection with ID {connection_id} not found",
            },
        )
    return ConnectionResponse.model_validate(connection)
