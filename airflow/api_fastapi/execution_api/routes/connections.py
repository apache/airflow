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
from typing import Annotated

from fastapi import HTTPException, status

from airflow.api_fastapi.common.router import AirflowRouter
from airflow.api_fastapi.execution_api import datamodels, deps
from airflow.exceptions import AirflowNotFoundException
from airflow.models.connection import Connection

# TODO: Add dependency on JWT token
router = AirflowRouter(
    responses={status.HTTP_404_NOT_FOUND: {"description": "Connection not found"}},
)

log = logging.getLogger(__name__)


@router.get(
    "/{connection_id}",
    responses={
        status.HTTP_401_UNAUTHORIZED: {"description": "Unauthorized"},
        status.HTTP_403_FORBIDDEN: {"description": "Task does not have access to the connection"},
    },
)
def get_connection(
    connection_id: str,
    token: deps.TokenDep,
) -> datamodels.ConnectionResponse:
    """Get an Airflow connection."""
    if not has_connection_access(connection_id, token):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail={
                "reason": "access_denied",
                "message": f"Task does not have access to connection {connection_id}",
            },
        )
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
    return datamodels.ConnectionResponse.model_validate(connection, from_attributes=True)


def has_connection_access(connection_id: str, token: datamodels.TIToken) -> bool:
    """Check if the task has access to the connection."""
    # TODO: Placeholder for actual implementation

    ti_key = token.ti_key
    log.debug(
        "Checking access for task instance with key '%s' to connection '%s'",
        ti_key,
        connection_id,
    )
    return True
