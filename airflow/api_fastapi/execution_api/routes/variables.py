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

from fastapi import APIRouter, Depends, HTTPException, Path, Request, status

from airflow.api_fastapi.execution_api.datamodels.variable import VariablePostBody, VariableResponse
from airflow.api_fastapi.execution_api.deps import JWTBearerDep
from airflow.models.variable import Variable


async def has_variable_access(
    request: Request,
    variable_key: str = Path(),
    token=JWTBearerDep,
):
    """Check if the task has access to the variable."""
    write = request.method not in {"GET", "HEAD", "OPTIONS"}
    # TODO: Placeholder for actual implementation
    log.debug(
        "Checking %s access for task instance with key '%s' to variable '%s'",
        "write" if write else "read",
        token.id,
        variable_key,
    )
    return True


router = APIRouter(
    responses={status.HTTP_404_NOT_FOUND: {"description": "Variable not found"}},
    dependencies=[Depends(has_variable_access)],
)

log = logging.getLogger(__name__)


@router.get(
    "/{variable_key}",
    responses={
        status.HTTP_401_UNAUTHORIZED: {"description": "Unauthorized"},
        status.HTTP_403_FORBIDDEN: {"description": "Task does not have access to the variable"},
    },
)
def get_variable(variable_key: str) -> VariableResponse:
    """Get an Airflow Variable."""
    try:
        variable_value = Variable.get(variable_key)
    except KeyError:
        raise HTTPException(
            status.HTTP_404_NOT_FOUND,
            detail={
                "reason": "not_found",
                "message": f"Variable with key '{variable_key}' not found",
            },
        )

    return VariableResponse(key=variable_key, value=variable_value)


@router.put(
    "/{variable_key}",
    status_code=status.HTTP_201_CREATED,
    responses={
        status.HTTP_401_UNAUTHORIZED: {"description": "Unauthorized"},
        status.HTTP_403_FORBIDDEN: {"description": "Task does not have access to the variable"},
    },
)
def put_variable(variable_key: str, body: VariablePostBody):
    """Set an Airflow Variable."""
    Variable.set(key=variable_key, value=body.value, description=body.description)
    return {"message": "Variable successfully set"}
