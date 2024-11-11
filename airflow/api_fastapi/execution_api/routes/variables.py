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

from fastapi import Depends, HTTPException, status
from typing_extensions import Annotated

from airflow.api_fastapi.common.router import AirflowRouter
from airflow.api_fastapi.execution_api import datamodels
from airflow.models.variable import Variable

# TODO: Add dependency on JWT token
router = AirflowRouter(
    responses={status.HTTP_404_NOT_FOUND: {"description": "Variable not found"}},
)

log = logging.getLogger(__name__)


def get_task_token() -> datamodels.TIToken:
    """TODO: Placeholder for task identity authentication. This should be replaced with actual JWT decoding and validation."""
    return datamodels.TIToken(ti_key="test_key")


@router.get(
    "/{variable_key}",
    responses={
        status.HTTP_401_UNAUTHORIZED: {"description": "Unauthorized"},
        status.HTTP_403_FORBIDDEN: {"description": "Task does not have access to the variable"},
    },
)
def get_variable(
    variable_key: str,
    token: Annotated[datamodels.TIToken, Depends(get_task_token)],
) -> datamodels.VariableResponse:
    """Get an Airflow Variable."""
    if not has_variable_access(variable_key, token):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail={
                "reason": "access_denied",
                "message": f"Task does not have access to variable {variable_key}",
            },
        )

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

    return datamodels.VariableResponse(key=variable_key, value=variable_value)


def has_variable_access(variable_key: str, token: datamodels.TIToken) -> bool:
    """Check if the task has access to the variable."""
    # TODO: Placeholder for actual implementation

    ti_key = token.ti_key
    log.debug(
        "Checking access for task instance with key '%s' to variable '%s'",
        ti_key,
        variable_key,
    )
    return True
