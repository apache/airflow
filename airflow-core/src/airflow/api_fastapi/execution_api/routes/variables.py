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

from fastapi import APIRouter, Depends, HTTPException, Path, Query, Request, status
from sqlalchemy import func, select

from airflow.api_fastapi.common.db.common import SessionDep
from airflow.api_fastapi.execution_api.datamodels.variable import (
    VariableKeysResponse,
    VariablePostBody,
    VariableResponse,
)
from airflow.api_fastapi.execution_api.security import CurrentTIToken, get_team_name_dep
from airflow.models.variable import Variable


async def has_variable_access(
    request: Request,
    variable_key: Annotated[str, Path(min_length=1)],
    token=CurrentTIToken,
):
    """Check if the task has access to the variable."""
    write = request.method not in {"GET", "HEAD", "OPTIONS"}

    log.debug(
        "Checking %s access for task instance with key '%s' to variable '%s'",
        "write" if write else "read",
        token.id,
        variable_key,
    )

    # The current version of Airflow does not support true
    # multi-tenancy yet (this is well-documented at
    # https://airflow.apache.org/docs/apache-airflow/stable/security/security_model.html#limiting-dag-author-access-to-subset-of-dags),
    # so for now we always return 'True' here.
    # When we introduce true multi-tenancy in the future
    # this would be the place to do add a check.
    return True


router = APIRouter()

log = logging.getLogger(__name__)


# /keys must be declared before /{variable_key:path} so the static path is
# matched first; otherwise the catch-all path param would swallow it.
# has_variable_access is applied per-route below (not at router level) because
# it requires a variable_key path parameter that /keys does not have.
@router.get(
    "/keys",
    responses={
        status.HTTP_401_UNAUTHORIZED: {"description": "Unauthorized"},
    },
)
def get_variable_keys(
    session: SessionDep,
    team_name: Annotated[str | None, Depends(get_team_name_dep)] = None,
    prefix: Annotated[str | None, Query()] = None,
    limit: Annotated[int, Query(ge=1, le=10_000)] = 1000,
    offset: Annotated[int, Query(ge=0)] = 0,
) -> VariableKeysResponse:
    """
    Get Airflow Variable keys, optionally filtered by prefix.

    .. note::
        This endpoint deliberately bypasses the per-variable ``has_variable_access``
        check, since access scoping requires a specific variable key. Any authenticated
        task within a team can therefore enumerate every variable key in that team —
        including keys for variables it would not be allowed to read. This is consistent
        with Airflow's security model (workers within a deployment trust each other),
        but the asymmetry between key enumeration and value access is intentional.
    """
    stmt = select(Variable.key).order_by(Variable.key)
    if prefix is not None:
        stmt = stmt.where(Variable.key.startswith(prefix, autoescape=True))
    if team_name is not None:
        stmt = stmt.where(Variable.team_name == team_name)

    total_entries = session.scalar(select(func.count()).select_from(stmt.subquery())) or 0
    keys = session.scalars(stmt.offset(offset).limit(limit)).all()
    return VariableKeysResponse(keys=list(keys), total_entries=total_entries)


@router.get(
    "/{variable_key:path}",
    dependencies=[Depends(has_variable_access)],
    responses={
        status.HTTP_404_NOT_FOUND: {"description": "Variable not found"},
        status.HTTP_401_UNAUTHORIZED: {"description": "Unauthorized"},
        status.HTTP_403_FORBIDDEN: {"description": "Task does not have access to the variable"},
    },
)
def get_variable(
    variable_key: Annotated[str, Path(min_length=1)],
    team_name: Annotated[str | None, Depends(get_team_name_dep)],
) -> VariableResponse:
    """Get an Airflow Variable."""
    try:
        variable_value = Variable.get(variable_key, team_name=team_name)
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
    "/{variable_key:path}",
    dependencies=[Depends(has_variable_access)],
    status_code=status.HTTP_201_CREATED,
    responses={
        status.HTTP_404_NOT_FOUND: {"description": "Variable not found"},
        status.HTTP_401_UNAUTHORIZED: {"description": "Unauthorized"},
        status.HTTP_403_FORBIDDEN: {"description": "Task does not have access to the variable"},
    },
)
def put_variable(
    variable_key: Annotated[str, Path(min_length=1)],
    body: VariablePostBody,
    team_name: Annotated[str | None, Depends(get_team_name_dep)],
):
    """Set an Airflow Variable."""
    Variable.set(key=variable_key, value=body.value, description=body.description, team_name=team_name)
    return {"message": "Variable successfully set"}


@router.delete(
    "/{variable_key:path}",
    dependencies=[Depends(has_variable_access)],
    status_code=status.HTTP_204_NO_CONTENT,
    responses={
        status.HTTP_404_NOT_FOUND: {"description": "Variable not found"},
        status.HTTP_401_UNAUTHORIZED: {"description": "Unauthorized"},
        status.HTTP_403_FORBIDDEN: {"description": "Task does not have access to the variable"},
    },
)
def delete_variable(
    variable_key: Annotated[str, Path(min_length=1)],
    team_name: Annotated[str | None, Depends(get_team_name_dep)],
):
    """Delete an Airflow Variable."""
    Variable.delete(key=variable_key, team_name=team_name)
