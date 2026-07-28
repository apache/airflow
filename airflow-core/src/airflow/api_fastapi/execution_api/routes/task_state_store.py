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

import json
from typing import Annotated
from uuid import UUID

from cadwyn import VersionedAPIRouter
from fastapi import HTTPException, Path, Security, status
from sqlalchemy.orm import Session

from airflow._shared.state import TaskScope
from airflow.api_fastapi.common.db.common import SessionDep
from airflow.api_fastapi.execution_api.datamodels.task_state_store import (
    TaskStateStorePutBody,
    TaskStateStoreResponse,
)
from airflow.api_fastapi.execution_api.security import ExecutionAPIRoute, require_auth
from airflow.models.taskinstance import TaskInstance as TI
from airflow.state import get_state_backend

router = VersionedAPIRouter(
    route_class=ExecutionAPIRoute,
    responses={
        status.HTTP_401_UNAUTHORIZED: {"description": "Unauthorized"},
        status.HTTP_403_FORBIDDEN: {"description": "Access denied"},
        status.HTTP_404_NOT_FOUND: {"description": "Not found"},
    },
    dependencies=[Security(require_auth, scopes=["ti:self"])],
)


def _get_task_scope_for_ti(task_instance_id: UUID, session: Session) -> TaskScope:
    ti = session.get(TI, task_instance_id)
    if ti is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail={
                "reason": "not_found",
                "message": f"Task instance {task_instance_id} not found",
            },
        )
    return TaskScope(dag_id=ti.dag_id, run_id=ti.run_id, task_id=ti.task_id, map_index=ti.map_index)


@router.get("/{task_instance_id}/{key}")
def get_task_state_store(
    task_instance_id: UUID,
    key: Annotated[str, Path(min_length=1)],
    session: SessionDep,
) -> TaskStateStoreResponse:
    """Get value for a task state store key."""
    scope = _get_task_scope_for_ti(task_instance_id, session)
    value = get_state_backend().get(scope, key, session=session)
    if value is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail={
                "reason": "not_found",
                "message": f"Task state store key {key!r} not found",
            },
        )
    return TaskStateStoreResponse(value=json.loads(value))


@router.put("/{task_instance_id}/{key}", status_code=status.HTTP_204_NO_CONTENT)
def set_task_state_store(
    task_instance_id: UUID,
    key: Annotated[str, Path(min_length=1)],
    body: TaskStateStorePutBody,
    session: SessionDep,
) -> None:
    """Set a task state store key, creating or updating the row."""
    scope = _get_task_scope_for_ti(task_instance_id, session)
    get_state_backend().set(scope, key, json.dumps(body.value), expires_at=body.expires_at, session=session)


@router.delete("/{task_instance_id}/{key}", status_code=status.HTTP_204_NO_CONTENT)
def delete_task_state_store(
    task_instance_id: UUID,
    key: Annotated[str, Path(min_length=1)],
    session: SessionDep,
) -> None:
    """Delete a single task state store key."""
    scope = _get_task_scope_for_ti(task_instance_id, session)
    get_state_backend().delete(scope, key, session=session)


@router.delete("/{task_instance_id}", status_code=status.HTTP_204_NO_CONTENT)
def clear_task_state_store(
    task_instance_id: UUID,
    session: SessionDep,
) -> None:
    """Delete all task state store keys for this task instance."""
    scope = _get_task_scope_for_ti(task_instance_id, session)
    get_state_backend().clear(scope, session=session)
