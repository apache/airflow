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

from typing import Annotated
from uuid import UUID

from cadwyn import VersionedAPIRouter
from fastapi import HTTPException, Path, Query, Security, status
from sqlalchemy.orm import Session

from airflow._shared.state import TaskScope
from airflow.api_fastapi.common.db.common import SessionDep
from airflow.api_fastapi.execution_api.datamodels.task_state import (
    TaskStatePutBody,
    TaskStateResponse,
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
def get_task_state(
    task_instance_id: UUID,
    key: Annotated[str, Path(min_length=1)],
    session: SessionDep,
) -> TaskStateResponse:
    """Get value for a task state."""
    scope = _get_task_scope_for_ti(task_instance_id, session)
    value = get_state_backend().get(scope, key, session=session)  # type: ignore[call-arg]  # @provide_session adds session kwarg at runtime; BaseStateBackend signature omits it so mypy can't see it
    if value is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail={
                "reason": "not_found",
                "message": f"Task state key {key!r} not found",
            },
        )
    return TaskStateResponse(value=value)


@router.put("/{task_instance_id}/{key}", status_code=status.HTTP_204_NO_CONTENT)
def set_task_state(
    task_instance_id: UUID,
    key: Annotated[str, Path(min_length=1)],
    body: TaskStatePutBody,
    session: SessionDep,
) -> None:
    """Set a task state key, creating or updating the row."""
    scope = _get_task_scope_for_ti(task_instance_id, session)
    get_state_backend().set(scope, key, body.value, session=session)  # type: ignore[call-arg]  # @provide_session adds session kwarg at runtime; BaseStateBackend signature omits it so mypy can't see it


@router.delete("/{task_instance_id}/{key}", status_code=status.HTTP_204_NO_CONTENT)
def delete_task_state(
    task_instance_id: UUID,
    key: Annotated[str, Path(min_length=1)],
    session: SessionDep,
) -> None:
    """Delete a single task state key."""
    scope = _get_task_scope_for_ti(task_instance_id, session)
    get_state_backend().delete(scope, key, session=session)  # type: ignore[call-arg]  # @provide_session adds session kwarg at runtime; BaseStateBackend signature omits it so mypy can't see it


@router.delete("/{task_instance_id}", status_code=status.HTTP_204_NO_CONTENT)
def clear_task_state(
    task_instance_id: UUID,
    session: SessionDep,
    all_map_indices: Annotated[bool, Query()] = False,
) -> None:
    """
    Delete all task state keys for this task instance.

    By default, only keys for the requesting TI's exact ``map_index`` are
    cleared — same isolation as DELETE endpoint above.

    Pass ``?all_map_indices=true`` to wipe state for every mapped sibling of
    the task in the same DAG run.  This is intentionally fleet-wide: the
    ``ti:self`` JWT authentication scope authenticates that the caller is
    a legitimate member of the mapped task group, and grants it authority
    to reset shared task state on behalf of the whole group.
    The SDK only forwards this flag when the user calls ``task_state.clear(all_map_indices=True)``
    explicitly, so the expanded scope is always an explicit opt-in by the task author.

    For non-mapped tasks (``map_index=-1``), there is only ever one index, so
    ``?all_map_indices=true`` is functionally identical to the default and is
    accepted without error.
    """
    scope = _get_task_scope_for_ti(task_instance_id, session)
    get_state_backend().clear(scope, all_map_indices=all_map_indices, session=session)  # type: ignore[call-arg]  # @provide_session adds session kwarg at runtime; BaseStateBackend signature omits it so mypy can't see it
