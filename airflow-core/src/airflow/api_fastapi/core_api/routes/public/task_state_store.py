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
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING, Annotated, Literal

from fastapi import Depends, HTTPException, Query, status
from sqlalchemy import select

from airflow._shared.state import INFRA_RETRIES_USED_STATE_KEY, TaskScope
from airflow.api_fastapi.auth.managers.models.resource_details import DagAccessEntity
from airflow.api_fastapi.common.db.common import SessionDep, paginated_select
from airflow.api_fastapi.common.parameters import QueryLimit, QueryOffset
from airflow.api_fastapi.common.router import AirflowRouter
from airflow.api_fastapi.core_api.datamodels.task_state_store import (
    TaskStateStoreBody,
    TaskStateStoreCollectionResponse,
    TaskStateStorePatchBody,
    TaskStateStoreResponse,
)
from airflow.api_fastapi.core_api.openapi.exceptions import create_openapi_http_exception_doc
from airflow.api_fastapi.core_api.security import requires_access_dag
from airflow.configuration import conf
from airflow.models.task_state_store import TaskStateStoreModel
from airflow.models.taskinstance import TaskInstance as TI
from airflow.state.metastore import _get_db_backend

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

task_state_store_router = AirflowRouter(
    tags=["Task State Store"],
    prefix="/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances/{task_id}/state-store",
)


def _require_task_instance(
    dag_id: str,
    dag_run_id: str,
    task_id: str,
    map_index: int | None,
    session: Session,
) -> None:
    """Raise 404 unless the task instance exists. ``map_index=None`` matches any map index."""
    statement = select(TI.task_id).where(
        TI.dag_id == dag_id,
        TI.run_id == dag_run_id,
        TI.task_id == task_id,
    )
    if map_index is not None:
        statement = statement.where(TI.map_index == map_index)
    if session.scalar(statement.limit(1)) is None:
        addressed_by = "all_map_indices=True" if map_index is None else f"map_index={map_index}"
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=(
                f"Task instance not found for dag_id={dag_id!r}, run_id={dag_run_id!r}, "
                f"task_id={task_id!r}, {addressed_by}"
            ),
        )


def _resolve_scope(
    dag_id: str,
    dag_run_id: str,
    task_id: str,
    map_index: Annotated[int, Query(ge=-1)] = -1,
) -> TaskScope:
    """Map the path and query parameters onto the task instance they address."""
    return TaskScope(dag_id=dag_id, run_id=dag_run_id, task_id=task_id, map_index=map_index)


TaskScopeDep = Annotated[TaskScope, Depends(_resolve_scope)]


def _validate_scope(scope: TaskScopeDep, session: SessionDep) -> TaskScope:
    """Resolve the scope, 404ing when the task instance it addresses does not exist."""
    _require_task_instance(scope.dag_id, scope.run_id, scope.task_id, scope.map_index, session)
    return scope


ValidatedTaskScopeDep = Annotated[TaskScope, Depends(_validate_scope)]


def _validate_clear_scope(
    scope: TaskScopeDep,
    session: SessionDep,
    all_map_indices: Annotated[bool, Query()] = False,
) -> TaskScope:
    """
    Resolve the scope for a clear request, 404ing when the task instance does not exist.

    ``all_map_indices`` addresses the task across every index, so it is validated against any
    instance -- an expanded mapped task has no ``map_index=-1`` instance to check.
    """
    _require_task_instance(
        scope.dag_id, scope.run_id, scope.task_id, None if all_map_indices else scope.map_index, session
    )
    return scope


ValidatedClearTaskScopeDep = Annotated[TaskScope, Depends(_validate_clear_scope)]


def _reject_airflow_owned_key(key: str) -> None:
    if key == INFRA_RETRIES_USED_STATE_KEY:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Task state store key {key!r} is managed by Airflow",
        )


def _resolve_expires_at(expires_at: datetime | None | Literal["default"]) -> datetime | None:
    """
    Resolve the expires_at value from the request body.

    - ``"default"``: apply configured ``[state_store] default_retention_days``.
      ``0`` means never expire. Negative values raise HTTP 400.
    - ``None``: never expire
    - datetime: use as-is
    """
    if expires_at == "default":
        days = conf.getint("state_store", "default_retention_days")
        if days < 0:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"[state_store] default_retention_days must be >= 0, got {days}. "
                "Set to 0 to disable expiry.",
            )
        return None if days == 0 else datetime.now(tz=timezone.utc) + timedelta(days=days)
    return expires_at


@task_state_store_router.get(
    "",
    dependencies=[Depends(requires_access_dag(method="GET", access_entity=DagAccessEntity.TASK_INSTANCE))],
)
def list_task_state_store(
    scope: TaskScopeDep,
    limit: QueryLimit,
    offset: QueryOffset,
    session: SessionDep,
) -> TaskStateStoreCollectionResponse:
    """List all task state store entries for a task instance."""
    base = (
        select(
            TaskStateStoreModel.key,
            TaskStateStoreModel.value,
            TaskStateStoreModel.updated_at,
            TaskStateStoreModel.expires_at,
        )
        .where(
            TaskStateStoreModel.dag_id == scope.dag_id,
            TaskStateStoreModel.run_id == scope.run_id,
            TaskStateStoreModel.task_id == scope.task_id,
            TaskStateStoreModel.map_index == scope.map_index,
        )
        .order_by(TaskStateStoreModel.key.asc())
    )
    paginated, total_entries = paginated_select(
        statement=base,
        filters=None,
        order_by=None,
        offset=offset,
        limit=limit,
        session=session,
    )
    rows = session.execute(paginated).all()
    entries = [
        TaskStateStoreResponse(
            key=r.key, value=json.loads(r.value), updated_at=r.updated_at, expires_at=r.expires_at
        )
        for r in rows
    ]
    return TaskStateStoreCollectionResponse(task_state_store=entries, total_entries=total_entries)


@task_state_store_router.get(
    "/{key:path}",
    responses=create_openapi_http_exception_doc([status.HTTP_404_NOT_FOUND]),
    dependencies=[Depends(requires_access_dag(method="GET", access_entity=DagAccessEntity.TASK_INSTANCE))],
)
def get_task_state_store(
    scope: TaskScopeDep,
    key: str,
    session: SessionDep,
) -> TaskStateStoreResponse:
    """Get a single task state store entry."""
    row = session.execute(
        select(
            TaskStateStoreModel.key,
            TaskStateStoreModel.value,
            TaskStateStoreModel.updated_at,
            TaskStateStoreModel.expires_at,
        ).where(
            TaskStateStoreModel.dag_id == scope.dag_id,
            TaskStateStoreModel.run_id == scope.run_id,
            TaskStateStoreModel.task_id == scope.task_id,
            TaskStateStoreModel.map_index == scope.map_index,
            TaskStateStoreModel.key == key,
        )
    ).one_or_none()
    if row is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Task state store key {key!r} not found",
        )
    return TaskStateStoreResponse(
        key=row.key, value=json.loads(row.value), updated_at=row.updated_at, expires_at=row.expires_at
    )


@task_state_store_router.put(
    "/{key:path}",
    status_code=status.HTTP_204_NO_CONTENT,
    responses=create_openapi_http_exception_doc([status.HTTP_404_NOT_FOUND]),
    dependencies=[Depends(requires_access_dag(method="PUT", access_entity=DagAccessEntity.TASK_INSTANCE))],
)
def set_task_state_store(
    scope: ValidatedTaskScopeDep,
    key: str,
    body: TaskStateStoreBody,
    session: SessionDep,
) -> None:
    """Set a task state store value. Creates or overwrites the key."""
    _reject_airflow_owned_key(key)
    expires_at = _resolve_expires_at(body.expires_at)
    try:
        _get_db_backend().set(scope, key, json.dumps(body.value), expires_at=expires_at, session=session)
    except ValueError as e:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(e)) from e


@task_state_store_router.patch(
    "/{key:path}",
    status_code=status.HTTP_200_OK,
    responses=create_openapi_http_exception_doc([status.HTTP_404_NOT_FOUND]),
    dependencies=[Depends(requires_access_dag(method="PUT", access_entity=DagAccessEntity.TASK_INSTANCE))],
)
def patch_task_state_store(
    scope: ValidatedTaskScopeDep,
    key: str,
    body: TaskStateStorePatchBody,
    session: SessionDep,
) -> None:
    """Update the value of an existing task state store key."""
    _reject_airflow_owned_key(key)
    existing = session.execute(
        select(TaskStateStoreModel.expires_at).where(
            TaskStateStoreModel.dag_id == scope.dag_id,
            TaskStateStoreModel.run_id == scope.run_id,
            TaskStateStoreModel.task_id == scope.task_id,
            TaskStateStoreModel.map_index == scope.map_index,
            TaskStateStoreModel.key == key,
        )
    ).one_or_none()

    if existing is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Task state store key {key!r} not found",
        )

    _get_db_backend().set(scope, key, json.dumps(body.value), expires_at=existing.expires_at, session=session)


@task_state_store_router.delete(
    "/{key:path}",
    status_code=status.HTTP_204_NO_CONTENT,
    responses=create_openapi_http_exception_doc([status.HTTP_404_NOT_FOUND]),
    dependencies=[Depends(requires_access_dag(method="DELETE", access_entity=DagAccessEntity.TASK_INSTANCE))],
)
def delete_task_state_store(
    scope: ValidatedTaskScopeDep,
    key: str,
    session: SessionDep,
) -> None:
    """Delete a single task state store key. No-op if the key does not exist."""
    _reject_airflow_owned_key(key)
    _get_db_backend().delete(scope, key, session=session)


@task_state_store_router.delete(
    "",
    status_code=status.HTTP_204_NO_CONTENT,
    responses=create_openapi_http_exception_doc([status.HTTP_404_NOT_FOUND]),
    dependencies=[Depends(requires_access_dag(method="DELETE", access_entity=DagAccessEntity.TASK_INSTANCE))],
)
def clear_task_state_store(
    scope: ValidatedClearTaskScopeDep,
    session: SessionDep,
    all_map_indices: Annotated[bool, Query()] = False,
) -> None:
    """
    Delete all task state store keys for a task instance.

    When ``all_map_indices=true``, state store is cleared for every map index of the task and
    the ``map_index`` parameter is ignored.
    """
    _get_db_backend().clear_user_task_state(
        scope,
        all_map_indices=all_map_indices,
        session=session,
    )
