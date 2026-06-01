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
from typing import Annotated, Literal

from fastapi import Depends, HTTPException, Query, status
from sqlalchemy import select

from airflow._shared.state import TaskScope
from airflow.api_fastapi.auth.managers.models.resource_details import DagAccessEntity
from airflow.api_fastapi.common.db.common import SessionDep, paginated_select
from airflow.api_fastapi.common.parameters import QueryLimit, QueryOffset
from airflow.api_fastapi.common.router import AirflowRouter
from airflow.api_fastapi.core_api.datamodels.task_state import (
    TaskStateBody,
    TaskStateCollectionResponse,
    TaskStatePatchBody,
    TaskStateResponse,
)
from airflow.api_fastapi.core_api.openapi.exceptions import create_openapi_http_exception_doc
from airflow.api_fastapi.core_api.security import requires_access_dag
from airflow.configuration import conf
from airflow.models.task_state import TaskStateModel
from airflow.models.taskinstance import TaskInstance as TI
from airflow.state import get_state_backend

task_state_router = AirflowRouter(
    tags=["Task State"],
    prefix="/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances/{task_id}/states",
)


def _get_scope(dag_id: str, dag_run_id: str, task_id: str, map_index: int) -> TaskScope:
    return TaskScope(dag_id=dag_id, run_id=dag_run_id, task_id=task_id, map_index=map_index)


def _resolve_expires_at(expires_at: datetime | None | Literal["default"]) -> datetime | None:
    """
    Resolve the expires_at value from the request body.

    - ``"default"``: apply configured default_retention_days
    - ``None``: never expire
    - datetime: use as-is
    """
    if expires_at == "default":
        days = conf.getint("state_store", "default_retention_days")
        return datetime.now(tz=timezone.utc) + timedelta(days=days)
    return expires_at


def _require_ti(dag_id: str, dag_run_id: str, task_id: str, map_index: int, session: SessionDep) -> None:
    ti_exists = session.scalar(
        select(TI.task_id).where(
            TI.dag_id == dag_id,
            TI.run_id == dag_run_id,
            TI.task_id == task_id,
            TI.map_index == map_index,
        )
    )
    if ti_exists is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Task instance not found for dag_id={dag_id!r}, run_id={dag_run_id!r}, task_id={task_id!r}, map_index={map_index}",
        )


@task_state_router.get(
    "",
    responses=create_openapi_http_exception_doc([status.HTTP_404_NOT_FOUND]),
    dependencies=[Depends(requires_access_dag(method="GET", access_entity=DagAccessEntity.TASK_INSTANCE))],
)
def list_task_states(
    dag_id: str,
    dag_run_id: str,
    task_id: str,
    limit: QueryLimit,
    offset: QueryOffset,
    session: SessionDep,
    map_index: Annotated[int, Query(ge=-1)] = -1,
) -> TaskStateCollectionResponse:
    """List all task state entries for a task instance."""
    base = (
        select(
            TaskStateModel.key,
            TaskStateModel.value,
            TaskStateModel.updated_at,
            TaskStateModel.expires_at,
        )
        .where(
            TaskStateModel.dag_id == dag_id,
            TaskStateModel.run_id == dag_run_id,
            TaskStateModel.task_id == task_id,
            TaskStateModel.map_index == map_index,
        )
        .order_by(TaskStateModel.key.asc())
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
        TaskStateResponse(
            key=r.key, value=json.loads(r.value), updated_at=r.updated_at, expires_at=r.expires_at
        )
        for r in rows
    ]
    return TaskStateCollectionResponse(task_states=entries, total_entries=total_entries)


@task_state_router.get(
    "/{key:path}",
    responses=create_openapi_http_exception_doc([status.HTTP_404_NOT_FOUND]),
    dependencies=[Depends(requires_access_dag(method="GET", access_entity=DagAccessEntity.TASK_INSTANCE))],
)
def get_task_state(
    dag_id: str,
    dag_run_id: str,
    task_id: str,
    key: str,
    session: SessionDep,
    map_index: Annotated[int, Query(ge=-1)] = -1,
) -> TaskStateResponse:
    """Get a single task state entry."""
    row = session.execute(
        select(
            TaskStateModel.key,
            TaskStateModel.value,
            TaskStateModel.updated_at,
            TaskStateModel.expires_at,
        ).where(
            TaskStateModel.dag_id == dag_id,
            TaskStateModel.run_id == dag_run_id,
            TaskStateModel.task_id == task_id,
            TaskStateModel.map_index == map_index,
            TaskStateModel.key == key,
        )
    ).one_or_none()
    if row is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Task state key {key!r} not found",
        )
    return TaskStateResponse(
        key=row.key, value=json.loads(row.value), updated_at=row.updated_at, expires_at=row.expires_at
    )


@task_state_router.put(
    "/{key:path}",
    status_code=status.HTTP_204_NO_CONTENT,
    responses=create_openapi_http_exception_doc([status.HTTP_404_NOT_FOUND]),
    dependencies=[Depends(requires_access_dag(method="PUT", access_entity=DagAccessEntity.TASK_INSTANCE))],
)
def set_task_state(
    dag_id: str,
    dag_run_id: str,
    task_id: str,
    key: str,
    body: TaskStateBody,
    session: SessionDep,
    map_index: Annotated[int, Query(ge=-1)] = -1,
) -> None:
    """Set a task state value. Creates or overwrites the key."""
    _require_ti(dag_id, dag_run_id, task_id, map_index, session)
    expires_at = _resolve_expires_at(body.expires_at)
    scope = _get_scope(dag_id, dag_run_id, task_id, map_index)
    try:
        get_state_backend().set(scope, key, json.dumps(body.value), expires_at=expires_at, session=session)
    except ValueError as e:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(e)) from e


@task_state_router.patch(
    "/{key:path}",
    status_code=status.HTTP_200_OK,
    responses=create_openapi_http_exception_doc([status.HTTP_404_NOT_FOUND]),
    dependencies=[Depends(requires_access_dag(method="PUT", access_entity=DagAccessEntity.TASK_INSTANCE))],
)
def patch_task_state(
    dag_id: str,
    dag_run_id: str,
    task_id: str,
    key: str,
    body: TaskStatePatchBody,
    session: SessionDep,
    map_index: Annotated[int, Query(ge=-1)] = -1,
) -> None:
    """Update the value of an existing task state key."""
    _require_ti(dag_id, dag_run_id, task_id, map_index, session)

    existing = session.execute(
        select(TaskStateModel.expires_at).where(
            TaskStateModel.dag_id == dag_id,
            TaskStateModel.run_id == dag_run_id,
            TaskStateModel.task_id == task_id,
            TaskStateModel.map_index == map_index,
            TaskStateModel.key == key,
        )
    ).one_or_none()

    if existing is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Task state key {key!r} not found",
        )

    scope = _get_scope(dag_id, dag_run_id, task_id, map_index)
    get_state_backend().set(
        scope, key, json.dumps(body.value), expires_at=existing.expires_at, session=session
    )


@task_state_router.delete(
    "/{key:path}",
    status_code=status.HTTP_204_NO_CONTENT,
    responses=create_openapi_http_exception_doc([status.HTTP_404_NOT_FOUND]),
    dependencies=[Depends(requires_access_dag(method="DELETE", access_entity=DagAccessEntity.TASK_INSTANCE))],
)
def delete_task_state(
    dag_id: str,
    dag_run_id: str,
    task_id: str,
    key: str,
    session: SessionDep,
    map_index: Annotated[int, Query(ge=-1)] = -1,
) -> None:
    """Delete a single task state key. No-op if the key does not exist."""
    scope = _get_scope(dag_id, dag_run_id, task_id, map_index)
    get_state_backend().delete(scope, key, session=session)


@task_state_router.delete(
    "",
    status_code=status.HTTP_204_NO_CONTENT,
    responses=create_openapi_http_exception_doc([status.HTTP_404_NOT_FOUND]),
    dependencies=[Depends(requires_access_dag(method="DELETE", access_entity=DagAccessEntity.TASK_INSTANCE))],
)
def clear_task_state(
    dag_id: str,
    dag_run_id: str,
    task_id: str,
    session: SessionDep,
    map_index: Annotated[int, Query(ge=-1)] = -1,
    all_map_indices: Annotated[bool, Query()] = False,
) -> None:
    """
    Delete all task state keys for a task instance.

    When ``all_map_indices=true``, state is cleared for every map index of the task and
    the ``map_index`` parameter is ignored.
    """
    scope = _get_scope(dag_id, dag_run_id, task_id, map_index)
    get_state_backend().clear(scope, all_map_indices=all_map_indices, session=session)
