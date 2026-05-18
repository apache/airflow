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

from fastapi import Depends, HTTPException, Query, status
from sqlalchemy import select

from airflow._shared.state import TaskScope
from airflow.api_fastapi.auth.managers.models.resource_details import DagAccessEntity
from airflow.api_fastapi.common.db.common import SessionDep
from airflow.api_fastapi.common.router import AirflowRouter
from airflow.api_fastapi.core_api.datamodels.task_state import (
    TaskStateBody,
    TaskStateCollectionResponse,
    TaskStateEntry,
)
from airflow.api_fastapi.core_api.openapi.exceptions import create_openapi_http_exception_doc
from airflow.api_fastapi.core_api.security import requires_access_dag
from airflow.models.task_state import TaskStateModel
from airflow.state.metastore import MetastoreStateBackend

task_state_router = AirflowRouter(
    tags=["Task State"],
    prefix="/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances/{task_id}/state",
)


def _get_scope(dag_id: str, dag_run_id: str, task_id: str, map_index: int) -> TaskScope:
    return TaskScope(dag_id=dag_id, run_id=dag_run_id, task_id=task_id, map_index=map_index)


@task_state_router.get(
    "",
    responses=create_openapi_http_exception_doc([status.HTTP_404_NOT_FOUND]),
    dependencies=[Depends(requires_access_dag(method="GET", access_entity=DagAccessEntity.TASK_INSTANCE))],
)
def list_task_state(
    dag_id: str,
    dag_run_id: str,
    task_id: str,
    session: SessionDep,
    map_index: Annotated[int, Query(ge=-1)] = -1,
) -> TaskStateCollectionResponse:
    """List all task state entries for a task instance."""
    rows = session.execute(
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
        )
    ).all()
    entries = [
        TaskStateEntry(key=r.key, value=r.value, updated_at=r.updated_at, expires_at=r.expires_at)
        for r in rows
    ]
    return TaskStateCollectionResponse(task_states=entries, total_entries=len(entries))


@task_state_router.get(
    "/{key}",
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
) -> TaskStateEntry:
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
            detail={"reason": "not_found", "message": f"Task state key {key!r} not found"},
        )
    return TaskStateEntry(key=row.key, value=row.value, updated_at=row.updated_at, expires_at=row.expires_at)


@task_state_router.put(
    "/{key}",
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
    scope = _get_scope(dag_id, dag_run_id, task_id, map_index)
    MetastoreStateBackend().set(scope, key, body.value, session=session)


@task_state_router.delete(
    "/{key}",
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
    MetastoreStateBackend().delete(scope, key, session=session)


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
    """Delete all task state keys for a task instance."""
    scope = _get_scope(dag_id, dag_run_id, task_id, map_index)
    MetastoreStateBackend().clear(scope, all_map_indices=all_map_indices, session=session)
