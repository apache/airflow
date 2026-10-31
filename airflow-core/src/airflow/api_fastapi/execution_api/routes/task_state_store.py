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
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from starlette.concurrency import run_in_threadpool

from airflow._shared.state import TaskScope
from airflow.api_fastapi.common.db.common import AsyncSessionDep
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


async def _get_task_scope_for_ti(task_instance_id: UUID, session: AsyncSession) -> TaskScope:
    row = (
        await session.execute(
            select(TI.dag_id, TI.run_id, TI.task_id, TI.region_index, TI.region_id).where(
                TI.id == task_instance_id, TI.working_set.is_(True)
            )
        )
    ).one_or_none()
    if row is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail={
                "reason": "not_found",
                "message": f"Task instance {task_instance_id} not found",
            },
        )
    dag_id, run_id, task_id, region_index, region_id = row
    return TaskScope(
        dag_id=dag_id,
        run_id=run_id,
        task_id=task_id,
        map_index=region_index,
        region_id=region_id,
    )


@router.get("/{task_instance_id}/{key:path}")
async def get_task_state_store(
    task_instance_id: UUID,
    key: Annotated[str, Path(min_length=1)],
    session: AsyncSessionDep,
) -> TaskStateStoreResponse:
    """Get value for a task state store key."""
    scope = await _get_task_scope_for_ti(task_instance_id, session)
    value = await get_state_backend().aget(scope, key, session=session)
    if value is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail={
                "reason": "not_found",
                "message": f"Task state store key {key!r} not found",
            },
        )
    # Decoding and validating a large stored value would block the event loop for every other request.
    return await run_in_threadpool(lambda: TaskStateStoreResponse(value=json.loads(value)))


@router.put("/{task_instance_id}/{key:path}", status_code=status.HTTP_204_NO_CONTENT)
async def set_task_state_store(
    task_instance_id: UUID,
    key: Annotated[str, Path(min_length=1)],
    body: TaskStateStorePutBody,
    session: AsyncSessionDep,
) -> None:
    """Set a task state store key, creating or updating the row."""
    scope = await _get_task_scope_for_ti(task_instance_id, session)
    await get_state_backend().aset(
        scope, key, json.dumps(body.value), expires_at=body.expires_at, session=session
    )


@router.delete("/{task_instance_id}/{key:path}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_task_state_store(
    task_instance_id: UUID,
    key: Annotated[str, Path(min_length=1)],
    session: AsyncSessionDep,
) -> None:
    """Delete a single task state store key."""
    scope = await _get_task_scope_for_ti(task_instance_id, session)
    await get_state_backend().adelete(scope, key, session=session)


@router.delete("/{task_instance_id}", status_code=status.HTTP_204_NO_CONTENT)
async def clear_task_state_store(
    task_instance_id: UUID,
    session: AsyncSessionDep,
) -> None:
    """Delete all task state store keys for this task instance."""
    scope = await _get_task_scope_for_ti(task_instance_id, session)
    await get_state_backend().aclear(scope, session=session)
