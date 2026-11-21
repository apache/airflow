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

from uuid import UUID

from fastapi import APIRouter, Security, status
from sqlalchemy import and_, func, select

from airflow.api_fastapi.common.db.common import AsyncSessionDep
from airflow.api_fastapi.common.types import UtcDateTime
from airflow.api_fastapi.execution_api.security import require_auth
from airflow.models.taskinstance import TaskInstance
from airflow.models.taskreschedule import TaskReschedule

router = APIRouter(
    dependencies=[
        # Validates that the JWT sub matches the task_instance_id path parameter.
        Security(require_auth, scopes=["ti:self"]),
    ],
    responses={
        status.HTTP_404_NOT_FOUND: {"description": "Task Instance not found"},
        status.HTTP_401_UNAUTHORIZED: {"description": "Unauthorized"},
    },
)


@router.get("/{task_instance_id}/start_date")
async def get_start_date(
    task_instance_id: UUID, session: AsyncSessionDep, try_number: int | None = None
) -> UtcDateTime | None:
    """
    Get the first reschedule date if found, None if no records exist.

    With ``try_number``, the reschedules of earlier tries of the same task count as well, starting
    from that try. Every try is its own task instance row, so without it only the reschedules of
    this try are seen and a sensor that retries would start its timeout again.
    """
    if try_number is None:
        return await session.scalar(
            select(func.min(TaskReschedule.start_date)).where(TaskReschedule.ti_id == task_instance_id)
        )
    # Core tables, because ORM queries on TaskInstance only see the live try.
    attempt = TaskInstance.__table__.alias("attempt")
    current = TaskInstance.__table__.alias("current")
    return await session.scalar(
        select(func.min(TaskReschedule.start_date))
        .select_from(TaskReschedule)
        .join(attempt, TaskReschedule.ti_id == attempt.c.id)
        .join(
            current,
            and_(
                attempt.c.dag_id == current.c.dag_id,
                attempt.c.task_id == current.c.task_id,
                attempt.c.run_id == current.c.run_id,
                attempt.c.region_id == current.c.region_id,
                attempt.c.region_index == current.c.region_index,
            ),
        )
        .where(
            current.c.id == task_instance_id,
            attempt.c.try_number >= try_number,
            attempt.c.try_number <= current.c.try_number,
        )
    )
