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

from fastapi import APIRouter, status
from sqlalchemy import select

from airflow.api_fastapi.common.db.common import SessionDep
from airflow.api_fastapi.common.types import UtcDateTime
from airflow.models.taskreschedule import TaskReschedule

router = APIRouter(
    responses={
        status.HTTP_404_NOT_FOUND: {"description": "Task Instance not found"},
        status.HTTP_401_UNAUTHORIZED: {"description": "Unauthorized"},
    },
)


@router.get("/{task_instance_id}/start_date")
def get_start_date(task_instance_id: UUID, session: SessionDep) -> UtcDateTime | None:
    """Get the first reschedule date if found, None if no records exist."""
    start_date = session.scalar(
        select(TaskReschedule.start_date)
        .where(TaskReschedule.ti_id == str(task_instance_id))
        .order_by(TaskReschedule.id.asc())
        .limit(1)
    )

    return start_date
