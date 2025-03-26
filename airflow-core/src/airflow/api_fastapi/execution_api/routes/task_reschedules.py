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

from fastapi import HTTPException, Query, status
from sqlalchemy import select

from airflow.api_fastapi.common.db.common import SessionDep
from airflow.api_fastapi.common.router import AirflowRouter
from airflow.api_fastapi.common.types import UtcDateTime
from airflow.models.taskreschedule import TaskReschedule

router = AirflowRouter(
    responses={
        status.HTTP_404_NOT_FOUND: {"description": "Task Instance not found"},
        status.HTTP_401_UNAUTHORIZED: {"description": "Unauthorized"},
    },
)


@router.get("/{task_instance_id}/start_date")
def get_start_date(
    task_instance_id: UUID, session: SessionDep, try_number: Annotated[int, Query()] = 1
) -> UtcDateTime:
    start_date = session.scalar(
        select(TaskReschedule)
        .where(
            TaskReschedule.ti_id == str(task_instance_id),
            TaskReschedule.try_number >= try_number,
        )
        .order_by(TaskReschedule.id.asc())
        .with_only_columns(TaskReschedule.start_date)
        .limit(1)
    )
    if start_date is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND)

    return start_date
