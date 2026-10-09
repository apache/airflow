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

from typing import TYPE_CHECKING
from uuid import UUID

from sqlalchemy import func, select
from sqlalchemy.orm import joinedload

from airflow.api_fastapi.core_api.services.public.task_coordinates import region_scope_filter
from airflow.models.dagrun import DagRun
from airflow.models.task_coordinates import public_map_index_expression
from airflow.models.taskinstance import TaskInstance as TI

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

    from airflow.api_fastapi.common.parameters import SortParam


def get_execution_members(
    run: DagRun,
    *,
    session: Session,
    order_by: SortParam | None = None,
    task_id: str | None = None,
    region_id: UUID | None = None,
    region_index: int | None = None,
    try_number: int | None = None,
    limit: int = 100,
    offset: int = 0,
) -> tuple[list[tuple[TI, int]], int]:
    """
    List a run's live task instances, or the tries numbered ``try_number`` when one is given.

    Each member is paired with its public map index.

    Live work has exactly one row per task coordinate; archived tries share their coordinate
    with the live row, so selecting one needs its try number.

    ``region_id`` scopes to that coordinate and everything nested beneath it, so asking for a
    loop's region returns the mapped expansions created inside its iterations too.
    """
    query = select(TI, public_map_index_expression(TI).label("map_index")).where(
        TI.dag_id == run.dag_id, TI.run_id == run.run_id
    )
    if try_number is not None:
        query = query.execution_options(include_all_attempts=True)
    if region_id is not None:
        query = query.where(
            region_scope_filter(
                dag_id=run.dag_id,
                run_id=run.run_id,
                region_id=region_id,
                region_index=region_index,
                session=session,
            )
        )
    for column, value in (
        (TI.task_id, task_id),
        (TI.try_number, try_number),
    ):
        if value is not None:
            query = query.where(column == value)
    total = session.scalar(
        select(func.count()).select_from(query.subquery()).execution_options(**query.get_execution_options())
    )
    # A stable tail keeps paging deterministic when the chosen column ties.
    ordering = (
        (TI.task_id, TI.region_id, TI.region_index, TI.try_number, TI.id)
        if order_by is None or not order_by.value
        else (*order_by.to_orm(select(TI))._order_by_clauses, TI.id)
    )
    members = session.execute(
        query.options(joinedload(TI.task_instance_note)).order_by(*ordering).limit(limit).offset(offset)
    ).all()
    return [(ti, map_index) for ti, map_index in members], total or 0
