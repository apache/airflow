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

from fastapi import Depends, Query, status
from sqlalchemy import and_, func, or_, select
from sqlalchemy.orm import joinedload

from airflow.api_fastapi.common.db.common import SessionDep, paginated_select
from airflow.api_fastapi.common.parameters import (
    FilterParam,
    QueryLimit,
    QueryOffset,
    SortParam,
    filter_param_factory,
)
from airflow.api_fastapi.common.router import AirflowRouter
from airflow.api_fastapi.core_api.datamodels.backfills import (
    BackfillResponse,
    BackfillUiCollectionResponse,
    BackfillUiResponse,
)
from airflow.api_fastapi.core_api.openapi.exceptions import (
    create_openapi_http_exception_doc,
)
from airflow.api_fastapi.core_api.security import ReadableBackfillsFilterDep, requires_access_backfill
from airflow.models import DagRun
from airflow.models.backfill import Backfill
from airflow.models.taskinstance import TaskInstance
from airflow.utils.state import TaskInstanceState

backfills_router = AirflowRouter(tags=["Backfill"], prefix="/backfills")


def _running_task_instances_subquery():
    return (
        select(func.count(TaskInstance.id))
        .join(DagRun, and_(DagRun.dag_id == TaskInstance.dag_id, DagRun.run_id == TaskInstance.run_id))
        .where(DagRun.backfill_id == Backfill.id, TaskInstance.state == TaskInstanceState.RUNNING)
        .correlate(Backfill)
        .scalar_subquery()
    )


@backfills_router.get(
    path="",
    responses=create_openapi_http_exception_doc([status.HTTP_404_NOT_FOUND]),
    dependencies=[
        Depends(requires_access_backfill(method="GET")),
    ],
)
def list_backfills_ui(
    limit: QueryLimit,
    offset: QueryOffset,
    order_by: Annotated[
        SortParam,
        Depends(SortParam(["id"], Backfill).dynamic_depends()),
    ],
    readable_backfills_filter: ReadableBackfillsFilterDep,
    session: SessionDep,
    dag_id: Annotated[FilterParam[str | None], Depends(filter_param_factory(Backfill.dag_id, str | None))],
    active: Annotated[bool | None, Query()] = None,
) -> BackfillUiCollectionResponse:
    running_task_instances = _running_task_instances_subquery()
    statement = select(Backfill, running_task_instances.label("running_task_instances")).options(
        joinedload(Backfill.dag_model)
    )
    if active is True:
        statement = statement.where(or_(Backfill.completed_at.is_(None), running_task_instances > 0))
    elif active is False:
        statement = statement.where(Backfill.completed_at.is_not(None), running_task_instances == 0)

    select_stmt, total_entries = paginated_select(
        statement=statement,
        filters=[dag_id, readable_backfills_filter],
        order_by=order_by,
        offset=offset,
        limit=limit,
        session=session,
    )
    backfills = [
        BackfillUiResponse(
            **BackfillResponse.model_validate(backfill).model_dump(),
            running_task_instances=running_task_instances,
        )
        for backfill, running_task_instances in session.execute(select_stmt)
    ]
    return BackfillUiCollectionResponse(
        backfills=backfills,
        total_entries=total_entries,
    )
