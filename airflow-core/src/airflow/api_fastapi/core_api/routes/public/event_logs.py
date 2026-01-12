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

from datetime import datetime
from typing import Annotated

from fastapi import Depends, HTTPException, status
from sqlalchemy import select
from sqlalchemy.orm import joinedload

from airflow.api_fastapi.common.db.common import (
    SessionDep,
    paginated_select,
)
from airflow.api_fastapi.common.parameters import (
    FilterOptionEnum,
    FilterParam,
    QueryLimit,
    QueryOffset,
    SortParam,
    _SearchParam,
    filter_param_factory,
    search_param_factory,
)
from airflow.api_fastapi.common.router import AirflowRouter
from airflow.api_fastapi.core_api.datamodels.event_logs import (
    EventLogCollectionResponse,
    EventLogResponse,
)
from airflow.api_fastapi.core_api.openapi.exceptions import create_openapi_http_exception_doc
from airflow.api_fastapi.core_api.security import DagAccessEntity, requires_access_dag
from airflow.models import Log

event_logs_router = AirflowRouter(tags=["Event Log"], prefix="/eventLogs")


@event_logs_router.get(
    "/{event_log_id}",
    responses=create_openapi_http_exception_doc([status.HTTP_404_NOT_FOUND]),
    dependencies=[Depends(requires_access_dag("GET", DagAccessEntity.AUDIT_LOG))],
)
def get_event_log(
    event_log_id: int,
    session: SessionDep,
) -> EventLogResponse:
    event_log = session.scalar(
        select(Log).where(Log.id == event_log_id).options(joinedload(Log.task_instance))
    )
    if event_log is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, f"The Event Log with id: `{event_log_id}` not found")
    return event_log


@event_logs_router.get(
    "",
    dependencies=[Depends(requires_access_dag("GET", DagAccessEntity.AUDIT_LOG))],
)
def get_event_logs(
    limit: QueryLimit,
    offset: QueryOffset,
    session: SessionDep,
    order_by: Annotated[
        SortParam,
        Depends(
            SortParam(
                [
                    "id",  # event_log_id
                    "dttm",  # when
                    "dag_id",
                    "task_id",
                    "run_id",
                    "event",
                    "logical_date",
                    "owner",
                    "extra",
                ],
                Log,
                to_replace={"when": "dttm", "event_log_id": "id"},
            ).dynamic_depends()
        ),
    ],
    # Exact match filters (for backward compatibility)
    dag_id: Annotated[FilterParam[str | None], Depends(filter_param_factory(Log.dag_id, str | None))],
    task_id: Annotated[FilterParam[str | None], Depends(filter_param_factory(Log.task_id, str | None))],
    run_id: Annotated[FilterParam[str | None], Depends(filter_param_factory(Log.run_id, str | None))],
    map_index: Annotated[FilterParam[int | None], Depends(filter_param_factory(Log.map_index, int | None))],
    try_number: Annotated[FilterParam[int | None], Depends(filter_param_factory(Log.try_number, int | None))],
    owner: Annotated[FilterParam[str | None], Depends(filter_param_factory(Log.owner, str | None))],
    event: Annotated[FilterParam[str | None], Depends(filter_param_factory(Log.event, str | None))],
    excluded_events: Annotated[
        FilterParam[list[str] | None],
        Depends(
            filter_param_factory(Log.event, list[str] | None, FilterOptionEnum.NOT_IN, "excluded_events")
        ),
    ],
    included_events: Annotated[
        FilterParam[list[str] | None],
        Depends(filter_param_factory(Log.event, list[str] | None, FilterOptionEnum.IN, "included_events")),
    ],
    before: Annotated[
        FilterParam[datetime | None],
        Depends(filter_param_factory(Log.dttm, datetime | None, FilterOptionEnum.LESS_THAN, "before")),
    ],
    after: Annotated[
        FilterParam[datetime | None],
        Depends(filter_param_factory(Log.dttm, datetime | None, FilterOptionEnum.GREATER_THAN, "after")),
    ],
    # Pattern search filters (new - for partial matching)
    dag_id_pattern: Annotated[_SearchParam, Depends(search_param_factory(Log.dag_id, "dag_id_pattern"))],
    task_id_pattern: Annotated[_SearchParam, Depends(search_param_factory(Log.task_id, "task_id_pattern"))],
    run_id_pattern: Annotated[_SearchParam, Depends(search_param_factory(Log.run_id, "run_id_pattern"))],
    owner_pattern: Annotated[_SearchParam, Depends(search_param_factory(Log.owner, "owner_pattern"))],
    event_pattern: Annotated[_SearchParam, Depends(search_param_factory(Log.event, "event_pattern"))],
) -> EventLogCollectionResponse:
    """Get all Event Logs."""
    query = select(Log).options(joinedload(Log.task_instance), joinedload(Log.dag_model))
    event_logs_select, total_entries = paginated_select(
        statement=query,
        order_by=order_by,
        filters=[
            # Exact match filters
            dag_id,
            task_id,
            run_id,
            map_index,
            try_number,
            owner,
            event,
            excluded_events,
            included_events,
            before,
            after,
            # Pattern search filters
            dag_id_pattern,
            task_id_pattern,
            run_id_pattern,
            owner_pattern,
            event_pattern,
        ],
        offset=offset,
        limit=limit,
        session=session,
    )
    event_logs = session.scalars(event_logs_select)

    return EventLogCollectionResponse(
        event_logs=event_logs,
        total_entries=total_entries,
    )
