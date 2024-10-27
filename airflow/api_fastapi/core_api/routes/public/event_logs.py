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

from fastapi import Depends, HTTPException, Query, status
from sqlalchemy import select
from sqlalchemy.orm import Session
from typing_extensions import Annotated

from airflow.api_fastapi.common.db.common import (
    get_session,
    paginated_select,
)
from airflow.api_fastapi.common.parameters import (
    FilterParam,
    QueryLimit,
    QueryOffset,
    SortParam,
)
from airflow.api_fastapi.common.router import AirflowRouter
from airflow.api_fastapi.core_api.openapi.exceptions import create_openapi_http_exception_doc
from airflow.api_fastapi.core_api.serializers.event_logs import (
    EventLogCollectionResponse,
    EventLogResponse,
)
from airflow.models import Log

event_logs_router = AirflowRouter(tags=["Event Log"], prefix="/eventLogs")


@event_logs_router.get(
    "/{event_log_id}",
    responses=create_openapi_http_exception_doc(
        [status.HTTP_401_UNAUTHORIZED, status.HTTP_403_FORBIDDEN, status.HTTP_404_NOT_FOUND]
    ),
)
async def get_event_log(
    event_log_id: int,
    session: Annotated[Session, Depends(get_session)],
) -> EventLogResponse:
    event_log = session.scalar(select(Log).where(Log.id == event_log_id))
    if event_log is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, f"The Event Log with id: `{event_log_id}` not found")
    return EventLogResponse.model_validate(
        event_log,
        from_attributes=True,
    )


@event_logs_router.get("/")
async def get_event_logs(
    limit: QueryLimit,
    offset: QueryOffset,
    session: Annotated[Session, Depends(get_session)],
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
                    "execution_date",  # logical_date
                    "owner",
                    "extra",
                ],
                Log,
            ).dynamic_depends()
        ),
    ],
    dag_id: str | None = None,
    task_id: str | None = None,
    run_id: str | None = None,
    map_index: int | None = None,
    try_number: int | None = None,
    owner: str | None = None,
    event: str | None = None,
    excluded_events: list[str] | None = Query(None),
    included_events: list[str] | None = Query(None),
    before: datetime | None = None,
    after: datetime | None = None,
) -> EventLogCollectionResponse:
    """Get all Event Logs."""
    base_select = select(Log).group_by(Log.id)
    event_logs_select, total_entries = paginated_select(
        base_select,
        [
            FilterParam(Log.dag_id, dag_id),
            FilterParam(Log.task_id, task_id),
            FilterParam(Log.run_id, run_id),
            FilterParam(Log.map_index, map_index),
            FilterParam(Log.event, event),
            FilterParam(Log.try_number, try_number),
            FilterParam(Log.owner, owner),
            FilterParam(Log.event, excluded_events, "not_in"),
            FilterParam(Log.event, included_events, "in"),
            FilterParam(Log.dttm, before, "lt"),
            FilterParam(Log.dttm, after, "gt"),
        ],
        order_by,
        offset,
        limit,
        session,
    )
    event_logs = session.scalars(event_logs_select).all()

    return EventLogCollectionResponse(
        event_logs=[
            EventLogResponse.model_validate(
                event_log,
                from_attributes=True,
            )
            for event_log in event_logs
        ],
        total_entries=total_entries,
    )
