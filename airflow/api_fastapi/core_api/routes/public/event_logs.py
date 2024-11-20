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

from fastapi import Depends, HTTPException, Query, status
from sqlalchemy import select
from sqlalchemy.orm import Session

from airflow.api_fastapi.common.db.common import (
    get_session,
    paginated_select,
)
from airflow.api_fastapi.common.parameters import (
    QueryLimit,
    QueryOffset,
    SortParam,
)
from airflow.api_fastapi.common.router import AirflowRouter
from airflow.api_fastapi.core_api.datamodels.event_logs import (
    EventLogCollectionResponse,
    EventLogResponse,
)
from airflow.api_fastapi.core_api.openapi.exceptions import create_openapi_http_exception_doc
from airflow.models import Log

event_logs_router = AirflowRouter(tags=["Event Log"], prefix="/eventLogs")


@event_logs_router.get(
    "/{event_log_id}",
    responses=create_openapi_http_exception_doc([status.HTTP_404_NOT_FOUND]),
)
def get_event_log(
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


@event_logs_router.get(
    "",
)
def get_event_logs(
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
                    "logical_date",
                    "owner",
                    "extra",
                ],
                Log,
                to_replace={"when": "dttm", "event_log_id": "id"},
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
    # TODO: Refactor using the `FilterParam` class in commit `574b72e41cc5ed175a2bbf4356522589b836bb11`
    if dag_id is not None:
        base_select = base_select.where(Log.dag_id == dag_id)
    if task_id is not None:
        base_select = base_select.where(Log.task_id == task_id)
    if run_id is not None:
        base_select = base_select.where(Log.run_id == run_id)
    if map_index is not None:
        base_select = base_select.where(Log.map_index == map_index)
    if try_number is not None:
        base_select = base_select.where(Log.try_number == try_number)
    if owner is not None:
        base_select = base_select.where(Log.owner == owner)
    if event is not None:
        base_select = base_select.where(Log.event == event)
    if excluded_events is not None:
        base_select = base_select.where(Log.event.notin_(excluded_events))
    if included_events is not None:
        base_select = base_select.where(Log.event.in_(included_events))
    if before is not None:
        base_select = base_select.where(Log.dttm < before)
    if after is not None:
        base_select = base_select.where(Log.dttm > after)
    event_logs_select, total_entries = paginated_select(
        base_select,
        [],
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
