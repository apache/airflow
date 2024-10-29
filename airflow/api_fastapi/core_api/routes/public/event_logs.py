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

from fastapi import Depends, HTTPException
from sqlalchemy import select
from sqlalchemy.orm import Session
from typing_extensions import Annotated

from airflow.api_fastapi.common.db.common import (
    get_session,
)
from airflow.api_fastapi.common.router import AirflowRouter
from airflow.api_fastapi.core_api.openapi.exceptions import create_openapi_http_exception_doc
from airflow.api_fastapi.core_api.serializers.event_logs import (
    EventLogResponse,
)
from airflow.models import Log

event_logs_router = AirflowRouter(tags=["Event Log"], prefix="/eventLogs")


@event_logs_router.get(
    "/{event_log_id}",
    responses=create_openapi_http_exception_doc([401, 403, 404]),
)
async def get_event_log(
    event_log_id: int,
    session: Annotated[Session, Depends(get_session)],
) -> EventLogResponse:
    event_log = session.scalar(select(Log).where(Log.id == event_log_id))
    if event_log is None:
        raise HTTPException(404, f"The Event Log with id: `{event_log_id}` not found")
    return EventLogResponse.model_validate(
        event_log,
        from_attributes=True,
    )
