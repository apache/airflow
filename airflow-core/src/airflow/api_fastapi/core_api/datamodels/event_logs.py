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

from pydantic import AliasPath, Field

from airflow.api_fastapi.core_api.base import BaseModel


class EventLogResponse(BaseModel):
    """Event Log Response."""

    id: int = Field(alias="event_log_id")
    dttm: datetime = Field(alias="when")
    dag_id: str | None
    task_id: str | None
    run_id: str | None
    map_index: int | None
    try_number: int | None
    event: str
    logical_date: datetime | None
    owner: str | None
    extra: str | None
    dag_display_name: str | None = Field(
        validation_alias=AliasPath("dag_model", "dag_display_name"), default=None
    )
    task_display_name: str | None = Field(
        validation_alias=AliasPath("task_instance", "task_display_name"), default=None
    )


class EventLogCollectionResponse(BaseModel):
    """Event Log Collection Response."""

    event_logs: list[EventLogResponse]
    total_entries: int
