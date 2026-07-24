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

from sqlalchemy import inspect
from sqlalchemy.orm.attributes import set_committed_value

from airflow.api_fastapi.auth.managers.models.base_user import BaseUser
from airflow.api_fastapi.core_api.datamodels.event_logs import EventLogResponse
from airflow.models import Log


def _resolve_owner_display_name(event_log: Log, user: BaseUser | None) -> str | None:
    if event_log.owner_display_name:
        return event_log.owner_display_name
    if not event_log.owner:
        return None
    if user is not None and event_log.owner == user.get_name():
        return user.get_name()
    return event_log.owner


def _prepare_event_log_for_response(event_log: Log, *, owner_display_name: str | None) -> None:
    # Response-only fallbacks must not make the ORM object dirty at request commit time.
    set_committed_value(event_log, "owner_display_name", owner_display_name)

    unloaded: set[str] = inspect(event_log).unloaded
    for relationship_name in ("dag_model", "task_instance"):
        if relationship_name in unloaded:
            set_committed_value(event_log, relationship_name, None)


def event_log_to_response(event_log: Log, user: BaseUser | None) -> EventLogResponse:
    _prepare_event_log_for_response(
        event_log,
        owner_display_name=_resolve_owner_display_name(event_log, user),
    )
    return EventLogResponse.model_validate(event_log)
