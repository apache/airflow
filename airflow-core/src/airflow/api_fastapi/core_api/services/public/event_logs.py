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

from sqlalchemy.orm.attributes import set_committed_value

from airflow.api_fastapi.auth.managers.models.base_user import BaseUser
from airflow.api_fastapi.core_api.datamodels.event_logs import EventLogResponse
from airflow.models import Log


def get_user_display_name(user: BaseUser) -> str:
    """Return the most readable identity label available for audit logs."""
    first_name = (getattr(user, "first_name", None) or "").strip()
    last_name = (getattr(user, "last_name", None) or "").strip()
    email = (getattr(user, "email", None) or "").strip()
    username = user.get_name()

    if first_name and last_name and last_name != "-":
        return f"{first_name} {last_name}"
    if email:
        return email
    return username


def resolve_owner_display_name(event_log: Log, user: BaseUser | None) -> str | None:
    if event_log.owner_display_name:
        return event_log.owner_display_name
    if not event_log.owner:
        return None
    if user is not None and event_log.owner == user.get_name():
        return get_user_display_name(user)
    return event_log.owner


def event_log_to_response(event_log: Log, user: BaseUser | None) -> EventLogResponse:
    owner_display_name = resolve_owner_display_name(event_log, user)
    # API sessions commit at request end; expose the read-time fallback without marking the row dirty.
    set_committed_value(event_log, "owner_display_name", owner_display_name)
    return EventLogResponse.model_validate(event_log)
