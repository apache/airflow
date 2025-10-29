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

from datetime import datetime, timezone
from typing import TYPE_CHECKING

from pydantic import Field, SecretStr, field_validator

from airflow.api_fastapi.core_api.base import BaseModel, StrictBaseModel
from airflow.providers.fab.auth_manager.api_fastapi.datamodels.roles import RoleRef

if TYPE_CHECKING:
    from airflow.providers.fab.auth_manager.api_fastapi.datamodels.roles import RoleRef


class UserBody(StrictBaseModel):
    """Incoming payload for creating a user."""

    username: str = Field(min_length=1)
    email: str = Field(min_length=1)
    first_name: str = Field(min_length=1)
    last_name: str = Field(min_length=1)
    roles: list[RoleRef] | None = None
    password: SecretStr


class UserResponse(BaseModel):
    """Outgoing representation of a user (no password)."""

    username: str
    email: str
    first_name: str
    last_name: str
    roles: list[RoleRef] | None = None
    active: bool | None = None
    last_login: datetime | None = None
    login_count: int | None = None
    fail_login_count: int | None = None
    created_on: datetime | None = None
    changed_on: datetime | None = None

    @field_validator("created_on", "changed_on")
    @classmethod
    def _coerce_tzaware(cls, v: datetime | None) -> datetime | None:
        if v is None:
            return None
        return v if (v.tzinfo and v.utcoffset() is not None) else v.replace(tzinfo=timezone.utc)
