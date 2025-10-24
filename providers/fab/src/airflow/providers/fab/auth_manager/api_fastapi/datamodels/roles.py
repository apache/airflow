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

from pydantic import Field

from airflow.api_fastapi.core_api.base import BaseModel, StrictBaseModel


class ActionOut(BaseModel):
    """Outgoing representation of an action (permission name)."""

    name: str


class ResourceOut(BaseModel):
    """Outgoing representation of a resource."""

    name: str


class ActionResourceOut(BaseModel):
    """Pairing of an action with a resource."""

    action: ActionOut
    resource: ResourceOut


class RoleIn(StrictBaseModel):
    """Incoming payload for creating/updating a role."""

    name: str
    permissions: list[ActionResourceOut] = Field(
        default_factory=list, alias="actions", validation_alias="actions"
    )


class RoleOut(BaseModel):
    """Outgoing representation of a role and its permissions."""

    name: str
    permissions: list[ActionResourceOut] = Field(default_factory=list, serialization_alias="actions")


class RoleCollectionOut(BaseModel):
    """Collection wrapper for roles with total count."""

    roles: list[RoleOut]
    total_entries: int
