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


class ActionResponse(BaseModel):
    """Outgoing representation of an action (permission name)."""

    name: str


class ResourceResponse(BaseModel):
    """Outgoing representation of a resource."""

    name: str


class ActionResourceResponse(BaseModel):
    """Pairing of an action with a resource."""

    action: ActionResponse
    resource: ResourceResponse


class RoleBody(StrictBaseModel):
    """Incoming payload for creating/updating a role."""

    name: str = Field(min_length=1)
    permissions: list[ActionResourceResponse] = Field(
        default_factory=list, alias="actions", validation_alias="actions"
    )


class RoleResponse(BaseModel):
    """Outgoing representation of a role and its permissions."""

    name: str
    permissions: list[ActionResourceResponse] = Field(default_factory=list, serialization_alias="actions")


class RoleCollectionResponse(BaseModel):
    """Outgoing representation of a paginated collection of roles."""

    roles: list[RoleResponse]
    total_entries: int
