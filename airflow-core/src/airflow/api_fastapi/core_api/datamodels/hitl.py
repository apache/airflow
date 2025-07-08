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

from collections.abc import Mapping
from datetime import datetime
from typing import Any

from pydantic import Field, field_validator

from airflow.api_fastapi.core_api.base import BaseModel
from airflow.sdk import Param


class UpdateHITLResponsePayload(BaseModel):
    """Schema for updating the content of a Human-in-the-loop response."""

    response_content: list[str]
    params_input: Mapping = Field(default_factory=dict)


class HITLResponseContentDetail(BaseModel):
    """Response of updating a Human-in-the-loop response."""

    user_id: str
    response_at: datetime
    response_content: list[str]
    params_input: Mapping = Field(default_factory=dict)


class HITLResponseDetail(BaseModel):
    """Schema for Human-in-the-loop response."""

    ti_id: str

    # Input Request
    options: list[str]
    subject: str
    body: str | None = None
    default: list[str] | None = None
    multiple: bool = False
    params: dict[str, Any] = Field(default_factory=dict)

    # Response Content Detail
    user_id: str | None = None
    response_at: datetime | None = None
    response_content: list[str] | None = None
    params_input: dict[str, Any] = Field(default_factory=dict)

    response_received: bool = False

    @field_validator("params", mode="before")
    @classmethod
    def get_params(cls, params: dict[str, Any]) -> dict[str, Any]:
        """Convert params attribute to dict representation."""
        return {k: v.dump() if isinstance(v, Param) else v for k, v in params.items()}


class HITLResponseDetailCollection(BaseModel):
    """Schema for a collection of Human-in-the-loop responses."""

    hitl_responses: list[HITLResponseDetail]
    total_entries: int


class HITLSharedLinkRequest(BaseModel):
    """Schema for requesting a HITL shared link."""

    link_type: str = Field(
        default="action",
        description="Type of link to generate: 'action' for direct action or 'redirect' for UI interaction",
    )
    action: str | None = Field(
        default=None,
        description="Optional action to perform when link is accessed (e.g., 'approve', 'reject'). Required for action links.",
    )
    expires_in_hours: int | None = Field(default=None, description="Optional custom expiration time in hours")


class HITLSharedLinkResponse(BaseModel):
    """Schema for HITL shared link response."""

    task_instance_id: str
    link_url: str
    expires_at: datetime
    action: str | None = None
    link_type: str = "action"


class HITLSharedLinkActionRequest(BaseModel):
    """Schema for HITL shared link action request."""

    response_content: list[str]
    params_input: Mapping = Field(default_factory=dict)
