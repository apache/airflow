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
from airflow.api_fastapi.core_api.datamodels.task_instances import TaskInstanceResponse
from airflow.sdk import Param


class HITLSharedLinkFields(BaseModel):
    """Common shared link fields for HITL models."""

    link_type: str = Field(
        default="action",
        description="Type of link to generate: 'action' for direct action or 'redirect' for UI interaction",
    )
    action: str | None = Field(
        default=None,
        description="Optional action to perform when link is accessed (e.g., 'approve', 'reject'). Required for action links.",
    )
    expires_in_hours: int | None = Field(default=None, description="Optional custom expiration time in hours")


class HITLSharedLinkPayload(HITLSharedLinkFields, BaseModel):
    """Payload data for HITL shared links containing task and link metadata."""

    dag_id: str
    dag_run_id: str
    task_id: str
    try_number: int
    map_index: int | None = None
    expires_at: str


class UpdateHITLDetailPayload(HITLSharedLinkFields, BaseModel):
    """Schema for updating the content of a Human-in-the-loop detail."""

    chosen_options: list[str]
    params_input: Mapping = Field(default_factory=dict)
    try_number: int = Field(default=1, description="Try number for the task")


class HITLDetailResponse(BaseModel):
    """Response of updating a Human-in-the-loop detail."""

    user_id: str
    response_at: datetime
    chosen_options: list[str]
    params_input: Mapping = Field(default_factory=dict)

    # Shared link response fields
    task_instance_id: str | None = None
    link_url: str | None = None
    expires_at: datetime | None = None
    action: str | None = None
    link_type: str = "action"


class HITLDetail(HITLSharedLinkFields, BaseModel):
    """Schema for Human-in-the-loop detail."""

    task_instance: TaskInstanceResponse

    # User Request Detail
    options: list[str]
    subject: str
    body: str | None = None
    defaults: list[str] | None = None
    multiple: bool = False
    params: dict[str, Any] = Field(default_factory=dict)

    # Response Content Detail
    user_id: str | None = None
    response_at: datetime | None = None
    chosen_options: list[str] | None = None
    params_input: dict[str, Any] = Field(default_factory=dict)

    response_received: bool = False
    link_url: str | None = None
    expires_at: datetime | None = None

    @field_validator("params", mode="before")
    @classmethod
    def get_params(cls, params: dict[str, Any]) -> dict[str, Any]:
        """Convert params attribute to dict representation."""
        return {k: v.dump() if isinstance(v, Param) else v for k, v in params.items()}


class HITLDetailCollection(BaseModel):
    """Schema for a collection of Human-in-the-loop details."""

    hitl_details: list[HITLDetail]
    total_entries: int

    # Shared link action request fields
    response_content: list[str] | None = None
    params_input: Mapping = Field(default_factory=dict)
