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


class UpdateHITLDetailPayload(BaseModel):
    """Schema for updating the content of a Human-in-the-loop detail."""

    chosen_options: list[str] = Field(min_length=1)
    params_input: Mapping = Field(default_factory=dict)


class HITLDetailResponse(BaseModel):
    """Response of updating a Human-in-the-loop detail."""

    responded_by: HITLUser
    responded_at: datetime
    chosen_options: list[str] = Field(min_length=1)
    params_input: Mapping = Field(default_factory=dict)


class HITLUser(BaseModel):
    """Schema for a Human-in-the-loop users."""

    id: str
    name: str


class HITLDetail(BaseModel):
    """Schema for Human-in-the-loop detail."""

    task_instance: TaskInstanceResponse

    # User Request Detail
    options: list[str] = Field(min_length=1)
    subject: str
    body: str | None = None
    defaults: list[str] | None = None
    multiple: bool = False
    params: dict[str, Any] = Field(default_factory=dict)
    assigned_users: list[HITLUser] = Field(default_factory=list)

    # Response Content Detail
    responded_by_user: HITLUser | None = None
    responded_at: datetime | None = None
    chosen_options: list[str] | None = None
    params_input: dict[str, Any] = Field(default_factory=dict)

    response_received: bool = False

    @field_validator("params", mode="before")
    @classmethod
    def get_params(cls, params: dict[str, Any]) -> dict[str, Any]:
        """Convert params attribute to dict representation."""
        return {k: v.dump() if isinstance(v, Param) else v for k, v in params.items()}


class HITLDetailCollection(BaseModel):
    """Schema for a collection of Human-in-the-loop details."""

    hitl_details: list[HITLDetail]
    total_entries: int
