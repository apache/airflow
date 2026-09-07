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

from collections.abc import Iterable, Mapping
from datetime import datetime
from typing import Any

from pydantic import Field, field_validator

from airflow._shared.serialization import SERDE_RESERVED_DICT_KEYS
from airflow.api_fastapi.core_api.base import BaseModel
from airflow.api_fastapi.core_api.datamodels.common import find_reserved_keys
from airflow.api_fastapi.core_api.datamodels.task_instance_history import TaskInstanceHistoryResponse
from airflow.api_fastapi.core_api.datamodels.task_instances import TaskInstanceResponse


class UpdateHITLDetailPayload(BaseModel):
    """Schema for updating the content of a Human-in-the-loop detail."""

    chosen_options: list[str] = Field(min_length=1)
    params_input: Mapping = Field(default_factory=dict)

    @field_validator("params_input")
    @classmethod
    def _check_serde_reserved_keys(cls, params_input: Mapping) -> Mapping:
        # serde.serialize refuses these keys, and it only runs once the task resumes, long after the
        # response was stored and the request returned. Reject it here instead, while the user can
        # still correct the input and resubmit.
        found = find_reserved_keys(params_input, SERDE_RESERVED_DICT_KEYS, root="params_input")
        if found is not None:
            path, keys = found
            raise ValueError(
                f"{path} contains reserved serialization keys: {', '.join(keys)}. "
                "These keys are reserved for internal use."
            )
        return params_input


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


class BaseHITLDetail(BaseModel):
    """The common part within HITLDetail and HITLDetailHistory."""

    # User Request Detail
    options: list[str] = Field(min_length=1)
    subject: str
    body: str | None = None
    defaults: list[str] | None = None
    multiple: bool = False
    params: Mapping = Field(default_factory=dict)
    assigned_users: list[HITLUser] = Field(default_factory=list)
    created_at: datetime

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
        return {
            key: value
            if BaseHITLDetail._is_param(value)
            else {
                "value": value,
                "description": None,
                "schema": {},
            }
            for key, value in params.items()
        }

    @staticmethod
    def _is_param(value: Any) -> bool:
        return isinstance(value, dict) and all(key in value for key in ("description", "schema", "value"))


class HITLDetail(BaseHITLDetail):
    """Schema for Human-in-the-loop detail."""

    task_instance: TaskInstanceResponse


class HITLDetailCollection(BaseModel):
    """Schema for a collection of Human-in-the-loop details."""

    hitl_details: Iterable[HITLDetail]
    total_entries: int


class HITLDetailHistory(BaseHITLDetail):
    """Schema for Human-in-the-loop detail history."""

    task_instance: TaskInstanceHistoryResponse
