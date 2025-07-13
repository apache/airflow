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
from typing import Any
from uuid import UUID

from pydantic import Field

from airflow.api_fastapi.core_api.base import BaseModel
from airflow.models.hitl import HITLDetail


class HITLDetailRequest(BaseModel):
    """Schema for the request part of a Human-in-the-loop detail for a specific task instance."""

    ti_id: UUID
    options: list[str]
    subject: str
    body: str | None = None
    defaults: list[str] | None = None
    multiple: bool = False
    params: dict[str, Any] = Field(default_factory=dict)


class GetHITLDetailResponsePayload(BaseModel):
    """Schema for getting the response part of a Human-in-the-loop detail for a specific task instance."""

    ti_id: UUID


class UpdateHITLDetailPayload(BaseModel):
    """Schema for writing the response part of a Human-in-the-loop detail for a specific task instance."""

    ti_id: UUID
    chosen_options: list[str]
    params_input: dict[str, Any] = Field(default_factory=dict)


class HITLDetailResponse(BaseModel):
    """Schema for the response part of a Human-in-the-loop detail for a specific task instance."""

    response_received: bool
    user_id: str | None
    response_at: datetime | None
    chosen_options: list[str] | None
    params_input: dict[str, Any] = Field(default_factory=dict)

    @classmethod
    def from_hitl_detail_orm(cls, hitl_detail: HITLDetail) -> HITLDetailResponse:
        return HITLDetailResponse(
            response_received=hitl_detail.response_received,
            response_at=hitl_detail.response_at,
            user_id=hitl_detail.user_id,
            chosen_options=hitl_detail.chosen_options,
            params_input=hitl_detail.params_input or {},
        )
