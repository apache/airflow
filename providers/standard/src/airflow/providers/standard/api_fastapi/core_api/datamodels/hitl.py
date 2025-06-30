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

from collections.abc import MutableMapping
from datetime import datetime

from pydantic import field_validator

from airflow.api_fastapi.core_api.base import BaseModel


class AddHITLResponsePayload(BaseModel):
    """Schema for adding an Human-in-the-loop Response for a specific Task Instance."""

    content: str


class HITLResponse(BaseModel):
    """Schema for added HITLResponse."""

    input_request_id: str
    content: str
    created_at: datetime
    user_id: str


class HITLInputRequest(BaseModel):
    """Schema for details for that awaiting Human-in-the-loop input request."""

    ti_id: str

    options: list[str]
    subject: str
    body: str | None = None
    default: str | None = None
    params: MutableMapping | None = None

    @field_validator("params", mode="before")
    @classmethod
    def get_params(cls, params: MutableMapping | None) -> dict | None:
        """Convert params attribute to dict representation."""
        if params is None:
            return None
        return {k: v.dump() for k, v in params.items()}


class HITLInputRequestCollection(BaseModel):
    """Schema for a collection details for that awaiting Human-in-the-loop input request."""

    hitl_input_requests: list[HITLInputRequest]
    total_entries: int
