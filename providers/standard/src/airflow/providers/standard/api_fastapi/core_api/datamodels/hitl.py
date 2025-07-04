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


class UpdateHITLResponsePayload(BaseModel):
    """Schema for updating the content of a Human-in-the-loop response."""

    response_content: str


class HITLResponseContentDetail(BaseModel):
    """Response of updating a Human-in-the-loop response."""

    response_content: str
    response_at: datetime
    user_id: str


class HITLResponseDetail(BaseModel):
    """Schema for Human-in-the-loop response."""

    ti_id: str

    # Input Request
    options: list[str]
    subject: str
    body: str | None = None
    default: list[str] | None = None
    multiple: bool = False
    params: MutableMapping | None = None

    # Response Content Detail
    response_at: datetime | None = None
    user_id: str | None = None
    response_content: str | None = None
    params_input: MutableMapping | None = None

    response_received: bool = False

    @field_validator("params", mode="before")
    @classmethod
    def get_params(cls, params: MutableMapping | None) -> dict | None:
        """Convert params attribute to dict representation."""
        if params is None:
            return None
        return {k: v.dump() for k, v in params.items()}


class HITLResponseDetailCollection(BaseModel):
    """Schema for a collection of Human-in-the-loop responses."""

    hitl_responses: list[HITLResponseDetail]
    total_entries: int
