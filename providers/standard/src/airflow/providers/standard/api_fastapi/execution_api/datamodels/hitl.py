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
from uuid import UUID

from airflow.api_fastapi.core_api.base import BaseModel


class HITLInputRequestResponse(BaseModel):
    """Schema for a Human-in-the-loop input request for a specific task instance."""

    ti_id: UUID
    options: list[str]
    subject: str
    body: str | None = None
    default: list[str] | None = None
    multiple: bool = False

    params: MutableMapping | None = None
    form_content: MutableMapping | None = None


class GetHITLResponseContentDetailPayload(BaseModel):
    """Schema for fetching a Human-in-the-loop response content detail for a specific task instance."""

    ti_id: UUID


class HITLResponseContentDetail(BaseModel):
    """Schema for Human-in-the-loop response content detail for a specific task instance."""

    response_received: bool
    response_at: datetime | None
    user_id: str | None
    response_content: str | None
