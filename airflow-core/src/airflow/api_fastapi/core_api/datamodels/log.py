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
from typing import Annotated

from pydantic import ConfigDict, WithJsonSchema

from airflow.api_fastapi.core_api.base import BaseModel


class StructuredLogMessage(BaseModel):
    """An individual log message."""

    # Not every message has a timestamp.
    timestamp: Annotated[
        datetime | None,
        # Schema level, say this is always a datetime if it exists
        WithJsonSchema({"type": "string", "format": "date-time"}),
    ] = None
    event: str

    model_config = ConfigDict(extra="allow", from_attributes=True)


class TaskInstancesLogResponse(BaseModel):
    """Log serializer for responses."""

    content: list[StructuredLogMessage] | list[str]
    """Either a list of parsed events, or a list of lines on parse error"""
    continuation_token: str | None


class ExternalLogUrlResponse(BaseModel):
    """Response for the external log URL endpoint."""

    url: str
