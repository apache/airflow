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

from pydantic import BaseModel, field_validator


class XComResponse(BaseModel):
    """Serializer for a xcom item."""

    key: str
    timestamp: datetime
    logical_date: datetime
    map_index: int
    task_id: str
    dag_id: str


class XComResponseNative(XComResponse):
    """XCom response serializer with native return type."""

    value: Any


class XComResponseString(XComResponse):
    """XCom response serializer with string return type."""

    value: str | None

    @field_validator("value", mode="before")
    def value_to_string(cls, v):
        return str(v) if v is not None else None
