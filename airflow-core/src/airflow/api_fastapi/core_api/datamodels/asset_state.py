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

import json
import math
from datetime import datetime

from pydantic import JsonValue, field_validator

from airflow.api_fastapi.core_api.base import BaseModel, StrictBaseModel

_MAX_SERIALIZED_BYTES = 65535


class AssetStateResponse(BaseModel):
    """A single asset state key/value pair with metadata."""

    key: str
    value: JsonValue
    updated_at: datetime


class AssetStateCollectionResponse(BaseModel):
    """All asset state entries for an asset."""

    asset_states: list[AssetStateResponse]
    total_entries: int


class AssetStateBody(StrictBaseModel):
    """Request body for setting an asset state value."""

    value: JsonValue

    @field_validator("value")
    @classmethod
    def value_is_json_representable(cls, v: JsonValue) -> JsonValue:
        if v is None:
            raise ValueError("value cannot be null")
        if isinstance(v, float) and not math.isfinite(v):
            raise ValueError("value must be a finite number; NaN and Inf are not JSON representable")
        if len(json.dumps(v)) > _MAX_SERIALIZED_BYTES:
            raise ValueError(f"value exceeds maximum serialized size of {_MAX_SERIALIZED_BYTES} bytes")
        return v
