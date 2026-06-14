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

import math

from pydantic import JsonValue, field_validator

from airflow.api_fastapi.core_api.base import StrictBaseModel


class AssetStateStoreResponse(StrictBaseModel):
    """Asset state store value returned to a worker."""

    value: JsonValue


class AssetStateStorePutBody(StrictBaseModel):
    """Request body for setting an asset state store value."""

    value: JsonValue

    @field_validator("value")
    @classmethod
    def value_is_json_representable(cls, v: JsonValue) -> JsonValue:
        if v is None:
            raise ValueError("value cannot be null")
        if isinstance(v, float) and not math.isfinite(v):
            raise ValueError("value must be a finite number; NaN and Inf are not JSON representable")
        return v
