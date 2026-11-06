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
from datetime import datetime
from typing import Literal

from pydantic import AwareDatetime, JsonValue, field_validator

from airflow.api_fastapi.core_api.base import BaseModel, StrictBaseModel
from airflow.configuration import conf


class TaskStateStoreResponse(BaseModel):
    """A single task state store key/value pair with metadata."""

    key: str
    value: JsonValue
    updated_at: datetime
    expires_at: datetime | None


class TaskStateStoreCollectionResponse(BaseModel):
    """All task state store entries for a task instance."""

    task_state_store: list[TaskStateStoreResponse]
    total_entries: int


class TaskStateStoreBody(StrictBaseModel):
    """
    Request body for setting a task state store value.

    ``expires_at`` controls expiry:

    - ``"default"``: apply the configured ``[state_store] default_retention_days``.
    - ``null``: never expire.
    - aware datetime: expire at that time.
    """

    value: JsonValue
    expires_at: AwareDatetime | None | Literal["default"] = "default"

    @field_validator("value")
    @classmethod
    def value_is_json_representable(cls, v: JsonValue) -> JsonValue:
        if v is None:
            raise ValueError("value cannot be null")
        try:
            serialized = json.dumps(v, allow_nan=False)
        except ValueError:
            raise ValueError("value contains non-finite numbers; NaN and Inf are not JSON representable")
        limit = conf.getint("state_store", "max_value_storage_bytes")
        if limit > 0 and len(serialized) > limit:
            raise ValueError(
                f"value exceeds max_value_storage_bytes ({limit}); "
                "raise [state_store] max_value_storage_bytes or set it to 0 to disable the limit"
            )
        return v


class TaskStateStorePatchBody(StrictBaseModel):
    """Request body for patching only the value of an existing task state store key."""

    value: JsonValue

    @field_validator("value")
    @classmethod
    def value_is_json_representable(cls, v: JsonValue) -> JsonValue:
        if v is None:
            raise ValueError("value cannot be null")
        try:
            serialized = json.dumps(v, allow_nan=False)
        except ValueError:
            raise ValueError("value contains non-finite numbers; NaN and Inf are not JSON representable")
        limit = conf.getint("state_store", "max_value_storage_bytes")
        if limit > 0 and len(serialized) > limit:
            raise ValueError(
                f"value exceeds max_value_storage_bytes ({limit}); "
                "raise [state_store] max_value_storage_bytes or set it to 0 to disable the limit"
            )
        return v
