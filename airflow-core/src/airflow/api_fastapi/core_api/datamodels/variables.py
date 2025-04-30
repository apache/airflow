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

from pydantic import Field, JsonValue, model_validator

from airflow.api_fastapi.core_api.base import BaseModel, StrictBaseModel
from airflow.models.base import ID_LEN
from airflow.sdk.execution_time.secrets_masker import redact
from airflow.typing_compat import Self


class VariableResponse(BaseModel):
    """Variable serializer for responses."""

    key: str
    val: str = Field(alias="value")
    description: str | None
    is_encrypted: bool

    @model_validator(mode="after")
    def redact_val(self) -> Self:
        if self.val is None:
            return self
        try:
            val_dict = json.loads(self.val)
            redacted_dict = redact(val_dict, max_depth=1)
            self.val = json.dumps(redacted_dict)
            return self
        except json.JSONDecodeError:
            # value is not a serialized string representation of a dict.
            self.val = redact(self.val, self.key)
            return self


class VariableBody(StrictBaseModel):
    """Variable serializer for bodies."""

    key: str = Field(max_length=ID_LEN)
    value: JsonValue = Field(serialization_alias="val")
    description: str | None = Field(default=None)


class VariableCollectionResponse(BaseModel):
    """Variable Collection serializer for responses."""

    variables: list[VariableResponse]
    total_entries: int


class VariablesImportResponse(BaseModel):
    """Import Variables serializer for responses."""

    created_variable_keys: list[str]
    import_count: int
    created_count: int
