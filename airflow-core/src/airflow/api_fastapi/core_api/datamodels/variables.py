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
from collections.abc import Iterable

from pydantic import Field, JsonValue, field_validator, model_validator

from airflow._shared.secrets_masker import redact
from airflow.api_fastapi.core_api.base import BaseModel, StrictBaseModel, make_partial_model
from airflow.configuration import conf
from airflow.models.base import ID_LEN
from airflow.typing_compat import Self


class VariableResponse(BaseModel):
    """Variable serializer for responses."""

    key: str
    val: str | None = Field(alias="value", default=None)
    description: str | None
    is_encrypted: bool
    team_name: str | None

    @model_validator(mode="after")
    def redact_val(self) -> Self:
        if self.val is None:
            return self
        try:
            val_dict = json.loads(self.val)
            redacted_dict = redact(val_dict, self.key)
            self.val = json.dumps(redacted_dict)
            return self
        except json.JSONDecodeError:
            # value is not a serialized string representation of a dict.
            self.val = str(redact(self.val, self.key))
            return self


class VariableBody(StrictBaseModel):
    """Variable serializer for bodies."""

    key: str = Field(max_length=ID_LEN)
    value: JsonValue = Field(serialization_alias="val")
    description: str | None = Field(default=None)
    team_name: str | None = Field(max_length=50, default=None)

    @field_validator("value", mode="after")
    @classmethod
    def serialize_non_string_value(cls, value: JsonValue) -> str:
        # Variables are stored as strings. A non-string JSON value (list, dict, number, bool,
        # null) must be JSON-encoded here: POST would otherwise store its Python repr, which
        # deserialize_json cannot read back, and PATCH would fail in the ORM Fernet step on
        # bytes(value, "utf-8") (see #71010). indent=2 matches Variable.set(serialize_json=True).
        # Not allow_nan=False: rejecting here yields a 422 whose response embeds the NaN input,
        # which starlette's JSONResponse cannot serialize -- the request would 500 instead.
        if isinstance(value, str):
            return value
        return json.dumps(value, indent=2)

    @model_validator(mode="after")
    def validate_team_name(self) -> VariableBody:
        if self.team_name is not None and not conf.getboolean("core", "multi_team"):
            raise ValueError(
                "team_name cannot be set when multi_team mode is disabled. Please contact your administrator."
            )
        return self


VariableBodyPartial = make_partial_model(VariableBody)


class VariableCollectionResponse(BaseModel):
    """Variable Collection serializer for responses."""

    variables: Iterable[VariableResponse]
    total_entries: int


class VariablesImportResponse(BaseModel):
    """Import Variables serializer for responses."""

    created_variable_keys: list[str]
    import_count: int
    created_count: int
