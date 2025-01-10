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
from typing import Literal, Union

from pydantic import ConfigDict, Field, model_validator

from airflow.api_fastapi.core_api.base import BaseModel
from airflow.models.base import ID_LEN
from airflow.typing_compat import Self
from airflow.utils.log.secrets_masker import redact


class VariableResponse(BaseModel):
    """Variable serializer for responses."""

    model_config = ConfigDict(populate_by_name=True, from_attributes=True)

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


class VariableBody(BaseModel):
    """Variable serializer for bodies."""

    key: str = Field(max_length=ID_LEN)
    value: str = Field(serialization_alias="val")
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


class VariableActionCreate(BaseModel):
    """Request body for creating variables."""

    action: Literal["create"] = "create"
    variables: list[VariableBody] = Field(..., description="A list of variables to be created.")
    action_if_exists: Literal["skip", "overwrite", "fail"] = "fail"


class VariableActionUpdate(BaseModel):
    """Request body for updating existing variables."""

    action: Literal["update"] = "update"
    variables: list[VariableBody] = Field(..., description="A list of variables to be updated.")
    action_if_not_exists: Literal["skip", "fail"] = "fail"


class VariableActionDelete(BaseModel):
    """Request body for deleting variables."""

    action: Literal["delete"] = "delete"
    keys: list[str] = Field(..., description="A list of variable keys to be deleted.")
    action_if_not_exists: Literal["skip", "fail"] = "fail"


VariableAction = Union[VariableActionCreate, VariableActionUpdate, VariableActionDelete]


class BulkVariableRequest(BaseModel):
    """Request body for bulk variable operations (create, update, delete)."""

    actions: list[VariableAction] = Field(..., description="A list of variable actions to perform.")


class BulkVariableResponse(BaseModel):
    """Response body for bulk variable operations."""

    created: list[str] = Field(
        default_factory=list, description="list of keys for successfully created variables."
    )
    updated: list[str] = Field(
        default_factory=list, description="list of keys for successfully updated variables."
    )
    deleted: list[str] = Field(
        default_factory=list, description="list of keys for successfully deleted variables."
    )
    errors: list[dict] = Field(
        default_factory=list, description="list of error details for failed operations."
    )
