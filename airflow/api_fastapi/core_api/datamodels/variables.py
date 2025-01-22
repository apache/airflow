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
from typing import Any

from pydantic import ConfigDict, Field, model_validator

from airflow.api_fastapi.core_api.base import BaseModel
from airflow.api_fastapi.core_api.datamodels.common import BulkAction, BulkBaseAction
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


class VariableBulkCreateAction(BulkBaseAction):
    """Bulk Create Variable serializer for request bodies."""

    action: BulkAction = BulkAction.CREATE
    variables: list[VariableBody] = Field(..., description="A list of variables to be created.")


class VariableBulkUpdateAction(BulkBaseAction):
    """Bulk Update Variable serializer for request bodies."""

    action: BulkAction = BulkAction.UPDATE
    variables: list[VariableBody] = Field(..., description="A list of variables to be updated.")


class VariableBulkDeleteAction(BulkBaseAction):
    """Bulk Delete Variable serializer for request bodies."""

    action: BulkAction = BulkAction.DELETE
    keys: list[str] = Field(..., description="A list of variable keys to be deleted.")


class VariableBulkBody(BaseModel):
    """Request body for bulk variable operations (create, update, delete)."""

    actions: list[VariableBulkCreateAction | VariableBulkUpdateAction | VariableBulkDeleteAction] = Field(
        ..., description="A list of variable actions to perform."
    )


class VariableBulkActionResponse(BaseModel):
    """
    Serializer for individual bulk action responses.

    Represents the outcome of a single bulk operation (create, update, or delete).
    The response includes a list of successful keys and any errors encountered during the operation.
    This structure helps users understand which key actions succeeded and which failed.
    """

    success: list[str] = Field(default=[], description="A list of keys representing successful operations.")
    errors: list[dict[str, Any]] = Field(
        default=[],
        description="A list of errors encountered during the operation, each containing details about the issue.",
    )


class VariableBulkResponse(BaseModel):
    """
    Serializer for responses to bulk variable operations.

    This represents the results of create, update, and delete actions performed on variables in bulk.
    Each action (if requested) is represented as a field containing details about successful keys and any encountered errors.
    Fields are populated in the response only if the respective action was part of the request, else are set None.
    """

    create: VariableBulkActionResponse | None = Field(
        default=None,
        description="Details of the bulk create operation, including successful keys and errors.",
    )
    update: VariableBulkActionResponse | None = Field(
        default=None,
        description="Details of the bulk update operation, including successful keys and errors.",
    )
    delete: VariableBulkActionResponse | None = Field(
        default=None,
        description="Details of the bulk delete operation, including successful keys and errors.",
    )
