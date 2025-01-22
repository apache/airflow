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

from pydantic import Field, field_validator
from pydantic_core.core_schema import ValidationInfo

from airflow.api_fastapi.core_api.base import BaseModel
from airflow.api_fastapi.core_api.datamodels.common import BulkAction, BulkBaseAction
from airflow.utils.log.secrets_masker import redact


# Response Models
class ConnectionResponse(BaseModel):
    """Connection serializer for responses."""

    connection_id: str = Field(serialization_alias="connection_id", validation_alias="conn_id")
    conn_type: str
    description: str | None
    host: str | None
    login: str | None
    schema_: str | None = Field(alias="schema")
    port: int | None
    password: str | None
    extra: str | None

    @field_validator("password", mode="after")
    @classmethod
    def redact_password(cls, v: str | None, field_info: ValidationInfo) -> str | None:
        if v is None:
            return None
        return redact(v, field_info.field_name)

    @field_validator("extra", mode="before")
    @classmethod
    def redact_extra(cls, v: str | None) -> str | None:
        if v is None:
            return None
        try:
            extra_dict = json.loads(v)
            redacted_dict = redact(extra_dict)
            return json.dumps(redacted_dict)
        except json.JSONDecodeError:
            # we can't redact fields in an unstructured `extra`
            return v


class ConnectionCollectionResponse(BaseModel):
    """Connection Collection serializer for responses."""

    connections: list[ConnectionResponse]
    total_entries: int


class ConnectionTestResponse(BaseModel):
    """Connection Test serializer for responses."""

    status: bool
    message: str


# Request Models
class ConnectionBody(BaseModel):
    """Connection Serializer for requests body."""

    connection_id: str = Field(serialization_alias="conn_id", max_length=200, pattern=r"^[\w.-]+$")
    conn_type: str
    description: str | None = Field(default=None)
    host: str | None = Field(default=None)
    login: str | None = Field(default=None)
    schema_: str | None = Field(None, alias="schema")
    port: int | None = Field(default=None)
    password: str | None = Field(default=None)
    extra: str | None = Field(default=None)


class ConnectionBulkCreateAction(BulkBaseAction):
    """Bulk Create Variable serializer for request bodies."""

    action: BulkAction = BulkAction.CREATE
    connections: list[ConnectionBody] = Field(..., description="A list of connections to be created.")


class ConnectionBulkUpdateAction(BulkBaseAction):
    """Bulk Update Connection serializer for request bodies."""

    action: BulkAction = BulkAction.UPDATE
    connections: list[ConnectionBody] = Field(..., description="A list of connections to be updated.")


class ConnectionBulkDeleteAction(BulkBaseAction):
    """Bulk Delete Connection serializer for request bodies."""

    action: BulkAction = BulkAction.DELETE
    connection_ids: list[str] = Field(..., description="A list of connection IDs to be deleted.")


class ConnectionBulkBody(BaseModel):
    """Request body for bulk Connection operations (create, update, delete)."""

    actions: list[ConnectionBulkCreateAction | ConnectionBulkUpdateAction | ConnectionBulkDeleteAction] = (
        Field(..., description="A list of Connection actions to perform.")
    )


class ConnectionBulkActionResponse(BaseModel):
    """
    Serializer for individual bulk action responses.

    Represents the outcome of a single bulk operation (create, update, or delete).
    The response includes a list of successful connection_ids and any errors encountered during the operation.
    This structure helps users understand which key actions succeeded and which failed.
    """

    success: list[str] = Field(
        default_factory=list, description="A list of connection_ids representing successful operations."
    )
    errors: list[dict[str, Any]] = Field(
        default_factory=list,
        description="A list of errors encountered during the operation, each containing details about the issue.",
    )


class ConnectionBulkResponse(BaseModel):
    """
    Serializer for responses to bulk connection operations.

    This represents the results of create, update, and delete actions performed on connections in bulk.
    Each action (if requested) is represented as a field containing details about successful connection_ids and any encountered errors.
    Fields are populated in the response only if the respective action was part of the request, else are set None.
    """

    create: ConnectionBulkActionResponse | None = Field(
        default=None,
        description="Details of the bulk create operation, including successful connection_ids and errors.",
    )
    update: ConnectionBulkActionResponse | None = Field(
        default=None,
        description="Details of the bulk update operation, including successful connection_ids and errors.",
    )
    delete: ConnectionBulkActionResponse | None = Field(
        default=None,
        description="Details of the bulk delete operation, including successful connection_ids and errors.",
    )
