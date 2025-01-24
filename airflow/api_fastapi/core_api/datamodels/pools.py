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

from typing import Annotated, Any, Callable

from pydantic import BeforeValidator, ConfigDict, Field

from airflow.api_fastapi.core_api.base import BaseModel
from airflow.api_fastapi.core_api.datamodels.common import (
    BulkAction,
    BulkActionNotOnExistence,
    BulkActionOnExistence,
    BulkBaseAction,
)


def _call_function(function: Callable[[], int]) -> int:
    """
    Call the given function.

    Used for the BeforeValidator to get the actual values from the bound method.
    """
    return function()


class BasePool(BaseModel):
    """Base serializer for Pool."""

    pool: str = Field(serialization_alias="name")
    slots: int
    description: str | None
    include_deferred: bool


class PoolResponse(BasePool):
    """Pool serializer for responses."""

    occupied_slots: Annotated[int, BeforeValidator(_call_function)]
    running_slots: Annotated[int, BeforeValidator(_call_function)]
    queued_slots: Annotated[int, BeforeValidator(_call_function)]
    scheduled_slots: Annotated[int, BeforeValidator(_call_function)]
    open_slots: Annotated[int, BeforeValidator(_call_function)]
    deferred_slots: Annotated[int, BeforeValidator(_call_function)]


class PoolCollectionResponse(BaseModel):
    """Pool Collection serializer for responses."""

    pools: list[PoolResponse]
    total_entries: int


class PoolPatchBody(BaseModel):
    """Pool serializer for patch bodies."""

    model_config = ConfigDict(populate_by_name=True, from_attributes=True)

    name: str | None = Field(default=None, alias="pool")
    slots: int | None = None
    description: str | None = None
    include_deferred: bool | None = None


class PoolPostBody(BasePool):
    """Pool serializer for post bodies."""

    pool: str = Field(alias="name", max_length=256)
    description: str | None = None
    include_deferred: bool = False


class PoolPostBulkBody(BaseModel):
    """Pools serializer for post bodies."""

    pools: list[PoolPostBody]
    overwrite: bool | None = Field(default=False)


class PoolBulkCreateAction(BulkBaseAction):
    """Bulk Create Pool serializer for request bodies."""

    action: BulkAction = BulkAction.CREATE
    pools: list[PoolPostBody] = Field(..., description="A list of pools to be created.")
    action_on_existence: BulkActionOnExistence = BulkActionOnExistence.FAIL


class PoolBulkUpdateAction(BulkBaseAction):
    """Bulk Update Pool serializer for request bodies."""

    action: BulkAction = BulkAction.UPDATE
    pools: list[PoolPatchBody] = Field(..., description="A list of pools to be updated.")
    action_on_non_existence: BulkActionNotOnExistence = BulkActionNotOnExistence.FAIL


class PoolBulkDeleteAction(BulkBaseAction):
    """Bulk Delete Pool serializer for request bodies."""

    action: BulkAction = BulkAction.DELETE
    pool_names: list[str] = Field(..., description="A list of pool names to be deleted.")
    action_on_non_existence: BulkActionNotOnExistence = BulkActionNotOnExistence.FAIL


class PoolBulkBody(BaseModel):
    """Request body for bulk Pool operations (create, update, delete)."""

    actions: list[PoolBulkCreateAction | PoolBulkUpdateAction | PoolBulkDeleteAction] = Field(
        ..., description="A list of Pool actions to perform."
    )


class PoolBulkActionResponse(BaseModel):
    """
    Serializer for individual bulk action responses.

    Represents the outcome of a single bulk operation (create, update, or delete).
    The response includes a list of successful pool names and any errors encountered during the operation.
    This structure helps users understand which key actions succeeded and which failed.
    """

    success: list[str] = Field(
        default_factory=list, description="A list of pool names representing successful operations."
    )
    errors: list[dict[str, Any]] = Field(
        default_factory=list,
        description="A list of errors encountered during the operation, each containing details about the issue.",
    )


class PoolBulkResponse(BaseModel):
    """
    Serializer for responses to bulk pool operations.

    This represents the results of create, update, and delete actions performed on pools in bulk.
    Each action (if requested) is represented as a field containing details about successful pool names and any encountered errors.
    Fields are populated in the response only if the respective action was part of the request, else are set None.
    """

    create: PoolBulkActionResponse | None = Field(
        default=None,
        description="Details of the bulk create operation, including successful pool names and errors.",
    )
    update: PoolBulkActionResponse | None = Field(
        default=None,
        description="Details of the bulk update operation, including successful pool names and errors.",
    )
    delete: PoolBulkActionResponse | None = Field(
        default=None,
        description="Details of the bulk delete operation, including successful pool names and errors.",
    )
