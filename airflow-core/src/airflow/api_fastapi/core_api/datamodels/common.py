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
"""
Common Data Models for Airflow REST API.

:meta private:
"""

from __future__ import annotations

import enum
from typing import Annotated, Any, Generic, Literal, TypeVar, Union

from pydantic import Discriminator, Field, Tag

from airflow.api_fastapi.core_api.base import BaseModel, StrictBaseModel

# Common Bulk Data Models
T = TypeVar("T")
K = TypeVar("K")


class BulkAction(str, enum.Enum):
    """Bulk Action to be performed on the used model."""

    CREATE = "create"
    DELETE = "delete"
    UPDATE = "update"


class BulkActionOnExistence(enum.Enum):
    """Bulk Action to be taken if the entity already exists or not."""

    FAIL = "fail"
    SKIP = "skip"
    OVERWRITE = "overwrite"


class BulkActionNotOnExistence(enum.Enum):
    """Bulk Action to be taken if the entity does not exist."""

    FAIL = "fail"
    SKIP = "skip"


class BulkBaseAction(StrictBaseModel, Generic[T]):
    """Base class for bulk actions."""

    action: BulkAction = Field(..., description="The action to be performed on the entities.")


class BulkCreateAction(BulkBaseAction[T]):
    """Bulk Create entity serializer for request bodies."""

    action: Literal[BulkAction.CREATE] = Field(description="The action to be performed on the entities.")
    entities: list[T] = Field(..., description="A list of entities to be created.")
    action_on_existence: BulkActionOnExistence = BulkActionOnExistence.FAIL


class BulkUpdateAction(BulkBaseAction[T]):
    """Bulk Update entity serializer for request bodies."""

    action: Literal[BulkAction.UPDATE] = Field(description="The action to be performed on the entities.")
    entities: list[T] = Field(..., description="A list of entities to be updated.")
    update_mask: list[str] | None = Field(
        default=None,
        description=(
            "A list of field names to update for each entity."
            "Only these fields will be applied from the request body to the database model."
            "Any extra fields provided will be ignored."
        ),
    )
    action_on_non_existence: BulkActionNotOnExistence = BulkActionNotOnExistence.FAIL


class BulkDeleteAction(BulkBaseAction[T]):
    """Bulk Delete entity serializer for request bodies."""

    action: Literal[BulkAction.DELETE] = Field(description="The action to be performed on the entities.")
    entities: list[Union[str, T]] = Field(
        ...,
        description="A list of entity id/key or entity objects to be deleted.",
    )
    action_on_non_existence: BulkActionNotOnExistence = BulkActionNotOnExistence.FAIL


def _action_discriminator(action: Any) -> str:
    return BulkAction(action["action"]).value


class BulkBody(StrictBaseModel, Generic[T]):
    """Serializer for bulk entity operations."""

    actions: list[
        Annotated[
            Union[
                Annotated[BulkCreateAction[T], Tag(BulkAction.CREATE.value)],
                Annotated[BulkUpdateAction[T], Tag(BulkAction.UPDATE.value)],
                Annotated[BulkDeleteAction[T], Tag(BulkAction.DELETE.value)],
            ],
            Discriminator(_action_discriminator),
        ]
    ]


class BulkActionResponse(BaseModel):
    """
    Serializer for individual bulk action responses.

    Represents the outcome of a single bulk operation (create, update, or delete).
    The response includes a list of successful keys and any errors encountered during the operation.
    This structure helps users understand which key actions succeeded and which failed.
    """

    success: list[str] = Field(
        default=[], description="A list of unique id/key representing successful operations."
    )
    errors: list[dict[str, Any]] = Field(
        default=[],
        description="A list of errors encountered during the operation, each containing details about the issue.",
    )


class BulkResponse(BaseModel):
    """
    Serializer for responses to bulk entity operations.

    This represents the results of create, update, and delete actions performed on entity in bulk.
    Each action (if requested) is represented as a field containing details about successful keys and any encountered errors.
    Fields are populated in the response only if the respective action was part of the request, else are set None.
    """

    create: BulkActionResponse | None = Field(
        default=None,
        description="Details of the bulk create operation, including successful keys and errors.",
    )
    update: BulkActionResponse | None = Field(
        default=None,
        description="Details of the bulk update operation, including successful keys and errors.",
    )
    delete: BulkActionResponse | None = Field(
        default=None,
        description="Details of the bulk delete operation, including successful keys and errors.",
    )
