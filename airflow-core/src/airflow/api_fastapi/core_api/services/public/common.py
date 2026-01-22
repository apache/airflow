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

from abc import ABC, abstractmethod
from typing import Generic

from fastapi import HTTPException, status
from pydantic import BaseModel
from sqlalchemy.ext.declarative import DeclarativeMeta
from sqlalchemy.orm import Session

from airflow.api_fastapi.core_api.datamodels.common import (
    BulkAction,
    BulkActionResponse,
    BulkBody,
    BulkCreateAction,
    BulkDeleteAction,
    BulkResponse,
    BulkUpdateAction,
    T,
)


class BulkService(Generic[T], ABC):
    """Base class for bulk service operations."""

    def __init__(self, session: Session, request: BulkBody[T]):
        self.session = session
        self.request = request

    def handle_request(self) -> BulkResponse:
        """Handle request for bulk actions."""
        results: dict[str, BulkActionResponse] = {}

        for action in self.request.actions:
            if action.action.value not in results:
                results[action.action.value] = BulkActionResponse()

            if action.action == BulkAction.CREATE:
                self.handle_bulk_create(action, results[action.action.value])
            elif action.action == BulkAction.UPDATE:
                self.handle_bulk_update(action, results[action.action.value])
            elif action.action == BulkAction.DELETE:
                self.handle_bulk_delete(action, results[action.action.value])

        return BulkResponse(**results)

    @abstractmethod
    def handle_bulk_create(self, action: BulkCreateAction[T], results: BulkActionResponse) -> None:
        """Bulk create entities."""
        raise NotImplementedError

    @abstractmethod
    def handle_bulk_update(self, action: BulkUpdateAction[T], results: BulkActionResponse) -> None:
        """Bulk update entities."""
        raise NotImplementedError

    @abstractmethod
    def handle_bulk_delete(self, action: BulkDeleteAction[T], results: BulkActionResponse) -> None:
        """Bulk delete entities."""
        raise NotImplementedError

    @staticmethod
    def apply_patch_with_update_mask(
        model: DeclarativeMeta,
        patch_body: BaseModel,
        update_mask: list[str] | None,
        non_update_fields: set[str] | None = None,
    ) -> DeclarativeMeta:
        """
        Apply a patch to the given model using the provided update mask.

        :param model: The SQLAlchemy model instance to update.
        :param patch_body: Pydantic model containing patch data.
        :param update_mask: Optional list of fields to update.
        :param non_update_fields: Fields that should not be updated.
        :return: The updated SQLAlchemy model instance.
        :raises HTTPException: If invalid fields are provided in update_mask.
        """
        # Always dump without aliases for internal validation
        raw_data = patch_body.model_dump(by_alias=False)
        fields_to_update = set(patch_body.model_fields_set)

        non_update_fields = non_update_fields or set()

        if update_mask:
            restricted_in_mask = set(update_mask).intersection(non_update_fields)
            if restricted_in_mask:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail=f"Update not allowed: the following fields are immutable and cannot be modified: {restricted_in_mask}",
                )
            fields_to_update = fields_to_update.intersection(update_mask)

        if non_update_fields:
            fields_to_update = fields_to_update - non_update_fields

        validated_data = {key: raw_data[key] for key in fields_to_update if key in raw_data}

        data = patch_body.model_dump(include=set(validated_data.keys()), by_alias=True)

        # Update the model with the validated data
        for key, value in data.items():
            setattr(model, key, value)

        return model
