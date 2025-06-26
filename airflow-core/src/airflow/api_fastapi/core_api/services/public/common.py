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
                self.handle_bulk_create(action, results[action.action.value])  # type: ignore
            elif action.action == BulkAction.UPDATE:
                self.handle_bulk_update(action, results[action.action.value])  # type: ignore
            elif action.action == BulkAction.DELETE:
                self.handle_bulk_delete(action, results[action.action.value])  # type: ignore

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
