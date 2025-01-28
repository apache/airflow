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

from fastapi import HTTPException, status
from pydantic import ValidationError
from sqlalchemy import select

from airflow.api_fastapi.core_api.datamodels.common import (
    BulkActionNotOnExistence,
    BulkActionOnExistence,
    BulkActionResponse,
    BulkCreateAction,
    BulkDeleteAction,
    BulkUpdateAction,
)
from airflow.api_fastapi.core_api.datamodels.connections import ConnectionBody
from airflow.api_fastapi.core_api.services.public.common import BulkService
from airflow.models.connection import Connection


class BulkConnectionService(BulkService[ConnectionBody]):
    """Service for handling bulk operations on connections."""

    def categorize_connections(self, connection_ids: set) -> tuple[dict, set, set]:
        """
        Categorize the given connection_ids into matched_connection_ids and not_found_connection_ids based on existing connection_ids.

        Existed connections are returned as a dict of {connection_id : Connection}.

        :param connection_ids: set of connection_ids
        :return: tuple of dict of existed connections, set of matched connection_ids, set of not found connection_ids
        """
        existed_connections = self.session.execute(
            select(Connection).filter(Connection.conn_id.in_(connection_ids))
        ).scalars()
        existed_connections_dict = {conn.conn_id: conn for conn in existed_connections}
        matched_connection_ids = set(existed_connections_dict.keys())
        not_found_connection_ids = connection_ids - matched_connection_ids
        return existed_connections_dict, matched_connection_ids, not_found_connection_ids

    def handle_bulk_create(
        self, action: BulkCreateAction[ConnectionBody], results: BulkActionResponse
    ) -> None:
        """Bulk create connections."""
        to_create_connection_ids = {connection.connection_id for connection in action.entities}
        existed_connections_dict, matched_connection_ids, not_found_connection_ids = (
            self.categorize_connections(to_create_connection_ids)
        )
        try:
            if action.action_on_existence == BulkActionOnExistence.FAIL and matched_connection_ids:
                raise HTTPException(
                    status_code=status.HTTP_409_CONFLICT,
                    detail=f"The connections with these connection_ids: {matched_connection_ids} already exist.",
                )
            elif action.action_on_existence == BulkActionOnExistence.SKIP:
                create_connection_ids = not_found_connection_ids
            else:
                create_connection_ids = to_create_connection_ids

            for connection in action.entities:
                if connection.connection_id in create_connection_ids:
                    if connection.connection_id in matched_connection_ids:
                        existed_connection = existed_connections_dict[connection.connection_id]
                        for key, val in connection.model_dump(by_alias=True).items():
                            setattr(existed_connection, key, val)
                    else:
                        self.session.add(Connection(**connection.model_dump(by_alias=True)))
                    results.success.append(connection.connection_id)

        except HTTPException as e:
            results.errors.append({"error": f"{e.detail}", "status_code": e.status_code})

    def handle_bulk_update(
        self, action: BulkUpdateAction[ConnectionBody], results: BulkActionResponse
    ) -> None:
        """Bulk Update connections."""
        to_update_connection_ids = {connection.connection_id for connection in action.entities}
        _, matched_connection_ids, not_found_connection_ids = self.categorize_connections(
            to_update_connection_ids
        )

        try:
            if action.action_on_non_existence == BulkActionNotOnExistence.FAIL and not_found_connection_ids:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail=f"The connections with these connection_ids: {not_found_connection_ids} were not found.",
                )
            elif action.action_on_non_existence == BulkActionNotOnExistence.SKIP:
                update_connection_ids = matched_connection_ids
            else:
                update_connection_ids = to_update_connection_ids

            for connection in action.entities:
                if connection.connection_id in update_connection_ids:
                    old_connection = self.session.scalar(
                        select(Connection).filter(Connection.conn_id == connection.connection_id).limit(1)
                    )
                    ConnectionBody(**connection.model_dump())
                    for key, val in connection.model_dump(by_alias=True).items():
                        setattr(old_connection, key, val)
                    results.success.append(connection.connection_id)

        except HTTPException as e:
            results.errors.append({"error": f"{e.detail}", "status_code": e.status_code})

        except ValidationError as e:
            results.errors.append({"error": f"{e.errors()}"})

    def handle_bulk_delete(
        self, action: BulkDeleteAction[ConnectionBody], results: BulkActionResponse
    ) -> None:
        """Bulk delete connections."""
        to_delete_connection_ids = set(action.entities)
        _, matched_connection_ids, not_found_connection_ids = self.categorize_connections(
            to_delete_connection_ids
        )

        try:
            if action.action_on_non_existence == BulkActionNotOnExistence.FAIL and not_found_connection_ids:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail=f"The connections with these connection_ids: {not_found_connection_ids} were not found.",
                )
            elif action.action_on_non_existence == BulkActionNotOnExistence.SKIP:
                delete_connection_ids = matched_connection_ids
            else:
                delete_connection_ids = to_delete_connection_ids

            for connection_id in delete_connection_ids:
                existing_connection = self.session.scalar(
                    select(Connection).where(Connection.conn_id == connection_id).limit(1)
                )
                if existing_connection:
                    self.session.delete(existing_connection)
                    results.success.append(connection_id)

        except HTTPException as e:
            results.errors.append({"error": f"{e.detail}", "status_code": e.status_code})
