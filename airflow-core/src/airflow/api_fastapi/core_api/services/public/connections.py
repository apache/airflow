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
from airflow.sdk.execution_time.secrets_masker import merge


def update_orm_from_pydantic(
    orm_conn: Connection, pydantic_conn: ConnectionBody, update_mask: list[str] | None = None
) -> None:
    """Update ORM object from Pydantic object."""
    # Not all fields match and some need setters, therefore copy partly manually via setters
    non_update_fields = {"connection_id", "conn_id"}
    setter_fields = {"password", "extra"}
    fields_set = pydantic_conn.model_fields_set
    if "schema_" in fields_set:  # Alias is not resolved correctly, need to patch
        fields_set.remove("schema_")
        fields_set.add("schema")
    fields_to_update = fields_set - non_update_fields - setter_fields
    if update_mask:
        fields_to_update = fields_to_update.intersection(update_mask)
    conn_data = pydantic_conn.model_dump(by_alias=True)
    for key, val in conn_data.items():
        if key in fields_to_update:
            setattr(orm_conn, key, val)

    if (not update_mask and "password" in pydantic_conn.model_fields_set) or (
        update_mask and "password" in update_mask
    ):
        if pydantic_conn.password is None:
            orm_conn.set_password(pydantic_conn.password)
        else:
            merged_password = merge(pydantic_conn.password, orm_conn.password, "password")
            orm_conn.set_password(merged_password)
    if (not update_mask and "extra" in pydantic_conn.model_fields_set) or (
        update_mask and "extra" in update_mask
    ):
        if pydantic_conn.extra is None or orm_conn.extra is None:
            orm_conn.set_extra(pydantic_conn.extra)
            return
        try:
            merged_extra = merge(json.loads(pydantic_conn.extra), json.loads(orm_conn.extra))
            orm_conn.set_extra(json.dumps(merged_extra))
        except json.JSONDecodeError:
            # We can't merge fields in an unstructured `extra`
            orm_conn.set_extra(pydantic_conn.extra)


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
            if action.action_on_existence == BulkActionOnExistence.SKIP:
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
            if action.action_on_non_existence == BulkActionNotOnExistence.SKIP:
                update_connection_ids = matched_connection_ids
            else:
                update_connection_ids = to_update_connection_ids

            for connection in action.entities:
                if connection.connection_id in update_connection_ids:
                    old_connection: Connection = self.session.scalar(
                        select(Connection).filter(Connection.conn_id == connection.connection_id).limit(1)
                    )
                    if old_connection is None:
                        raise ValidationError(
                            f"The Connection with connection_id: `{connection.connection_id}` was not found"
                        )
                    ConnectionBody(**connection.model_dump())

                    update_orm_from_pydantic(old_connection, connection)
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
            if action.action_on_non_existence == BulkActionNotOnExistence.SKIP:
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
