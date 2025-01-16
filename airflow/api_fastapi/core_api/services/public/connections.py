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

from airflow.api_fastapi.core_api.datamodels.connections import (
    ConnectionBody,
    ConnectionBulkActionResponse,
    ConnectionBulkCreateAction,
    ConnectionBulkDeleteAction,
    ConnectionBulkUpdateAction,
)
from airflow.models.connection import Connection


def categorize_connection_ids(session, connection_ids: set) -> tuple[set, set]:
    """Categorize the given connection_ids into matched_connection_ids and not_found_connection_ids based on existing connection_ids."""
    existing_connection_ids = {
        connection for connection in session.execute(select(Connection.conn_id)).scalars()
    }
    matched_connection_ids = existing_connection_ids & connection_ids
    not_found_connection_ids = connection_ids - existing_connection_ids
    return matched_connection_ids, not_found_connection_ids


def handle_bulk_create(
    session, action: ConnectionBulkCreateAction, results: ConnectionBulkActionResponse
) -> None:
    """Bulk create connections."""
    to_create_connection_ids = {connection.connection_id for connection in action.connections}
    matched_connection_ids, not_found_connection_ids = categorize_connection_ids(
        session, to_create_connection_ids
    )

    try:
        if action.action_if_exists == "fail" and matched_connection_ids:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail=f"The connections with these connection_ids: {matched_connection_ids} already exist.",
            )
        elif action.action_if_exists == "skip":
            create_connection_ids = not_found_connection_ids
        else:
            create_connection_ids = to_create_connection_ids

        for connection in action.connections:
            if connection.connection_id in create_connection_ids:
                session.add(connection)
                results.success.append(connection.connection_id)

    except HTTPException as e:
        results.errors.append({"error": f"{e.detail}", "status_code": e.status_code})


def handle_bulk_update(
    session, action: ConnectionBulkUpdateAction, results: ConnectionBulkActionResponse
) -> None:
    """Bulk Update connections."""
    to_update_connection_ids = {connection.connection_id for connection in action.connections}
    matched_connection_ids, not_found_connection_ids = categorize_connection_ids(
        session, to_update_connection_ids
    )

    try:
        if action.action_if_not_exists == "fail" and not_found_connection_ids:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"The connections with these connection_ids: {not_found_connection_ids} were not found.",
            )
        elif action.action_if_not_exists == "skip":
            update_connection_ids = matched_connection_ids
        else:
            update_connection_ids = to_update_connection_ids

        for connection in action.connections:
            if connection.connection_id in update_connection_ids:
                old_connection = session.scalar(
                    select(Connection).filter_by(Connection.conn_id == connection.connection_id).limit(1)
                )
                ConnectionBody(**connection.model_dump())
                data = connection.model_dump(exclude={"key"}, by_alias=True)

                for key, val in data.items():
                    setattr(old_connection, key, val)
                results.success.append(connection.connection_id)

    except HTTPException as e:
        results.errors.append({"error": f"{e.detail}", "status_code": e.status_code})

    except ValidationError as e:
        results.errors.append({"error": f"{e.errors()}"})


def handle_bulk_delete(
    session, action: ConnectionBulkDeleteAction, results: ConnectionBulkActionResponse
) -> None:
    """Bulk delete connections."""
    to_delete_connection_ids = set(action.connection_ids)
    matched_connection_ids, not_found_connection_ids = categorize_connection_ids(
        session, to_delete_connection_ids
    )

    try:
        if action.action_if_not_exists == "fail" and not_found_connection_ids:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"The connections with these connection_ids: {not_found_connection_ids} were not found.",
            )
        elif action.action_if_not_exists == "skip":
            delete_connection_ids = matched_connection_ids
        else:
            delete_connection_ids = to_delete_connection_ids

        for key in delete_connection_ids:
            existing_connection = session.scalar(select(Connection).where(Connection.conn_id == key).limit(1))
            if existing_connection:
                session.delete(existing_connection)
                results.success.append(key)

    except HTTPException as e:
        results.errors.append({"error": f"{e.detail}", "status_code": e.status_code})
