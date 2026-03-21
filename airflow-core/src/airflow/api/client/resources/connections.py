#
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
"""Connections resource sub-client for the local REST client."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from airflow.api_fastapi.core_api.datamodels.connections import (
    ConnectionCollectionResponse,
    ConnectionResponse,
)

if TYPE_CHECKING:
    import httpx


class ConnectionsClient:
    """Client for ``/api/v2/connections`` endpoints."""

    def __init__(self, http: httpx.Client) -> None:
        self._http = http

    def get(self, connection_id: str) -> ConnectionResponse:
        """Get a connection by ID."""
        resp = self._http.get(f"/api/v2/connections/{connection_id}")
        resp.raise_for_status()
        return ConnectionResponse.model_validate(resp.json())

    def list(self, *, limit: int = 100, offset: int = 0) -> ConnectionCollectionResponse:
        """List all connections with pagination."""
        resp = self._http.get("/api/v2/connections", params={"limit": limit, "offset": offset})
        resp.raise_for_status()
        data = resp.json()
        return ConnectionCollectionResponse.model_construct(
            total_entries=data["total_entries"],
            connections=[ConnectionResponse.model_validate(c) for c in data["connections"]],
        )

    def create(
        self,
        *,
        connection_id: str,
        conn_type: str,
        description: str | None = None,
        host: str | None = None,
        login: str | None = None,
        schema: str | None = None,
        port: int | None = None,
        password: str | None = None,
        extra: str | None = None,
    ) -> ConnectionResponse:
        """Create a new connection."""
        body: dict[str, Any] = {
            "connection_id": connection_id,
            "conn_type": conn_type,
        }
        if description is not None:
            body["description"] = description
        if host is not None:
            body["host"] = host
        if login is not None:
            body["login"] = login
        if schema is not None:
            body["schema"] = schema
        if port is not None:
            body["port"] = port
        if password is not None:
            body["password"] = password
        if extra is not None:
            body["extra"] = extra
        resp = self._http.post("/api/v2/connections", json=body)
        resp.raise_for_status()
        return ConnectionResponse.model_validate(resp.json())

    def update(
        self,
        connection_id: str,
        *,
        conn_type: str | None = None,
        description: str | None = None,
        host: str | None = None,
        login: str | None = None,
        schema: str | None = None,
        port: int | None = None,
        password: str | None = None,
        extra: str | None = None,
    ) -> ConnectionResponse:
        """Update an existing connection (partial update via update_mask)."""
        # The Core API PATCH accepts ConnectionBody (with required fields) but
        # supports an update_mask query param to limit which fields are validated.
        body: dict[str, Any] = {"connection_id": connection_id, "conn_type": conn_type or ""}
        update_mask: list[str] = []
        if conn_type is not None:
            body["conn_type"] = conn_type
            update_mask.append("conn_type")
        if description is not None:
            body["description"] = description
            update_mask.append("description")
        if host is not None:
            body["host"] = host
            update_mask.append("host")
        if login is not None:
            body["login"] = login
            update_mask.append("login")
        if schema is not None:
            body["schema"] = schema
            update_mask.append("schema")
        if port is not None:
            body["port"] = port
            update_mask.append("port")
        if password is not None:
            body["password"] = password
            update_mask.append("password")
        if extra is not None:
            body["extra"] = extra
            update_mask.append("extra")
        resp = self._http.patch(
            f"/api/v2/connections/{connection_id}",
            json=body,
            params={"update_mask": update_mask},
        )
        resp.raise_for_status()
        return ConnectionResponse.model_validate(resp.json())

    def delete(self, connection_id: str) -> None:
        """Delete a connection by ID."""
        resp = self._http.delete(f"/api/v2/connections/{connection_id}")
        resp.raise_for_status()
