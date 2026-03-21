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
"""Pools resource sub-client for the local REST client."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    import httpx


class PoolsClient:
    """Client for ``/api/v2/pools`` endpoints."""

    def __init__(self, http: httpx.Client) -> None:
        self._http = http

    def get(self, name: str) -> dict[str, Any]:
        """Get a pool by name."""
        resp = self._http.get(f"/api/v2/pools/{name}")
        resp.raise_for_status()
        return resp.json()

    def list(self, *, limit: int = 100, offset: int = 0) -> dict[str, Any]:
        """List all pools with pagination."""
        resp = self._http.get("/api/v2/pools", params={"limit": limit, "offset": offset})
        resp.raise_for_status()
        return resp.json()

    def create(
        self,
        *,
        name: str,
        slots: int,
        description: str | None = None,
        include_deferred: bool = False,
    ) -> dict[str, Any]:
        """Create a new pool."""
        body: dict[str, Any] = {
            "name": name,
            "slots": slots,
            "include_deferred": include_deferred,
        }
        if description is not None:
            body["description"] = description
        resp = self._http.post("/api/v2/pools", json=body)
        resp.raise_for_status()
        return resp.json()

    def update(
        self,
        name: str,
        *,
        slots: int | None = None,
        description: str | None = None,
        include_deferred: bool | None = None,
    ) -> dict[str, Any]:
        """Update an existing pool (partial update via fetch-modify-update)."""
        # The Core API PATCH endpoint validates against BasePool which requires
        # all base fields, so we fetch the current state and merge changes.
        current = self.get(name)
        body: dict[str, Any] = {
            "name": current["name"],
            "slots": current["slots"],
            "include_deferred": current["include_deferred"],
        }
        if slots is not None:
            body["slots"] = slots
        if description is not None:
            body["description"] = description
        elif current.get("description") is not None:
            body["description"] = current["description"]
        if include_deferred is not None:
            body["include_deferred"] = include_deferred
        resp = self._http.patch(f"/api/v2/pools/{name}", json=body)
        resp.raise_for_status()
        return resp.json()

    def delete(self, name: str) -> None:
        """Delete a pool by name."""
        resp = self._http.delete(f"/api/v2/pools/{name}")
        resp.raise_for_status()
