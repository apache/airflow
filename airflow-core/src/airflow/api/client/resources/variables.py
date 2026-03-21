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
"""Variables resource sub-client for the local REST client."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    import httpx


class VariablesClient:
    """Client for ``/api/v2/variables`` endpoints."""

    def __init__(self, http: httpx.Client) -> None:
        self._http = http

    def get(self, key: str) -> dict[str, Any]:
        """Get a variable by key."""
        resp = self._http.get(f"/api/v2/variables/{key}")
        resp.raise_for_status()
        return resp.json()

    def list(self, *, limit: int = 100, offset: int = 0) -> dict[str, Any]:
        """List all variables with pagination."""
        resp = self._http.get("/api/v2/variables", params={"limit": limit, "offset": offset})
        resp.raise_for_status()
        return resp.json()

    def create(
        self,
        *,
        key: str,
        value: Any,
        description: str | None = None,
    ) -> dict[str, Any]:
        """Create a new variable."""
        body: dict[str, Any] = {
            "key": key,
            "value": value,
        }
        if description is not None:
            body["description"] = description
        resp = self._http.post("/api/v2/variables", json=body)
        resp.raise_for_status()
        return resp.json()

    def update(
        self,
        key: str,
        *,
        value: Any | None = None,
        description: str | None = None,
    ) -> dict[str, Any]:
        """Update an existing variable (partial update via update_mask)."""
        # The Core API PATCH accepts VariableBody (with required fields) but
        # supports an update_mask query param to limit which fields are validated.
        body: dict[str, Any] = {"key": key, "value": ""}
        update_mask: list[str] = []
        if value is not None:
            body["value"] = value
            update_mask.append("value")
        if description is not None:
            body["description"] = description
            update_mask.append("description")
        resp = self._http.patch(
            f"/api/v2/variables/{key}",
            json=body,
            params={"update_mask": update_mask},
        )
        resp.raise_for_status()
        return resp.json()

    def delete(self, key: str) -> None:
        """Delete a variable by key."""
        resp = self._http.delete(f"/api/v2/variables/{key}")
        resp.raise_for_status()
