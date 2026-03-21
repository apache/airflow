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
"""DAGs resource sub-client for the local REST client."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    import httpx


class DagsClient:
    """Client for ``/api/v2/dags`` endpoints."""

    def __init__(self, http: httpx.Client) -> None:
        self._http = http

    def get(self, dag_id: str) -> dict[str, Any]:
        """Get a DAG by ID."""
        resp = self._http.get(f"/api/v2/dags/{dag_id}")
        resp.raise_for_status()
        return resp.json()

    def get_details(self, dag_id: str) -> dict[str, Any]:
        """Get detailed DAG information including params and template paths."""
        resp = self._http.get(f"/api/v2/dags/{dag_id}/details")
        resp.raise_for_status()
        return resp.json()

    def list(self, *, limit: int = 100, offset: int = 0) -> dict[str, Any]:
        """List all DAGs with pagination."""
        resp = self._http.get("/api/v2/dags", params={"limit": limit, "offset": offset})
        resp.raise_for_status()
        return resp.json()

    def pause(self, dag_id: str) -> dict[str, Any]:
        """Pause a DAG."""
        resp = self._http.patch(f"/api/v2/dags/{dag_id}", json={"is_paused": True})
        resp.raise_for_status()
        return resp.json()

    def unpause(self, dag_id: str) -> dict[str, Any]:
        """Unpause a DAG."""
        resp = self._http.patch(f"/api/v2/dags/{dag_id}", json={"is_paused": False})
        resp.raise_for_status()
        return resp.json()

    def delete(self, dag_id: str) -> None:
        """Delete a DAG."""
        resp = self._http.delete(f"/api/v2/dags/{dag_id}")
        resp.raise_for_status()
