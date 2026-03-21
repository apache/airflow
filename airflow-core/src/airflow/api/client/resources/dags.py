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

import logging
from typing import TYPE_CHECKING

from airflow.api_fastapi.core_api.datamodels.dags import (
    DAGCollectionResponse,
    DAGDetailsResponse,
    DAGResponse,
)

_log = logging.getLogger(__name__)

if TYPE_CHECKING:
    import httpx


class DagsClient:
    """Client for ``/api/v2/dags`` endpoints."""

    def __init__(self, http: httpx.Client) -> None:
        self._http = http

    def get(self, dag_id: str) -> DAGResponse:
        """Get a DAG by ID."""
        resp = self._http.get(f"/api/v2/dags/{dag_id}")
        resp.raise_for_status()
        return DAGResponse.model_validate(resp.json())

    def get_details(self, dag_id: str) -> DAGDetailsResponse:
        """Get detailed DAG information including params and template paths."""
        resp = self._http.get(f"/api/v2/dags/{dag_id}/details")
        resp.raise_for_status()
        return DAGDetailsResponse.model_validate(resp.json())

    def list(self, *, limit: int = 100, offset: int = 0) -> DAGCollectionResponse:
        """List all DAGs with pagination."""
        resp = self._http.get("/api/v2/dags", params={"limit": limit, "offset": offset})
        resp.raise_for_status()
        data = resp.json()
        try:
            dags = [DAGResponse.model_validate(d) for d in data["dags"]]
        except Exception:
            # Log the failing DAG data and re-raise with context
            _log.error("Failed to validate DAG response: %s", data, exc_info=True)
            raise
        return DAGCollectionResponse.model_construct(
            total_entries=data["total_entries"],
            dags=dags,
        )

    def pause(self, dag_id: str) -> DAGResponse:
        """Pause a DAG."""
        resp = self._http.patch(f"/api/v2/dags/{dag_id}", json={"is_paused": True})
        resp.raise_for_status()
        return DAGResponse.model_validate(resp.json())

    def unpause(self, dag_id: str) -> DAGResponse:
        """Unpause a DAG."""
        resp = self._http.patch(f"/api/v2/dags/{dag_id}", json={"is_paused": False})
        resp.raise_for_status()
        return DAGResponse.model_validate(resp.json())

    def delete(self, dag_id: str) -> None:
        """Delete a DAG."""
        resp = self._http.delete(f"/api/v2/dags/{dag_id}")
        resp.raise_for_status()
