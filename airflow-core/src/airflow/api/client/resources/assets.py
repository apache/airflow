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
"""Assets resource sub-client for the local REST client."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from airflow.api_fastapi.core_api.datamodels.assets import (
    AssetCollectionResponse,
    AssetEventCollectionResponse,
    AssetEventResponse,
    AssetResponse,
    QueuedEventCollectionResponse,
)

if TYPE_CHECKING:
    import httpx


class AssetsClient:
    """Client for ``/api/v2/assets`` endpoints."""

    def __init__(self, http: httpx.Client) -> None:
        self._http = http

    def get(self, asset_id: int) -> AssetResponse:
        """Get an asset by ID."""
        resp = self._http.get(f"/api/v2/assets/{asset_id}")
        resp.raise_for_status()
        return AssetResponse.model_validate(resp.json())

    def list(self, *, limit: int = 100, offset: int = 0, only_active: bool = True) -> AssetCollectionResponse:
        """List assets with pagination."""
        resp = self._http.get(
            "/api/v2/assets",
            params={"limit": limit, "offset": offset, "only_active": only_active},
        )
        resp.raise_for_status()
        return AssetCollectionResponse.model_validate(resp.json())

    def create_event(self, *, asset_id: int, extra: dict[str, Any] | None = None) -> AssetEventResponse:
        """Create an asset event (materialize signal)."""
        body: dict[str, Any] = {"asset_id": asset_id}
        if extra is not None:
            body["extra"] = extra
        resp = self._http.post("/api/v2/assets/events", json=body)
        resp.raise_for_status()
        return AssetEventResponse.model_validate(resp.json())

    def list_events(
        self,
        *,
        limit: int = 100,
        offset: int = 0,
        asset_id: int | None = None,
        source_dag_id: str | None = None,
        source_task_id: str | None = None,
    ) -> AssetEventCollectionResponse:
        """List asset events with optional filters."""
        params: dict[str, Any] = {"limit": limit, "offset": offset}
        if asset_id is not None:
            params["asset_id"] = asset_id
        if source_dag_id is not None:
            params["source_dag_id"] = source_dag_id
        if source_task_id is not None:
            params["source_task_id"] = source_task_id
        resp = self._http.get("/api/v2/assets/events", params=params)
        resp.raise_for_status()
        data = resp.json()
        return AssetEventCollectionResponse.model_construct(
            total_entries=data["total_entries"],
            asset_events=[AssetEventResponse.model_validate(e) for e in data["asset_events"]],
        )

    def materialize(self, asset_id: int) -> dict[str, Any]:
        """Trigger materialization of an asset (creates a DAG run)."""
        resp = self._http.post(f"/api/v2/assets/{asset_id}/materialize")
        resp.raise_for_status()
        return resp.json()

    def get_queued_events(self, asset_id: int) -> QueuedEventCollectionResponse:
        """Get queued events for an asset."""
        resp = self._http.get(f"/api/v2/assets/{asset_id}/queuedEvents")
        resp.raise_for_status()
        return QueuedEventCollectionResponse.model_validate(resp.json())

    def delete_queued_events(self, asset_id: int) -> None:
        """Delete queued events for an asset."""
        resp = self._http.delete(f"/api/v2/assets/{asset_id}/queuedEvents")
        resp.raise_for_status()
