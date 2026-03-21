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
"""DAG Runs resource sub-client for the local REST client."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from airflow.api_fastapi.core_api.datamodels.dag_run import DAGRunCollectionResponse, DAGRunResponse
from airflow.api_fastapi.core_api.datamodels.task_instances import (
    TaskInstanceCollectionResponse,
    TaskInstanceResponse,
)

if TYPE_CHECKING:
    import httpx


class DagRunsClient:
    """Client for ``/api/v2/dags/{dag_id}/dagRuns`` endpoints."""

    def __init__(self, http: httpx.Client) -> None:
        self._http = http

    def get(self, dag_id: str, dag_run_id: str) -> DAGRunResponse:
        """Get a DAG run by ID."""
        resp = self._http.get(f"/api/v2/dags/{dag_id}/dagRuns/{dag_run_id}")
        resp.raise_for_status()
        return DAGRunResponse.model_validate(resp.json())

    def list(self, dag_id: str, *, limit: int = 100, offset: int = 0) -> DAGRunCollectionResponse:
        """List DAG runs for a specific DAG with pagination."""
        resp = self._http.get(
            f"/api/v2/dags/{dag_id}/dagRuns",
            params={"limit": limit, "offset": offset},
        )
        resp.raise_for_status()
        data = resp.json()
        return DAGRunCollectionResponse.model_construct(
            total_entries=data["total_entries"],
            dag_runs=[DAGRunResponse.model_validate(r) for r in data["dag_runs"]],
        )

    def trigger(
        self,
        dag_id: str,
        *,
        dag_run_id: str | None = None,
        logical_date: str | None = None,
        conf: dict[str, Any] | None = None,
        note: str | None = None,
    ) -> DAGRunResponse:
        """Trigger a new DAG run."""
        body: dict[str, Any] = {"logical_date": logical_date}
        if dag_run_id is not None:
            body["dag_run_id"] = dag_run_id
        if conf is not None:
            body["conf"] = conf
        if note is not None:
            body["note"] = note
        resp = self._http.post(f"/api/v2/dags/{dag_id}/dagRuns", json=body)
        resp.raise_for_status()
        return DAGRunResponse.model_validate(resp.json())

    def update(
        self,
        dag_id: str,
        dag_run_id: str,
        *,
        state: str | None = None,
        note: str | None = None,
    ) -> DAGRunResponse:
        """Update a DAG run (state or note)."""
        body: dict[str, Any] = {}
        if state is not None:
            body["state"] = state
        if note is not None:
            body["note"] = note
        resp = self._http.patch(f"/api/v2/dags/{dag_id}/dagRuns/{dag_run_id}", json=body)
        resp.raise_for_status()
        return DAGRunResponse.model_validate(resp.json())

    def delete(self, dag_id: str, dag_run_id: str) -> None:
        """Delete a DAG run."""
        resp = self._http.delete(f"/api/v2/dags/{dag_id}/dagRuns/{dag_run_id}")
        resp.raise_for_status()

    def clear(
        self,
        dag_id: str,
        dag_run_id: str,
        *,
        dry_run: bool = True,
        only_failed: bool = False,
    ) -> TaskInstanceCollectionResponse | DAGRunResponse:
        """Clear a DAG run's task instances."""
        body: dict[str, Any] = {
            "dry_run": dry_run,
            "only_failed": only_failed,
        }
        resp = self._http.post(f"/api/v2/dags/{dag_id}/dagRuns/{dag_run_id}/clear", json=body)
        resp.raise_for_status()
        data = resp.json()
        if dry_run:
            return TaskInstanceCollectionResponse.model_construct(
                total_entries=data["total_entries"],
                task_instances=[TaskInstanceResponse.model_validate(ti) for ti in data["task_instances"]],
            )
        return DAGRunResponse.model_validate(data)
