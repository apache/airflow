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
"""Task Instances resource sub-client for the local REST client."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    import httpx


class TaskInstancesClient:
    """Client for ``/api/v2/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances`` endpoints."""

    def __init__(self, http: httpx.Client) -> None:
        self._http = http

    def get(
        self,
        dag_id: str,
        dag_run_id: str,
        task_id: str,
        *,
        map_index: int | None = None,
    ) -> dict[str, Any]:
        """Get a task instance by ID, optionally with map_index for mapped tasks."""
        base = f"/api/v2/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances/{task_id}"
        if map_index is not None:
            base = f"{base}/{map_index}"
        resp = self._http.get(base)
        resp.raise_for_status()
        return resp.json()

    def list(
        self,
        dag_id: str,
        dag_run_id: str,
        *,
        limit: int = 100,
        offset: int = 0,
    ) -> dict[str, Any]:
        """
        List task instances for a DAG run.

        Use ``dag_id="~"`` and/or ``dag_run_id="~"`` to list across all DAGs/runs.
        """
        resp = self._http.get(
            f"/api/v2/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances",
            params={"limit": limit, "offset": offset},
        )
        resp.raise_for_status()
        return resp.json()

    def update(
        self,
        dag_id: str,
        dag_run_id: str,
        task_id: str,
        *,
        new_state: str | None = None,
        note: str | None = None,
        map_index: int | None = None,
    ) -> dict[str, Any]:
        """Update a task instance (set state or note)."""
        body: dict[str, Any] = {}
        if new_state is not None:
            body["new_state"] = new_state
        if note is not None:
            body["note"] = note
        base = f"/api/v2/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances/{task_id}"
        if map_index is not None:
            base = f"{base}/{map_index}"
        resp = self._http.patch(base, json=body)
        resp.raise_for_status()
        return resp.json()

    def clear(
        self,
        dag_id: str,
        *,
        task_ids: list[str] | None = None,
        dag_run_id: str | None = None,
        start_date: str | None = None,
        end_date: str | None = None,
        dry_run: bool = True,
        only_failed: bool = False,
        only_running: bool = False,
        reset_dag_runs: bool = True,
    ) -> dict[str, Any]:
        """Clear task instances for a DAG."""
        body: dict[str, Any] = {
            "dry_run": dry_run,
            "only_failed": only_failed,
            "only_running": only_running,
            "reset_dag_runs": reset_dag_runs,
        }
        if task_ids is not None:
            body["task_ids"] = task_ids
        if dag_run_id is not None:
            body["dag_run_id"] = dag_run_id
        if start_date is not None:
            body["start_date"] = start_date
        if end_date is not None:
            body["end_date"] = end_date
        resp = self._http.post(f"/api/v2/dags/{dag_id}/clearTaskInstances", json=body)
        resp.raise_for_status()
        return resp.json()
