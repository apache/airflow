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
from __future__ import annotations

from collections.abc import Collection, Iterable, Iterator
from functools import cached_property
from typing import TYPE_CHECKING, Any
from urllib.parse import quote

from asgiref.sync import sync_to_async
from packaging.version import Version

from airflow.providers.common.compat.sdk import AirflowOptionalProviderFeatureException
from airflow.providers.http.hooks.http import HttpHook
from airflow.providers.http.version_compat import AIRFLOW_V_3_0_PLUS

if not AIRFLOW_V_3_0_PLUS:
    raise AirflowOptionalProviderFeatureException("Waiting for a remote Airflow deployment needs Airflow 3+.")

from airflow.providers.standard.triggers.external_task import WorkflowTrigger

if TYPE_CHECKING:
    from datetime import datetime

    from requests import Response, Session

# The ``task_group_id`` filter of the task instances endpoint was added in Airflow 3.2.0.
# Older API servers silently ignore it and would return the task instances of all tasks.
MIN_TASK_GROUP_FILTER_VERSION = Version("3.2.0")


class _AirflowApiClient:
    """
    Query the REST API (v2) of the remote Airflow deployment of an HTTP connection.

    Login and password of the connection are exchanged for a JWT access token of the remote auth
    manager, a password without login is used as bearer token as-is.
    """

    page_limit = 100

    def __init__(self, http_conn_id: str) -> None:
        self.http_conn_id = http_conn_id
        self._hook = HttpHook(method="GET", http_conn_id=http_conn_id)
        self._session: Session | None = None
        self._uses_access_token = False
        self._remote_version: Version | None = None

    def _create_session(self) -> Session:
        session = self._hook.get_conn()
        # Airflow 3 API servers reject the basic auth HttpHook derives from the connection login.
        session.auth = None
        connection = self._hook.get_connection(self.http_conn_id)
        self._uses_access_token = bool(connection.login)
        if connection.login:
            response = session.post(
                self._hook.url_from_endpoint("auth/token"),
                json={"username": connection.login, "password": connection.password},
                timeout=self._hook.merged_extra.get("timeout"),
            )
            response.raise_for_status()
            session.headers["Authorization"] = f"Bearer {response.json()['access_token']}"
        elif connection.password:
            session.headers["Authorization"] = f"Bearer {connection.password}"
        return session

    def _request(self, method: str, endpoint: str, **kwargs: Any) -> Any:
        if self._session is None:
            self._session = self._create_session()
        url = self._hook.url_from_endpoint(f"api/v2/{endpoint}")
        timeout = self._hook.merged_extra.get("timeout")
        response: Response = self._session.request(method, url, timeout=timeout, **kwargs)
        if response.status_code == 401 and self._uses_access_token:
            # Access tokens expire, which long-running sensors and triggers outlive.
            self._session = self._create_session()
            response = self._session.request(method, url, timeout=timeout, **kwargs)
        response.raise_for_status()
        return response.json()

    def get_dr_count(
        self, dag_id: str, logical_dates: Iterable[datetime], states: Collection[str] | None
    ) -> int:
        return sum(
            self._request(
                "GET",
                f"dags/{quote(dag_id, safe='')}/dagRuns",
                params={
                    "logical_date_gte": logical_date.isoformat(),
                    "logical_date_lte": logical_date.isoformat(),
                    "state": list(states or []),
                    "limit": 1,
                },
            )["total_entries"]
            for logical_date in logical_dates
        )

    def get_ti_count(
        self,
        dag_id: str,
        task_ids: Collection[str],
        logical_dates: Iterable[datetime],
        states: Collection[str] | None,
    ) -> int:
        return sum(
            self._request(
                "POST",
                "dags/~/dagRuns/~/taskInstances/list",
                json={
                    "dag_ids": [dag_id],
                    "task_ids": list(task_ids),
                    "state": list(states) if states else None,
                    "logical_date_gte": logical_date.isoformat(),
                    "logical_date_lte": logical_date.isoformat(),
                    "page_limit": 1,
                },
            )["total_entries"]
            for logical_date in logical_dates
        )

    def get_task_group_states(
        self, dag_id: str, task_group_id: str, logical_dates: Iterable[datetime]
    ) -> dict[str, dict[str, Any]]:
        version = self._get_remote_version()
        if version.release < MIN_TASK_GROUP_FILTER_VERSION.release:
            raise ValueError(
                f"Waiting for a task group requires the remote Airflow deployment to run Airflow "
                f"{MIN_TASK_GROUP_FILTER_VERSION} or later, but it runs Airflow {version}."
            )
        task_states: dict[str, dict[str, Any]] = {}
        for logical_date in logical_dates:
            for ti in self._iter_task_group_instances(dag_id, task_group_id, logical_date):
                # Keyed like the Execution API's task states to reuse the same state matching.
                key = ti["task_id"] if ti["map_index"] < 0 else f"{ti['task_id']}_{ti['map_index']}"
                task_states.setdefault(ti["dag_run_id"], {})[key] = ti["state"]
        return task_states

    def _iter_task_group_instances(
        self, dag_id: str, task_group_id: str, logical_date: datetime
    ) -> Iterator[dict[str, Any]]:
        offset = 0
        while True:
            page = self._request(
                "GET",
                f"dags/{quote(dag_id, safe='')}/dagRuns/~/taskInstances",
                params={
                    "task_group_id": task_group_id,
                    "logical_date_gte": logical_date.isoformat(),
                    "logical_date_lte": logical_date.isoformat(),
                    "order_by": "id",
                    "limit": self.page_limit,
                    "offset": offset,
                },
            )
            yield from page["task_instances"]
            offset += len(page["task_instances"])
            if not page["task_instances"] or offset >= page["total_entries"]:
                return

    def _get_remote_version(self) -> Version:
        if self._remote_version is None:
            self._remote_version = Version(self._request("GET", "version")["version"])
        return self._remote_version


class HttpExternalTaskTrigger(WorkflowTrigger):
    """
    Wait for a Dag, task group or task of a remote Airflow deployment to reach a state.

    Behaves like :class:`~airflow.providers.standard.triggers.external_task.WorkflowTrigger`,
    but queries the REST API (v2) of the remote Airflow 3 deployment configured in the HTTP connection.
    Only ``logical_dates`` are supported to select the remote Dag runs.

    :param http_conn_id: :ref:`http connection<howto/connection:http>` of the remote Airflow deployment.

    All other parameters are the same as those of
    :class:`~airflow.providers.standard.triggers.external_task.WorkflowTrigger`.
    """

    def __init__(self, *, http_conn_id: str, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.http_conn_id = http_conn_id

    def serialize(self) -> tuple[str, dict[str, Any]]:
        classpath, data = super().serialize()
        return classpath, {**data, "http_conn_id": self.http_conn_id}

    @cached_property
    def _client(self) -> _AirflowApiClient:
        return _AirflowApiClient(self.http_conn_id)

    async def _get_dr_count(self, states: Collection[str] | None) -> int:
        return await sync_to_async(self._client.get_dr_count)(
            self.external_dag_id, self.logical_dates or [], states
        )

    async def _get_ti_count(self, states: Collection[str] | None) -> int:
        if TYPE_CHECKING:
            assert self.external_task_ids
        return await sync_to_async(self._client.get_ti_count)(
            self.external_dag_id, self.external_task_ids, self.logical_dates or [], states
        )

    async def _get_task_group_states(self) -> dict[str, dict[str, Any]]:
        if TYPE_CHECKING:
            assert self.external_task_group_id
        return await sync_to_async(self._client.get_task_group_states)(
            self.external_dag_id, self.external_task_group_id, self.logical_dates or []
        )
