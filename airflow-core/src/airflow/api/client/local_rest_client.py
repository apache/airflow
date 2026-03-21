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
"""
Local REST API client for trusted Airflow processes.

Provides a zero-configuration way for code running inside trusted Airflow
processes (scheduler, DAG processor, triggerer, plugins) to call Core API
endpoints without needing user credentials.  The calling process must have
access to the Airflow metadata database.

Example usage::

    from airflow.api.client import get_local_rest_client

    client = get_local_rest_client()
    client.pools.create(name="my_pool", slots=5)
    pools = client.pools.list()
"""

from __future__ import annotations

import json
import logging
import os
from functools import cached_property

import httpx

from airflow import settings
from airflow.api.client.in_process_core_api import InProcessCoreAPI
from airflow.api.client.resources.assets import AssetsClient
from airflow.api.client.resources.config import ConfigClient
from airflow.api.client.resources.connections import ConnectionsClient
from airflow.api.client.resources.dag_runs import DagRunsClient
from airflow.api.client.resources.dags import DagsClient
from airflow.api.client.resources.pools import PoolsClient
from airflow.api.client.resources.task_instances import TaskInstancesClient
from airflow.api.client.resources.variables import VariablesClient
from airflow.configuration import AIRFLOW_HOME, conf

_log = logging.getLogger(__name__)


class LocalRESTClient:
    """
    REST API client for use inside trusted Airflow processes.

    Uses an in-process ASGI transport to call the Core API without network
    overhead, with authentication bypassed via a ``SystemUser`` override.

    :param process_type: Identifier for the calling process, used in audit logs.
        Defaults to ``"unknown"``.
    """

    def __init__(self, *, process_type: str = "unknown") -> None:
        self._process_type = process_type
        self._in_process_api = InProcessCoreAPI(process_type=process_type)

    @cached_property
    def _http(self) -> httpx.Client:
        if self._is_orm_access_blocked():
            base_url = self._get_http_fallback_base_url()
            token = self._get_http_bearer_token(base_url=base_url)
            if not token:
                raise RuntimeError(
                    "LocalRESTClient cannot use in-process Core API because ORM access is blocked in this "
                    "process, and it could not obtain an auth token for HTTP fallback."
                )
            return httpx.Client(
                base_url=base_url,
                headers={"Authorization": f"Bearer {token}"},
            )

        return httpx.Client(
            transport=self._in_process_api.transport,
            base_url="http://in-process",
        )

    def _is_orm_access_blocked(self) -> bool:
        # Task SDK worker subprocesses replace settings.Session with BlockedDBSession.
        session_factory = getattr(settings, "Session", None)
        return (
            session_factory is not None
            and getattr(session_factory, "__module__", "") == "airflow.sdk.execution_time.supervisor"
            and getattr(session_factory, "__name__", "") == "BlockedDBSession"
        )

    def _get_http_fallback_base_url(self) -> str:
        return conf.get("api", "base_url", fallback="http://localhost:8080").rstrip("/")

    def _get_http_bearer_token(self, *, base_url: str) -> str | None:
        with httpx.Client(base_url=base_url) as http:
            # In standalone all-admin mode, this endpoint returns a token with no credentials.
            token_resp = http.get("/auth/token")
            if token_resp.is_success:
                token = token_resp.json().get("access_token")
                if token:
                    return token

            for username, password in self._iter_simple_auth_credentials():
                cli_token_resp = http.post(
                    "/auth/token/cli",
                    json={"username": username, "password": password},
                )
                if cli_token_resp.is_success:
                    token = cli_token_resp.json().get("access_token")
                    if token:
                        return token

        return None

    def _iter_simple_auth_credentials(self) -> list[tuple[str, str]]:
        users = conf.getlist("core", "simple_auth_manager_users", fallback=[])
        usernames = [raw_user.split(":", 1)[0] for raw_user in users if raw_user]
        if not usernames:
            return []

        password_file = conf.get("core", "simple_auth_manager_passwords_file", fallback=None)
        if not password_file:
            password_file = os.path.join(AIRFLOW_HOME, "simple_auth_manager_passwords.json.generated")

        try:
            with open(password_file) as file:
                passwords = json.load(file)
        except (FileNotFoundError, PermissionError, json.JSONDecodeError):
            _log.debug("Could not read SimpleAuthManager password file: %s", password_file)
            return []

        return [(username, passwords[username]) for username in usernames if username in passwords]

    @cached_property
    def pools(self) -> PoolsClient:
        """Access pool management endpoints."""
        return PoolsClient(self._http)

    @cached_property
    def dags(self) -> DagsClient:
        """Access DAG management endpoints."""
        return DagsClient(self._http)

    @cached_property
    def dag_runs(self) -> DagRunsClient:
        """Access DAG run management endpoints."""
        return DagRunsClient(self._http)

    @cached_property
    def connections(self) -> ConnectionsClient:
        """Access connection management endpoints."""
        return ConnectionsClient(self._http)

    @cached_property
    def variables(self) -> VariablesClient:
        """Access variable management endpoints."""
        return VariablesClient(self._http)

    @cached_property
    def task_instances(self) -> TaskInstancesClient:
        """Access task instance endpoints."""
        return TaskInstancesClient(self._http)

    @cached_property
    def config(self) -> ConfigClient:
        """Access configuration endpoints (read-only)."""
        return ConfigClient(self._http)

    @cached_property
    def assets(self) -> AssetsClient:
        """Access asset management endpoints."""
        return AssetsClient(self._http)

    def close(self) -> None:
        """Close the underlying HTTP client."""
        if "_http" in self.__dict__:
            self._http.close()
