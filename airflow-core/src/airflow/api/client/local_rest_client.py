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
processes (DAG processor, scheduler, workers, triggerer) to call Core API
endpoints without needing user credentials.

Example usage::

    from airflow.api.client import get_local_rest_client

    client = get_local_rest_client()
    client.pools.create(name="my_pool", slots=5)
    pools = client.pools.list()
"""

from __future__ import annotations

from functools import cached_property

import httpx

from airflow.api.client.in_process_core_api import InProcessCoreAPI
from airflow.api.client.resources.assets import AssetsClient
from airflow.api.client.resources.config import ConfigClient
from airflow.api.client.resources.connections import ConnectionsClient
from airflow.api.client.resources.dag_runs import DagRunsClient
from airflow.api.client.resources.dags import DagsClient
from airflow.api.client.resources.pools import PoolsClient
from airflow.api.client.resources.task_instances import TaskInstancesClient
from airflow.api.client.resources.variables import VariablesClient


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
        return httpx.Client(
            transport=self._in_process_api.transport,
            base_url="http://in-process",
        )

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
