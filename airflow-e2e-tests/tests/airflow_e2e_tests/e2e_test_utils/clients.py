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

import time
from datetime import datetime, timezone
from functools import cached_property

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from airflow_e2e_tests.constants import (
    AIRFLOW_WWW_USER_PASSWORD,
    AIRFLOW_WWW_USER_USERNAME,
    DOCKER_COMPOSE_HOST_PORT,
)


class AirflowClient:
    """Client for interacting with the Airflow REST API."""

    def __init__(self):
        self.session = requests.Session()

    @cached_property
    def token(self):
        Retry.DEFAULT_BACKOFF_MAX = 32
        retry = Retry(total=10, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
        session = requests.Session()
        session.mount("http://", HTTPAdapter(max_retries=retry))
        session.mount("https://", HTTPAdapter(max_retries=retry))

        api_server_url = DOCKER_COMPOSE_HOST_PORT
        if not api_server_url.startswith(("http://", "https://")):
            api_server_url = "http://" + DOCKER_COMPOSE_HOST_PORT

        url = f"{api_server_url}/auth/token"

        login_response = session.post(
            url,
            json={"username": AIRFLOW_WWW_USER_USERNAME, "password": AIRFLOW_WWW_USER_PASSWORD},
        )
        access_token = login_response.json().get("access_token")

        assert access_token, (
            f"Failed to get JWT token from redirect url {url} with status code {login_response}"
        )
        return access_token

    def _make_request(
        self,
        method: str,
        endpoint: str,
        base_url: str = f"http://{DOCKER_COMPOSE_HOST_PORT}/api/v2",
        **kwargs,
    ):
        response = requests.request(
            method=method,
            url=f"{base_url}/{endpoint}",
            headers={"Authorization": f"Bearer {self.token}", "Content-Type": "application/json"},
            **kwargs,
        )
        response.raise_for_status()
        return response.json()

    def un_pause_dag(self, dag_id: str):
        return self._make_request(
            method="PATCH",
            endpoint=f"dags/{dag_id}",
            json={"is_paused": False},
        )

    def trigger_dag(self, dag_id: str, json=None):
        if json is None:
            json = {}
        return self._make_request(method="POST", endpoint=f"dags/{dag_id}/dagRuns", json=json)

    def wait_for_dag_run(self, dag_id: str, run_id: str, timeout=300, check_interval=5):
        start_time = time.time()
        while time.time() - start_time < timeout:
            response = self._make_request(
                method="GET",
                endpoint=f"dags/{dag_id}/dagRuns/{run_id}",
            )
            state = response.get("state")
            if state in {"success", "failed"}:
                return state
            time.sleep(check_interval)
        raise TimeoutError(f"DAG run {run_id} for DAG {dag_id} did not complete within {timeout} seconds.")

    def get_xcom_value(self, dag_id: str, task_id: str, run_id: str, key: str, map_index=-1):
        return self._make_request(
            method="GET",
            endpoint=f"dags/{dag_id}/dagRuns/{run_id}/taskInstances/{task_id}/xcomEntries/{key}?map_index={map_index}",
        )

    def trigger_dag_and_wait(self, dag_id: str, json=None):
        """Trigger a DAG and wait for it to complete."""
        self.un_pause_dag(dag_id)

        resp = self.trigger_dag(dag_id, json=json or {"logical_date": datetime.now(timezone.utc).isoformat()})

        # Wait for the DAG run to complete
        return self.wait_for_dag_run(
            dag_id=dag_id,
            run_id=resp["dag_run_id"],
        )


class TaskSDKClient:
    """Client for interacting with the Task SDK API."""

    def __init__(self):
        pass

    @cached_property
    def client(self):
        from airflow.sdk.api.client import Client

        client = Client(base_url=f"http://{DOCKER_COMPOSE_HOST_PORT}/execution", token="not-a-token")
        return client

    def health_check(self):
        response = self.client.get("health/ping", headers={"Airflow-API-Version": "2025-08-10"})
        return response
