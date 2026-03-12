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

import boto3
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from airflow_e2e_tests.constants import (
    AIRFLOW_WWW_USER_PASSWORD,
    AIRFLOW_WWW_USER_USERNAME,
    DOCKER_COMPOSE_HOST_PORT,
)


def get_s3_client():
    """Return a boto3 S3 client configured to use the local LocalStack endpoint."""
    return boto3.client(
        "s3",
        endpoint_url="http://localhost:4566",
        aws_access_key_id="test",
        aws_secret_access_key="test",
        region_name="us-east-1",
    )


def create_request_session_with_retries(status_forcelist: list[int]):
    """Create a requests Session with retry logic for handling transient errors."""
    Retry.DEFAULT_BACKOFF_MAX = 32
    retry_strategy = Retry(
        total=10,
        backoff_factor=1,
        status_forcelist=status_forcelist,
    )
    session = requests.Session()
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session


class AirflowClient:
    """Client for interacting with the Airflow REST API."""

    def __init__(self):
        self.session = create_request_session_with_retries(status_forcelist=[429])

    @cached_property
    def token(self):
        session = create_request_session_with_retries(status_forcelist=[429, 500, 502, 503, 504])

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

    def get_task_instances(self, dag_id: str, run_id: str):
        """Get task instances for a given DAG run."""
        return self._make_request(
            method="GET",
            endpoint=f"dags/{dag_id}/dagRuns/{run_id}/taskInstances",
        )

    def get_task_logs(
        self, dag_id: str, run_id: str, task_id: str, try_number: int = 1, map_index: int | None = None
    ):
        """Get task logs via API."""
        endpoint = f"dags/{dag_id}/dagRuns/{run_id}/taskInstances/{task_id}/logs/{try_number}"
        if map_index is not None:
            endpoint += f"?map_index={map_index}"
        return self._make_request(
            method="GET",
            endpoint=endpoint,
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
