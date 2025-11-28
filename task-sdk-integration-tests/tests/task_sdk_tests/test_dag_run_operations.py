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
Integration tests for DAG Run operations.

These tests validate the Execution API endpoints for DAG Run operations:
- get_state(): Get DAG run state
- get_count(): Get count of DAG runs
- get_previous(): Get previous DAG run
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from airflow.sdk.api.client import ServerResponseError
from airflow.sdk.api.datamodels._generated import DagRunStateResponse
from airflow.sdk.execution_time.comms import DRCount, PreviousDagRunResult
from task_sdk_tests import console


def test_dag_run_get_state(sdk_client, dag_info):
    """
    Test getting state for DAG run.

    Expected: DagRunStateResponse with running or success state
    Endpoint: GET /execution/dag-runs/{dag_id}/{run_id}/state
    """
    console.print("[yellow]Getting DAG run state...")

    response = sdk_client.dag_runs.get_state(
        dag_id=dag_info["dag_id"],
        run_id=dag_info["dag_run_id"],
    )

    console.print(" DAG Run State Response ".center(72, "="))
    console.print(f"[bright_blue]Response Type:[/] {type(response).__name__}")
    console.print(f"[bright_blue]State:[/] {response.state}")
    console.print("=" * 72)

    assert isinstance(response, DagRunStateResponse)
    assert response.state is not None
    assert response.state in ["running", "success"]
    console.print("[green]✅ DAG run get state test passed!")


def test_dag_run_get_count(sdk_client, dag_info):
    """
    Test getting count of DAG run.

    Expected: DRCount with count >= 1
    Endpoint: GET /execution/dag-runs/count
    """
    console.print("[yellow]Getting DAG run count...")

    response = sdk_client.dag_runs.get_count(
        dag_id=dag_info["dag_id"],
        run_ids=[dag_info["dag_run_id"]],
    )

    console.print(" DAG Run Count Response ".center(72, "="))
    console.print(f"[bright_blue]Response Type:[/] {type(response).__name__}")
    console.print(f"[bright_blue]DAG ID:[/] {dag_info['dag_id']}")
    console.print(f"[bright_blue]Count:[/] {response.count}")
    console.print("=" * 72)

    assert isinstance(response, DRCount)
    assert response.count >= 1, f"Expected at least 1 DAG run, got {response.count}"
    console.print("[green]✅ DAG run get count test passed!")


def test_dag_run_get_state_not_found(sdk_client, dag_info):
    """
    Test getting state for non-existent DAG run.

    Expected: ServerResponseError with 404 status code
    Endpoint: GET /execution/dag-runs/{dag_id}/{run_id}/state
    """
    console.print("[yellow]Getting non-existent DAG run state...")

    with pytest.raises(ServerResponseError) as exc_info:
        sdk_client.dag_runs.get_state(
            dag_id="not_exist",
            run_id=dag_info["dag_run_id"],
        )

    console.print(" Non-existent DAG Run State Response ".center(72, "="))
    console.print(f"[bright_blue]Exception Type:[/] {type(exc_info.value).__name__}")
    console.print(f"[bright_blue]Status Code:[/] {exc_info.value.response.status_code}")
    console.print(f"[bright_blue]Error Message:[/] {str(exc_info.value)}")
    console.print("=" * 72)

    assert exc_info.value.response.status_code == 404
    console.print("[green]✅ DAG run get state (not found) test passed!")


def test_dag_run_get_count_not_found(sdk_client, dag_info):
    """
    Test getting count of non-existent DAG run.

    Expected: DRCount with count=0
    Endpoint: GET /execution/dag-runs/count
    """
    console.print("[yellow]Getting non-existent DAG run count...")

    response = sdk_client.dag_runs.get_count(
        dag_id="not_exist",
        run_ids=[dag_info["dag_run_id"]],
    )
    console.print(" Non-existent DAG Run Count Response ".center(72, "="))
    console.print(f"[bright_blue]Response Type:[/] {type(response).__name__}")
    console.print(f"[bright_blue]Count:[/] {response.count}")
    console.print("=" * 72)

    assert isinstance(response, DRCount)
    assert response.count == 0, f"Expected 0 DAG run, got {response.count}"
    console.print("[green]✅ Non-existent DAG run get count test passed!")


def test_dag_run_get_previous(sdk_client, dag_info):
    """
    Test getting previous DAG run before a logical date.

    Expected: PreviousDagRunResult with dag_run field
    Endpoint: GET /execution/dag-runs/{dag_id}/previous
    """
    console.print("[yellow]Getting previous DAG run state...")

    response = sdk_client.dag_runs.get_previous(
        dag_id=dag_info["dag_id"],
        logical_date=datetime.now(timezone.utc) + timedelta(seconds=10),
    )

    console.print(" Previous DAG Run Response ".center(72, "="))
    console.print(f"[bright_blue]Response Type:[/] {type(response).__name__}")
    console.print(f"[bright_blue]Previous DAG Run:[/] {response.dag_run}")
    console.print("=" * 72)

    assert isinstance(response, PreviousDagRunResult)
    assert response.dag_run is not None
    console.print("[green]✅ Previous DAG run get test passed!")


def test_dag_run_get_previous_not_found(sdk_client):
    """
    Test getting previous DAG run for non-existent DAG.

    Expected: PreviousDagRunResult with dag_run is None
    Endpoint: GET /execution/dag-runs/{dag_id}/previous
    """
    console.print("[yellow]Getting non-existent previous DAG run...")

    response = sdk_client.dag_runs.get_previous(
        dag_id="not_exist",
        logical_date=datetime.now(timezone.utc) + timedelta(seconds=10),
    )

    console.print(" Non-existent Previous DAG Run Response ".center(72, "="))
    console.print(f"[bright_blue]Response Type:[/] {type(response).__name__}")
    console.print(f"[bright_blue]Detail:[/] {response.dag_run}")
    console.print("=" * 72)

    assert isinstance(response, PreviousDagRunResult)
    assert response.dag_run is None
    console.print("[green]✅ Previous DAG run (not found) get test passed!")
