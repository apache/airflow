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

import requests

from airflow.sdk.api.datamodels._generated import (
    InactiveAssetsResponse,
    PrevSuccessfulDagRunResponse,
    TaskStatesResponse,
    TerminalStateNonSuccess,
)
from airflow.sdk.execution_time.comms import TICount
from airflow.sdk.timezone import utcnow
from task_sdk_tests import console


def test_ti_get_previous_successful_dagrun(sdk_client, task_instance_id):
    """Test getting previous successful DAG run for a task instance."""
    console.print("[yellow]Getting previous successful DAG run...")

    response = sdk_client.task_instances.get_previous_successful_dagrun(task_instance_id)

    console.print(" Previous Successful DAG Run ".center(72, "="))
    console.print(f"[bright_blue]Response Type:[/] {type(response).__name__}")
    console.print(f"[bright_blue]Data Interval Start:[/] {response.data_interval_start}")
    console.print(f"[bright_blue]Data Interval End:[/] {response.data_interval_end}")
    console.print(f"[bright_blue]Start Date:[/] {response.start_date}")
    console.print(f"[bright_blue]End Date:[/] {response.end_date}")
    console.print("=" * 72)

    assert isinstance(response, PrevSuccessfulDagRunResponse)
    console.print("[green]✅ Previous DAG run test passed!")


def test_ti_validate_inlets_and_outlets(sdk_client, task_instance_id):
    """Test validating inlets and outlets for inactive assets."""
    console.print("[yellow]Validating inlets and outlets...")

    response = sdk_client.task_instances.validate_inlets_and_outlets(task_instance_id)

    console.print(" Validate Inlets/Outlets ".center(72, "="))
    console.print(f"[bright_blue]Response Type:[/] {type(response).__name__}")
    console.print(f"[bright_blue]Inactive Assets Count:[/] {len(response.inactive_assets)}")
    console.print(f"[bright_blue]Inactive Assets:[/] {response.inactive_assets}")
    console.print("=" * 72)

    assert isinstance(response, InactiveAssetsResponse)
    assert isinstance(response.inactive_assets, list)
    console.print("[green]✅ Validate inlets/outlets test passed!")


def test_ti_get_count(sdk_client, dag_info):
    """Test getting count of task instances for a DAG."""
    console.print("[yellow]Getting task instance count...")

    response = sdk_client.task_instances.get_count(dag_id=dag_info["dag_id"])

    console.print(" Task Instance Count ".center(72, "="))
    console.print(f"[bright_blue]DAG ID:[/] {dag_info['dag_id']}")
    console.print(f"[bright_blue]Count:[/] {response.count}")
    console.print("=" * 72)

    assert isinstance(response, TICount)
    assert response.count >= 1, f"Expected at least 1 task instance, got {response.count}"
    console.print("[green]✅ Task instance count test passed!")


def test_ti_get_task_states(sdk_client, dag_info):
    """Test getting task states for a DAG run."""
    console.print("[yellow]Getting task states...")

    response = sdk_client.task_instances.get_task_states(
        dag_id=dag_info["dag_id"], run_ids=[dag_info["dag_run_id"]]
    )

    console.print(" Task States ".center(72, "="))
    console.print(f"[bright_blue]Response Type:[/] {type(response).__name__}")
    console.print(f"[bright_blue]DAG ID:[/] {dag_info['dag_id']}")
    console.print(f"[bright_blue]Run ID:[/] {dag_info['dag_run_id']}")
    console.print(f"[bright_blue]Task States:[/] {response.task_states}")
    console.print("=" * 72)

    assert isinstance(response, TaskStatesResponse)
    assert isinstance(response.task_states, dict)
    console.print("[green]✅ Task states test passed!")


def test_ti_set_rtif(sdk_client, task_instance_id):
    """
    Test setting Rendered Task Instance Fields (RTIF).
    """
    console.print("[yellow]Setting Rendered Task Instance Fields...")

    rtif_data = {
        "rendered_field_1": "test_value_1",
        "rendered_field_2": "1234",
    }

    response = sdk_client.task_instances.set_rtif(task_instance_id, rtif_data)

    console.print(" RTIF Response ".center(72, "="))
    console.print(f"[bright_blue]Response Type:[/] {type(response).__name__}")
    console.print(f"[bright_blue]Status:[/] {response.ok}")
    console.print(f"[bright_blue]Task Instance ID:[/] {task_instance_id}")
    console.print(f"[bright_blue]Fields Set:[/] {list(rtif_data.keys())}")
    console.print("=" * 72)

    assert response.ok is True
    console.print("[green]✅ RTIF test passed!")


def test_ti_heartbeat(sdk_client, task_instance_id, core_api_headers, dag_info, monkeypatch):
    """
    Test sending heartbeat for a running task instance.

    This test fetches the actual worker's PID and hostname from core API,
    then patches get_hostname() to return the worker's hostname, allowing
    the heartbeat to be accepted by the server.
    """
    console.print("[yellow]Getting task instance details for heartbeat...")

    ti_url = (
        f"http://localhost:8080/api/v2/dags/{dag_info['dag_id']}/"
        f"dagRuns/{dag_info['dag_run_id']}/taskInstances/long_running_task/tries/1"
    )
    ti_response = requests.get(ti_url, headers=core_api_headers, timeout=10)
    ti_response.raise_for_status()

    ti_data = ti_response.json()
    worker_hostname = ti_data.get("hostname")
    worker_pid = ti_data.get("pid")

    console.print(" Worker Information ".center(72, "="))
    console.print(f"[bright_blue]Worker Hostname:[/] {worker_hostname}")
    console.print(f"[bright_blue]Worker PID:[/] {worker_pid}")
    console.print("=" * 72)

    assert worker_hostname is not None
    assert worker_pid is not None

    # Patch get_hostname to return the worker's hostname
    from airflow.sdk.api import client as sdk_client_module

    monkeypatch.setattr(sdk_client_module, "get_hostname", lambda: worker_hostname)

    console.print("[yellow]Sending heartbeat with worker's PID/hostname...")

    sdk_client.task_instances.heartbeat(task_instance_id, pid=worker_pid)

    console.print(" Heartbeat Response ".center(72, "="))
    console.print("[bright_blue]Status:[/] Success (204 No Content)")
    console.print(f"[bright_blue]Task Instance ID:[/] {task_instance_id}")
    console.print(f"[bright_blue]Used PID:[/] {worker_pid}")
    console.print(f"[bright_blue]Used Hostname:[/] {worker_hostname}")
    console.print("=" * 72)

    console.print("[green]✅ Heartbeat test passed!")


def test_ti_state_transitions(sdk_client, task_instance_id):
    """
    Test task instance state transition to terminal state.
    """
    console.print("[yellow]Testing state transition: RUNNING → FAILED...")
    sdk_client.task_instances.finish(
        id=task_instance_id, state=TerminalStateNonSuccess.FAILED, when=utcnow(), rendered_map_index="-1"
    )

    console.print(" State: FAILED (Terminal) ".center(72, "="))
    console.print("[bright_blue]Transition:[/] RUNNING → FAILED")
    console.print("[bright_blue]Status:[/] Success (204 No Content)")
    console.print("[bright_blue]Final State:[/] FAILED")
    console.print(f"[bright_blue]Task Instance ID:[/] {task_instance_id}")
    console.print("=" * 72)
    console.print("[green]✅ Successfully transitioned to FAILED terminal state!")
