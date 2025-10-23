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


def test_ti_finish_failed(sdk_client, task_instance_id):
    """
    Test finishing a task instance with failed state.

    This is the LAST test and will terminate the long-running task.
    It must run after all other tests that need the task to be running.
    """
    console.print("[yellow]Finishing task instance as FAILED...")

    # Finish the task with failed state
    sdk_client.task_instances.finish(
        id=task_instance_id, state=TerminalStateNonSuccess.FAILED, when=utcnow(), rendered_map_index="-1"
    )

    console.print(" Task Finish Response ".center(72, "="))
    console.print("[bright_blue]Status:[/] Success (204 No Content)")
    console.print("[bright_blue]Final State:[/] FAILED")
    console.print(f"[bright_blue]Task Instance ID:[/] {task_instance_id}")
    console.print("=" * 72)

    console.print("[green]✅ Task instance finished successfully!")
