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
"""E2E tests for dag.test() method."""

from __future__ import annotations

import json
import time

from airflow_e2e_tests.conftest import _E2ETestState
from airflow_e2e_tests.e2e_test_utils.clients import AirflowClient


def _execute_dag_test(dag_id: str, use_executor: bool = False) -> dict:
    """
    Execute dag.test() inside the Airflow scheduler container and query state via REST API.

    Returns a dictionary with test results including dag_run state and task states.
    """
    # Minimal Python code to execute dag.test() and return run_id
    python_code = f"""
import json
import sys
from airflow.dag_processing.dagbag import DagBag

dagbag = DagBag(dag_folder="/opt/airflow/dags", include_examples=True)
dag = dagbag.get_dag("{dag_id}")

if dag is None:
    print(f"ERROR: DAG {dag_id} not found")
    sys.exit(1)

# Execute dag.test()
try:
    dr = dag.test(use_executor={str(use_executor)})
    result = {{"dag_run_id": dr.run_id}}
    print(json.dumps(result))
except Exception as e:
    print(f"ERROR: {{str(e)}}")
    import traceback
    traceback.print_exc()
    sys.exit(1)
"""
    # Execute the Python code in the scheduler container
    # Try scheduler first, fallback to dag-processor if scheduler doesn't exist
    # exec_in_container returns (stdout, stderr, exit_code) tuple
    try:
        stdout, stderr, exit_code = _E2ETestState.compose_instance.exec_in_container(
            command=["python", "-c", python_code],
            service_name="airflow-scheduler",
        )
    except Exception:
        # Fallback to dag-processor if scheduler service doesn't exist
        stdout, stderr, exit_code = _E2ETestState.compose_instance.exec_in_container(
            command=["python", "-c", python_code],
            service_name="airflow-dag-processor",
        )

    # Check exit code
    if exit_code != 0:
        raise RuntimeError(f"Command failed with exit code {exit_code}. stdout: {stdout}, stderr: {stderr}")

    if stderr:
        # Check if there's an actual error (not just warnings)
        if "ERROR" in stderr or "Traceback" in stderr:
            raise RuntimeError(f"Error executing dag.test(): {stderr}")

    # Parse the JSON output to get run_id
    output_lines = stdout.strip().split("\n")
    run_id = None
    for line in output_lines:
        if line.startswith("{") and "dag_run_id" in line:
            try:
                result = json.loads(line)
                run_id = result.get("dag_run_id")
                break
            except json.JSONDecodeError:
                continue

    if run_id is None:
        raise RuntimeError(f"Could not parse run_id from dag.test() output. stdout: {stdout}, stderr: {stderr}")

    # Wait a bit for state to propagate (especially for executor mode)
    if use_executor:
        time.sleep(2)

    # Use AirflowClient to query state via REST API
    airflow_client = AirflowClient()

    # Wait for DagRun to complete and get state
    dag_run_state = airflow_client.wait_for_dag_run(dag_id, run_id, timeout=300, check_interval=2)

    # Get task instances
    task_instances_response = airflow_client.get_dag_run_task_instances(dag_id, run_id)
    task_states = {
        ti["task_id"]: ti["state"]
        for ti in task_instances_response.get("task_instances", [])
    }

    return {
        "dag_run_state": dag_run_state,
        "dag_run_id": run_id,
        "task_states": task_states,
        "success": dag_run_state == "success",
    }


class TestDagTestMethod:
    """E2E tests for dag.test() method using example DAGs."""

    def test_dag_test_without_executor_simple(self):
        """Test dag.test() without executor for simple DAG."""
        result = _execute_dag_test("example_simplest_dag", use_executor=False)

        assert result["success"], f"DAG test failed. Result: {result}"
        assert result["dag_run_state"] == "success"
        # Verify all tasks completed successfully
        for task_id, state in result["task_states"].items():
            assert state == "success", f"Task {task_id} did not succeed. State: {state}"

    def test_dag_test_with_executor_simple(self):
        """Test dag.test() with executor for simple DAG."""
        result = _execute_dag_test("example_simplest_dag", use_executor=True)

        assert result["success"], f"DAG test failed. Result: {result}"
        assert result["dag_run_state"] == "success"
        # Verify all tasks completed successfully
        for task_id, state in result["task_states"].items():
            assert state == "success", f"Task {task_id} did not succeed. State: {state}"

    def test_dag_test_without_executor_xcom(self):
        """Test dag.test() without executor for DAG with XCom."""
        result = _execute_dag_test("example_xcom", use_executor=False)

        assert result["success"], f"DAG test failed. Result: {result}"
        assert result["dag_run_state"] == "success"
        # Verify all tasks completed successfully
        for task_id, state in result["task_states"].items():
            assert state == "success", f"Task {task_id} did not succeed. State: {state}"

    def test_dag_test_with_executor_xcom(self):
        """Test dag.test() with executor for DAG with XCom."""
        result = _execute_dag_test("example_xcom", use_executor=True)

        assert result["success"], f"DAG test failed. Result: {result}"
        assert result["dag_run_state"] == "success"
        # Verify all tasks completed successfully
        for task_id, state in result["task_states"].items():
            assert state == "success", f"Task {task_id} did not succeed. State: {state}"

    def test_dag_test_without_executor_branching(self):
        """Test dag.test() without executor for DAG with branching."""
        result = _execute_dag_test("example_branch_operator", use_executor=False)

        assert result["success"], f"DAG test failed. Result: {result}"
        assert result["dag_run_state"] == "success"
        # Verify all tasks completed successfully
        for task_id, state in result["task_states"].items():
            assert state in ["success", "skipped"], (
                f"Task {task_id} did not succeed or was skipped. State: {state}"
            )

    def test_dag_test_with_executor_branching(self):
        """Test dag.test() with executor for DAG with branching."""
        result = _execute_dag_test("example_branch_operator", use_executor=True)

        assert result["success"], f"DAG test failed. Result: {result}"
        assert result["dag_run_state"] == "success"
        # Verify all tasks completed successfully
        for task_id, state in result["task_states"].items():
            assert state in ["success", "skipped"], (
                f"Task {task_id} did not succeed or was skipped. State: {state}"
            )

    def test_dag_test_without_executor_task_groups(self):
        """Test dag.test() without executor for DAG with task groups."""
        result = _execute_dag_test("example_task_group", use_executor=False)

        assert result["success"], f"DAG test failed. Result: {result}"
        assert result["dag_run_state"] == "success"
        # Verify all tasks completed successfully
        for task_id, state in result["task_states"].items():
            assert state == "success", f"Task {task_id} did not succeed. State: {state}"

    def test_dag_test_with_executor_task_groups(self):
        """Test dag.test() with executor for DAG with task groups."""
        result = _execute_dag_test("example_task_group", use_executor=True)

        assert result["success"], f"DAG test failed. Result: {result}"
        assert result["dag_run_state"] == "success"
        # Verify all tasks completed successfully
        for task_id, state in result["task_states"].items():
            assert state == "success", f"Task {task_id} did not succeed. State: {state}"

