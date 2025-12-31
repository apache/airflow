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

import pytest

from airflow_e2e_tests.conftest import _E2ETestState


def _execute_dag_test(dag_id: str, use_executor: bool = False) -> dict:
    """
    Execute dag.test() inside the Airflow scheduler container.

    Returns a dictionary with test results including dag_run state and task states.
    """
    python_code = f"""
import json
import sys
from airflow.dag_processing.dagbag import DagBag
from airflow.sdk import DagRunState
from airflow.utils.state import TaskInstanceState

# Load the DAG
dagbag = DagBag(dag_folder="/opt/airflow/dags", include_examples=False)
dag = dagbag.get_dag("{dag_id}")

if dag is None:
    print(f"ERROR: DAG {dag_id} not found")
    sys.exit(1)

# Execute dag.test()
try:
    dr = dag.test(use_executor={str(use_executor)})
    
    # Refresh from DB to get latest state
    from airflow import settings
    from airflow.models.dagrun import DagRun
    import time
    session = settings.Session()
    
    # Wait a bit for state to propagate (especially for executor mode)
    if {str(use_executor)}:
        time.sleep(2)
    
    # Re-fetch the DagRun to ensure we have the latest state
    dr = session.query(DagRun).filter(
        DagRun.dag_id == dr.dag_id,
        DagRun.run_id == dr.run_id
    ).first()
    
    if dr is None:
        print(f"ERROR: DagRun not found after test completion")
        sys.exit(1)
    
    # Get task instance states using fetch_task_instances with our session
    task_states = {{}}
    task_instances = DagRun.fetch_task_instances(
        dag_id=dr.dag_id,
        run_id=dr.run_id,
        session=session
    )
    for ti in task_instances:
        task_states[ti.task_id] = ti.state
    
    result = {{
        "dag_run_state": str(dr.state),
        "dag_run_id": dr.run_id,
        "task_states": {{k: str(v) for k, v in task_states.items()}},
        "success": dr.state == DagRunState.SUCCESS
    }}
    
    print(json.dumps(result))
    session.close()
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

    # Parse the JSON output
    output_lines = stdout.strip().split("\n")
    json_output = None
    for line in output_lines:
        if line.startswith("{") and "dag_run_state" in line:
            try:
                json_output = json.loads(line)
                break
            except json.JSONDecodeError:
                continue

    if json_output is None:
        raise RuntimeError(f"Could not parse output from dag.test(). stdout: {stdout}, stderr: {stderr}")

    return json_output


class TestDagTestMethod:
    """E2E tests for dag.test() method."""

    def test_dag_test_without_executor_simple(self):
        """Test dag.test() without executor for simple DAG."""
        result = _execute_dag_test("test_dag_test_simple", use_executor=False)

        assert result["success"], f"DAG test failed. Result: {result}"
        assert result["dag_run_state"] == "success"
        # Verify all tasks completed successfully
        for task_id, state in result["task_states"].items():
            assert state == "success", f"Task {task_id} did not succeed. State: {state}"

    def test_dag_test_with_executor_simple(self):
        """Test dag.test() with executor for simple DAG."""
        result = _execute_dag_test("test_dag_test_simple", use_executor=True)

        assert result["success"], f"DAG test failed. Result: {result}"
        assert result["dag_run_state"] == "success"
        # Verify all tasks completed successfully
        for task_id, state in result["task_states"].items():
            assert state == "success", f"Task {task_id} did not succeed. State: {state}"

    def test_dag_test_without_executor_xcom(self):
        """Test dag.test() without executor for DAG with XCom."""
        result = _execute_dag_test("test_dag_test_xcom", use_executor=False)

        assert result["success"], f"DAG test failed. Result: {result}"
        assert result["dag_run_state"] == "success"
        # Verify all tasks completed successfully
        for task_id, state in result["task_states"].items():
            assert state == "success", f"Task {task_id} did not succeed. State: {state}"

    def test_dag_test_with_executor_xcom(self):
        """Test dag.test() with executor for DAG with XCom."""
        result = _execute_dag_test("test_dag_test_xcom", use_executor=True)

        assert result["success"], f"DAG test failed. Result: {result}"
        assert result["dag_run_state"] == "success"
        # Verify all tasks completed successfully
        for task_id, state in result["task_states"].items():
            assert state == "success", f"Task {task_id} did not succeed. State: {state}"

    def test_dag_test_without_executor_branching(self):
        """Test dag.test() without executor for DAG with branching."""
        result = _execute_dag_test("test_dag_test_branching", use_executor=False)

        assert result["success"], f"DAG test failed. Result: {result}"
        assert result["dag_run_state"] == "success"
        # Verify all tasks completed successfully
        for task_id, state in result["task_states"].items():
            assert state in ["success", "skipped"], (
                f"Task {task_id} did not succeed or was skipped. State: {state}"
            )

    def test_dag_test_with_executor_branching(self):
        """Test dag.test() with executor for DAG with branching."""
        result = _execute_dag_test("test_dag_test_branching", use_executor=True)

        assert result["success"], f"DAG test failed. Result: {result}"
        assert result["dag_run_state"] == "success"
        # Verify all tasks completed successfully
        for task_id, state in result["task_states"].items():
            assert state in ["success", "skipped"], (
                f"Task {task_id} did not succeed or was skipped. State: {state}"
            )

    def test_dag_test_without_executor_task_groups(self):
        """Test dag.test() without executor for DAG with task groups."""
        result = _execute_dag_test("test_dag_test_task_groups", use_executor=False)

        assert result["success"], f"DAG test failed. Result: {result}"
        assert result["dag_run_state"] == "success"
        # Verify all tasks completed successfully
        for task_id, state in result["task_states"].items():
            assert state == "success", f"Task {task_id} did not succeed. State: {state}"

    def test_dag_test_with_executor_task_groups(self):
        """Test dag.test() with executor for DAG with task groups."""
        result = _execute_dag_test("test_dag_test_task_groups", use_executor=True)

        assert result["success"], f"DAG test failed. Result: {result}"
        assert result["dag_run_state"] == "success"
        # Verify all tasks completed successfully
        for task_id, state in result["task_states"].items():
            assert state == "success", f"Task {task_id} did not succeed. State: {state}"

