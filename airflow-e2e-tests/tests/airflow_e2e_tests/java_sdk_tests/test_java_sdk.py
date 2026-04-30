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

from datetime import datetime, timezone

import pytest
import requests

from airflow_e2e_tests.constants import JAVA_PURE_DAG_ID, JAVA_STUB_DAG_ID
from airflow_e2e_tests.e2e_test_utils.clients import AirflowClient


class TestJavaSDK:
    """End-to-end tests for the Java SDK integration.

    Tests two DAGs that exercise the Java SDK:

    1. Python stub DAG (stub_dag.py):
       python_task_1 -> extract (Java) -> transform (Java) -> python_task_2
       Validates Python-Java interop, XCom flow in both directions, getConnection, getVariable.

    2. Pure Java DAG (JavaExample.java):
       extract -> transform -> load
       Validates pure Java DAG parsing. The load task throws RuntimeException("I failed").
    """

    airflow_client = AirflowClient()

    @pytest.fixture(autouse=True)
    def _create_prerequisites(self):
        """Create the Airflow connection and variable that the Java tasks depend on."""
        try:
            self.airflow_client._make_request(
                method="POST",
                endpoint="connections",
                json={
                    "connection_id": "test_http",
                    "conn_type": "http",
                    "host": "httpbin.org",
                },
            )
        except requests.exceptions.HTTPError as e:
            if e.response.status_code != 409:
                raise

        try:
            self.airflow_client._make_request(
                method="POST",
                endpoint="variables",
                json={
                    "key": "my_variable",
                    "value": "test_value",
                },
            )
        except requests.exceptions.HTTPError as e:
            if e.response.status_code != 409:
                raise

    def test_stub_java_dag(self):
        """Trigger the Python stub DAG once and validate all Python-Java interop.

        stub_dag.py defines: python_task_1 -> extract -> transform -> python_task_2
        All 4 tasks should succeed.
        """
        resp = self.airflow_client.trigger_dag(
            JAVA_STUB_DAG_ID,
            json={"logical_date": datetime.now(timezone.utc).isoformat()},
        )
        run_id = resp["dag_run_id"]
        state = self.airflow_client.wait_for_dag_run(
            dag_id=JAVA_STUB_DAG_ID,
            run_id=run_id,
            timeout=300,
        )

        assert state == "success", f"Stub DAG run expected to succeed, got: {state}"

        # Validate all 4 task instances succeed
        task_instances = self.airflow_client.get_task_instances(JAVA_STUB_DAG_ID, run_id)
        ti_map = {ti["task_id"]: ti["state"] for ti in task_instances["task_instances"]}

        expected_tasks = {"python_task_1", "extract", "transform", "python_task_2"}
        assert expected_tasks.issubset(ti_map.keys()), (
            f"Missing tasks. Expected {expected_tasks}, got {set(ti_map)}"
        )
        for task_id in expected_tasks:
            assert ti_map[task_id] == "success", (
                f"Task '{task_id}' expected 'success', got '{ti_map[task_id]}'"
            )

        # Validate Extract task logs
        extract_logs = str(
            self.airflow_client.get_task_logs(
                dag_id=JAVA_STUB_DAG_ID, run_id=run_id, task_id="extract", try_number=1
            )
        )
        assert "Hello from task" in extract_logs, "Expected 'Hello from task' in extract logs"
        assert "Got XCom from Python Task 'python_task_1'" in extract_logs, (
            "Extract task did not log receiving XCom from python_task_1"
        )
        assert "Got con" in extract_logs, "Extract task did not log getting connection"
        assert "Goodbye from task" in extract_logs, "Expected 'Goodbye from task' in extract logs"

        # Validate Transform task logs
        transform_logs = str(
            self.airflow_client.get_task_logs(
                dag_id=JAVA_STUB_DAG_ID, run_id=run_id, task_id="transform", try_number=1
            )
        )
        assert "Got XCom from 'extract'" in transform_logs, (
            "Transform task did not log receiving XCom from extract"
        )
        assert "Got variable" in transform_logs, "Transform task did not log getting variable"
        assert "Push XCom to python task 2" in transform_logs, "Transform task did not log pushing XCom"

        # Validate XCom flow: Java -> Python (transform -> python_task_2)
        python_task_2_logs = str(
            self.airflow_client.get_task_logs(
                dag_id=JAVA_STUB_DAG_ID, run_id=run_id, task_id="python_task_2", try_number=1
            )
        )
        assert "Pull Java Task" in python_task_2_logs, (
            "python_task_2 did not log pulling XCom from Java transform task"
        )

        # Validate XCom values via API
        python_task_1_xcom = self.airflow_client.get_xcom_value(
            dag_id=JAVA_STUB_DAG_ID, run_id=run_id, task_id="python_task_1", key="return_value"
        )
        assert python_task_1_xcom["value"] == "value-pushed-from-python_task_1", (
            f"python_task_1 XCom expected 'value-pushed-from-python_task_1', got: {python_task_1_xcom['value']}"
        )

        # Java extract and transform tasks push a timestamp (long) via client.setXCom()
        extract_xcom = self.airflow_client.get_xcom_value(
            dag_id=JAVA_STUB_DAG_ID, run_id=run_id, task_id="extract", key="return_value"
        )
        assert extract_xcom["value"] is not None, "extract XCom value should not be None"

        transform_xcom = self.airflow_client.get_xcom_value(
            dag_id=JAVA_STUB_DAG_ID, run_id=run_id, task_id="transform", key="return_value"
        )
        assert transform_xcom["value"] is not None, "transform XCom value should not be None"

    def test_pure_java_dag(self):
        """Trigger the pure Java DAG once and validate JavaExample.java behaviors.

        JavaExample.java defines: extract -> transform -> load
        extract and transform succeed; load throws RuntimeException("I failed").
        """
        resp = self.airflow_client.trigger_dag(
            JAVA_PURE_DAG_ID,
            json={"logical_date": datetime.now(timezone.utc).isoformat()},
        )
        run_id = resp["dag_run_id"]
        state = self.airflow_client.wait_for_dag_run(
            dag_id=JAVA_PURE_DAG_ID,
            run_id=run_id,
            timeout=300,
        )

        # DAG run should fail because load throws RuntimeException
        assert state == "failed", (
            f"Pure Java DAG expected to fail (load throws RuntimeException), got: {state}"
        )

        # Validate 3 task instances with expected states
        task_instances = self.airflow_client.get_task_instances(JAVA_PURE_DAG_ID, run_id)
        ti_map = {ti["task_id"]: ti["state"] for ti in task_instances["task_instances"]}

        expected_states = {
            "extract": "success",
            "transform": "success",
            "load": "failed",
        }
        assert set(expected_states).issubset(ti_map.keys()), (
            f"Missing tasks. Expected {set(expected_states)}, got {set(ti_map)}"
        )
        for task_id, expected_state in expected_states.items():
            assert ti_map[task_id] == expected_state, (
                f"Task '{task_id}' expected '{expected_state}', got '{ti_map[task_id]}'"
            )

        # Validate Extract task logs
        extract_logs = str(
            self.airflow_client.get_task_logs(
                dag_id=JAVA_PURE_DAG_ID, run_id=run_id, task_id="extract", try_number=1
            )
        )
        assert "Hello from task" in extract_logs, "Expected 'Hello from task' in extract logs"
        assert "Goodbye from task" in extract_logs, "Expected 'Goodbye from task' in extract logs"

        # Validate Transform task logs
        transform_logs = str(
            self.airflow_client.get_task_logs(
                dag_id=JAVA_PURE_DAG_ID, run_id=run_id, task_id="transform", try_number=1
            )
        )
        assert "Got XCom from 'extract'" in transform_logs, (
            "Transform task did not log receiving XCom from extract"
        )
        assert "Got variable" in transform_logs, "Transform task did not log getting variable"

        # Validate Load task logs (expected failure)
        load_logs = str(
            self.airflow_client.get_task_logs(
                dag_id=JAVA_PURE_DAG_ID, run_id=run_id, task_id="load", try_number=1
            )
        )
        assert "Got XCom from 'transform'" in load_logs, "Load task did not log receiving XCom from transform"
        assert "I failed" in load_logs, "Load task did not log the expected RuntimeException message"

        # Validate XCom values via API for successful tasks
        extract_xcom = self.airflow_client.get_xcom_value(
            dag_id=JAVA_PURE_DAG_ID, run_id=run_id, task_id="extract", key="return_value"
        )
        assert extract_xcom["value"] is not None, "extract XCom value should not be None"

        transform_xcom = self.airflow_client.get_xcom_value(
            dag_id=JAVA_PURE_DAG_ID, run_id=run_id, task_id="transform", key="return_value"
        )
        assert transform_xcom["value"] is not None, "transform XCom value should not be None"
