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

from airflow_e2e_tests.constants import GO_PURE_DAG_ID, GO_STUB_DAG_ID
from airflow_e2e_tests.e2e_test_utils.clients import AirflowClient


class TestGoSDKCoordinator:
    """End-to-end tests for Go SDK bundles through the executable coordinator."""

    airflow_client = AirflowClient()

    @pytest.fixture(autouse=True)
    def _create_prerequisites(self):
        """Create the Airflow connection and variable used by the Go example bundle."""
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

    def test_pure_go_dag(self):
        """Trigger the Dag declared directly by the packed Go executable bundle."""
        resp = self.airflow_client.trigger_dag(
            GO_PURE_DAG_ID,
            json={"logical_date": datetime.now(timezone.utc).isoformat()},
        )
        run_id = resp["dag_run_id"]
        state = self.airflow_client.wait_for_dag_run(
            dag_id=GO_PURE_DAG_ID,
            run_id=run_id,
            timeout=300,
        )

        assert state == "failed", f"Pure Go Dag expected to fail because load fails, got: {state}"

        task_instances = self.airflow_client.get_task_instances(GO_PURE_DAG_ID, run_id)
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

        extract_logs = str(
            self.airflow_client.get_task_logs(
                dag_id=GO_PURE_DAG_ID, run_id=run_id, task_id="extract", try_number=1
            )
        )
        assert "Hello from task" in extract_logs
        assert "Goodbye from task" in extract_logs

        transform_logs = str(
            self.airflow_client.get_task_logs(
                dag_id=GO_PURE_DAG_ID, run_id=run_id, task_id="transform", try_number=1
            )
        )
        assert "Obtained variable" in transform_logs

        load_logs = str(
            self.airflow_client.get_task_logs(
                dag_id=GO_PURE_DAG_ID, run_id=run_id, task_id="load", try_number=1
            )
        )
        assert "Please fail" in load_logs

    def test_stub_go_dag(self):
        """Trigger the Python stub Dag whose tasks resolve to a Go bundle by queue."""
        resp = self.airflow_client.trigger_dag(
            GO_STUB_DAG_ID,
            json={"logical_date": datetime.now(timezone.utc).isoformat()},
        )
        run_id = resp["dag_run_id"]
        state = self.airflow_client.wait_for_dag_run(
            dag_id=GO_STUB_DAG_ID,
            run_id=run_id,
            timeout=300,
        )

        assert state == "failed", f"Stub Go Dag expected to fail because load fails, got: {state}"

        task_instances = self.airflow_client.get_task_instances(GO_STUB_DAG_ID, run_id)
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

        extract_logs = str(
            self.airflow_client.get_task_logs(
                dag_id=GO_STUB_DAG_ID, run_id=run_id, task_id="extract", try_number=1
            )
        )
        assert "Hello from task" in extract_logs
        assert "Goodbye from task" in extract_logs

        transform_logs = str(
            self.airflow_client.get_task_logs(
                dag_id=GO_STUB_DAG_ID, run_id=run_id, task_id="transform", try_number=1
            )
        )
        assert "Obtained variable" in transform_logs

        load_logs = str(
            self.airflow_client.get_task_logs(
                dag_id=GO_STUB_DAG_ID, run_id=run_id, task_id="load", try_number=1
            )
        )
        assert "Please fail" in load_logs
