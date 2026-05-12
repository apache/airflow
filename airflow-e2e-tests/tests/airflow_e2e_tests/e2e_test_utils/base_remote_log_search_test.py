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

import pytest

from airflow_e2e_tests.e2e_test_utils.clients import AirflowClient, create_request_session_with_retries


class BaseRemoteLoggingSearchTest:
    """
    Base class for remote logging e2e tests against search backends (Elasticsearch, OpenSearch).

    Subclasses must set:
        - ``search_url``: base URL of the search backend, e.g. ``"http://localhost:9200"``
    """

    airflow_client = AirflowClient()
    dag_id = "example_xcom_test"
    task_id = "bash_pull"
    retry_interval_in_seconds = 5
    max_retries = 12
    target_index = "airflow-e2e-logs"
    expected_message = "finished"
    search_url: str  # set by subclass, e.g. "http://localhost:9200"
    expected_log_id_prefix = f"{dag_id}-{task_id}-"

    def _get_session(self):
        return create_request_session_with_retries(status_forcelist=[429, 500, 502, 503, 504])

    def _matches_expected_log(self, log_source: dict, run_id: str) -> bool:
        log_id = log_source.get("log_id", "")
        log_message = log_source.get("event", "")
        if not isinstance(log_message, str):
            log_message = log_source.get("message", "")
        return (
            log_id.startswith(self.expected_log_id_prefix)
            and run_id in log_id
            and self.expected_message in log_message
        )

    def test_remote_logging(self):
        """Test that a DAG using remote logging to the search backend completes successfully."""
        self.airflow_client.un_pause_dag(self.dag_id)

        resp = self.airflow_client.trigger_dag(
            self.dag_id,
            json={"logical_date": datetime.now(timezone.utc).isoformat()},
        )
        run_id = resp["dag_run_id"]
        state = self.airflow_client.wait_for_dag_run(
            dag_id=self.dag_id,
            run_id=run_id,
        )

        assert state == "success", f"DAG {self.dag_id} did not complete successfully. Final state: {state}"

        session = self._get_session()
        matching_logs = []
        for _ in range(self.max_retries):
            response = session.post(
                f"{self.search_url}/{self.target_index}/_search",
                json={"size": 200, "query": {"match_all": {}}},
            )
            if response.status_code == 404:
                time.sleep(self.retry_interval_in_seconds)
                continue
            response.raise_for_status()
            hits = response.json()["hits"]["hits"]
            matching_logs = [
                hit["_source"] for hit in hits if self._matches_expected_log(hit["_source"], run_id)
            ]
            if matching_logs:
                break
            time.sleep(self.retry_interval_in_seconds)

        if not matching_logs:
            pytest.fail(
                f"Expected search backend logs for run_id {run_id} in index {self.target_index}, "
                f"but none contained log_id starting with {self.expected_log_id_prefix!r} and event "
                f"or message containing {self.expected_message!r}"
            )

        task_logs = self.airflow_client.get_task_logs(
            dag_id=self.dag_id,
            task_id=self.task_id,
            run_id=run_id,
        )

        events = [item.get("event", "") for item in task_logs.get("content", []) if isinstance(item, dict)]
        assert any(self.expected_message in event for event in events), (
            f"Expected task logs to contain {self.expected_message!r}, got events: {events}"
        )

    def test_remote_logging_error_detail(self):
        """Test that log error_detail is retrieved correctly from the search backend."""
        dag_id = "example_failed_dag"
        task_id = "fail_task"

        self.airflow_client.un_pause_dag(dag_id)
        resp = self.airflow_client.trigger_dag(
            dag_id,
            json={"logical_date": datetime.now(timezone.utc).isoformat()},
        )
        run_id = resp["dag_run_id"]
        state = self.airflow_client.wait_for_dag_run(dag_id=dag_id, run_id=run_id)

        assert state == "failed"

        task_logs_content = []
        for _ in range(self.max_retries):
            task_logs_resp = self.airflow_client.get_task_logs(
                dag_id=dag_id,
                task_id=task_id,
                run_id=run_id,
            )
            task_logs_content = task_logs_resp.get("content", [])
            if any("error_detail" in item for item in task_logs_content if isinstance(item, dict)):
                break
            time.sleep(self.retry_interval_in_seconds)

        error_entries = [
            item for item in task_logs_content if isinstance(item, dict) and "error_detail" in item
        ]
        assert len(error_entries) > 0, (
            f"Expected error_detail in logs, but none found. Logs: {task_logs_content}"
        )

        error_detail = error_entries[0]["error_detail"]
        assert isinstance(error_detail, list), f"Expected error_detail to be a list, got {type(error_detail)}"
        assert len(error_detail) > 0, "Expected error_detail to have at least one exception"
        assert error_detail[0]["exc_type"] == "RuntimeError"
        assert "This is a test exception for stacktrace rendering" in error_detail[0]["exc_value"]
