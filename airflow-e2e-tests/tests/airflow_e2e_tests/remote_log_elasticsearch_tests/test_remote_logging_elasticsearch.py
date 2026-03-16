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

from airflow_e2e_tests.e2e_test_utils.clients import AirflowClient, get_elasticsearch_session


class TestRemoteLoggingElasticsearch:
    airflow_client = AirflowClient()
    dag_id = "example_xcom_test"
    task_id = "bash_pull"
    retry_interval_in_seconds = 5
    max_retries = 12
    target_index = "airflow-e2e-logs"
    expected_message = "finished"
    elasticsearch_url = "http://localhost:9200"
    expected_log_id_prefix = f"{dag_id}-{task_id}-"

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

    def test_remote_logging_elasticsearch(self):
        """Test that a DAG using remote logging to Elasticsearch completes successfully."""

        self.airflow_client.un_pause_dag(TestRemoteLoggingElasticsearch.dag_id)

        resp = self.airflow_client.trigger_dag(
            TestRemoteLoggingElasticsearch.dag_id,
            json={"logical_date": datetime.now(timezone.utc).isoformat()},
        )
        run_id = resp["dag_run_id"]
        state = self.airflow_client.wait_for_dag_run(
            dag_id=TestRemoteLoggingElasticsearch.dag_id,
            run_id=run_id,
        )

        assert state == "success", (
            f"DAG {TestRemoteLoggingElasticsearch.dag_id} did not complete successfully. Final state: {state}"
        )

        elasticsearch_session = get_elasticsearch_session()
        matching_logs = []
        for _ in range(self.max_retries):
            response = elasticsearch_session.post(
                f"{self.elasticsearch_url}/{self.target_index}/_search",
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
                f"Expected Elasticsearch logs for run_id {run_id} in index {self.target_index}, "
                f"but none contained log_id starting with {self.expected_log_id_prefix!r} and event "
                f"or message containing {self.expected_message!r}"
            )

        task_logs = self.airflow_client.get_task_logs(
            dag_id=TestRemoteLoggingElasticsearch.dag_id,
            task_id=self.task_id,
            run_id=run_id,
        )

        events = [item.get("event", "") for item in task_logs.get("content", []) if isinstance(item, dict)]
        assert any(self.expected_message in event for event in events), (
            f"Expected task logs to contain {self.expected_message!r}, got events: {events}"
        )
