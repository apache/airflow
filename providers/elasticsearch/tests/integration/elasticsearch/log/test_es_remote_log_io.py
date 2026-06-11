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

import dataclasses
import json
import uuid
from unittest.mock import patch

import elasticsearch
import pytest

from airflow.providers.elasticsearch.log.es_task_handler import ElasticsearchRemoteLogIO, _render_log_id

# The ES service hostname as defined in scripts/ci/docker-compose/integration-elasticsearch.yml
ES_HOST = "http://elasticsearch:9200"


@dataclasses.dataclass
class _MockTI:
    """Minimal TaskInstance-like object satisfying the RuntimeTI protocol for log ID rendering."""

    dag_id: str = "integration_test_dag"
    task_id: str = "integration_test_task"
    run_id: str = "integration_test_run"
    try_number: int = 1
    map_index: int = -1


@pytest.mark.integration("elasticsearch")
class TestElasticsearchRemoteLogIOIntegration:
    """
    Integration tests for ElasticsearchRemoteLogIO using the breeze elasticsearch service.

    These tests require the elasticsearch integration to be running:
        breeze testing providers-integration-tests --integration elasticsearch
    """

    @pytest.fixture(autouse=True)
    def setup(self, tmp_path):
        # Use a unique index per test to avoid cross-test data pollution
        self.target_index = f"airflow-logs-{uuid.uuid4()}"
        self.elasticsearch_io = ElasticsearchRemoteLogIO(
            write_to_es=True,
            write_stdout=False,
            delete_local_copy=False,
            host=ES_HOST,
            base_log_folder=tmp_path,
        )
        self.elasticsearch_io.target_index = self.target_index
        # Point index_patterns at the unique index so reads don't scan unrelated indices
        self.elasticsearch_io.index_patterns = self.target_index
        self.elasticsearch_io.client = elasticsearch.Elasticsearch(ES_HOST)

    @pytest.fixture
    def ti(self):
        return _MockTI()

    @pytest.fixture
    def tmp_json_log_file(self, tmp_path):
        log_file = tmp_path / "1.log"
        sample_logs = [
            {"message": "start"},
            {"message": "processing"},
            {"message": "end"},
        ]
        with open(log_file, "w") as f:
            for log in sample_logs:
                f.write(json.dumps(log) + "\n")
        return log_file

    @patch(
        "airflow.providers.elasticsearch.log.es_task_handler.TASK_LOG_FIELDS",
        ["message"],
    )
    def test_upload_and_read(self, tmp_json_log_file, ti):
        """Verify that logs uploaded to ES can be retrieved correctly."""
        self.elasticsearch_io.upload(tmp_json_log_file, ti)
        # Force index refresh so the documents are immediately searchable
        self.elasticsearch_io.client.indices.refresh(index=self.target_index)

        log_source_info, log_messages = self.elasticsearch_io.read("", ti)

        assert log_source_info[0] == ES_HOST
        assert len(log_messages) == 3

        expected_messages = ["start", "processing", "end"]
        for expected, log_message in zip(expected_messages, log_messages):
            log_entry = json.loads(log_message)
            assert "event" in log_entry
            assert log_entry["event"] == expected

    def test_read_missing_log(self, ti):
        """Verify that a missing log returns the expected error message.

        The index must exist first — _es_read raises NotFoundError (not returns None)
        when the index itself is absent. We create the index explicitly so ES returns
        count=0 (log not found) rather than a 404 (index not found).
        """
        self.elasticsearch_io.client.indices.create(index=self.target_index)

        log_source_info, log_messages = self.elasticsearch_io.read("", ti)

        assert log_source_info == []
        assert len(log_messages) == 1
        assert "not found in Elasticsearch" in log_messages[0]

    def test_read_error_detail_integration(self, ti):
        """Verify that error_detail is correctly retrieved and formatted in integration tests."""
        # Manually index a log entry with error_detail
        error_detail = [
            {
                "is_cause": False,
                "frames": [{"filename": "/opt/airflow/dags/fail.py", "lineno": 13, "name": "log_and_raise"}],
                "exc_type": "RuntimeError",
                "exc_value": "Woopsie. Something went wrong.",
            }
        ]
        body = {
            "event": "Task failed with exception",
            "log_id": _render_log_id(self.elasticsearch_io.log_id_template, ti, ti.try_number),
            "offset": 1,
            "error_detail": error_detail,
        }
        self.elasticsearch_io.client.index(index=self.target_index, document=body)
        self.elasticsearch_io.client.indices.refresh(index=self.target_index)

        log_source_info, log_messages = self.elasticsearch_io.read("", ti)

        assert len(log_messages) == 1
        log_entry = json.loads(log_messages[0])
        assert "error_detail" in log_entry
        assert log_entry["error_detail"] == error_detail
