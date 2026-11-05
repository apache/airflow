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

import pytest

from airflow.providers.opensearch.log.os_task_handler import OpensearchRemoteLogIO, _render_log_id

opensearchpy = pytest.importorskip("opensearchpy")

# The OpenSearch service hostname as defined in scripts/ci/docker-compose/integration-opensearch.yml
OS_HOST = "http://opensearch"


@dataclasses.dataclass
class _MockTI:
    """Minimal TaskInstance-like object satisfying the RuntimeTI protocol for log ID rendering."""

    dag_id: str = "integration_test_dag"
    task_id: str = "integration_test_task"
    run_id: str = "integration_test_run"
    try_number: int = 1
    map_index: int = -1
    id: uuid.UUID = dataclasses.field(default_factory=uuid.uuid4)


@pytest.mark.integration("opensearch")
class TestOpensearchRemoteLogIOIntegration:
    """
    Integration tests for OpensearchRemoteLogIO using the breeze opensearch service.

    These tests require the opensearch integration to be running:
        breeze testing providers-integration-tests --integration opensearch
    """

    @pytest.fixture(autouse=True)
    def setup(self, tmp_path):
        self.target_index = f"airflow-logs-{uuid.uuid4().hex}"
        self.opensearch_io = OpensearchRemoteLogIO(
            write_to_opensearch=True,
            write_stdout=False,
            delete_local_copy=False,
            host=OS_HOST,
            port=9200,
            username="",
            password="",
            base_log_folder=tmp_path,
            target_index=self.target_index,
        )
        self.opensearch_io.index_patterns = self.target_index
        self.opensearch_io.client = opensearchpy.OpenSearch(
            hosts=[{"host": "opensearch", "port": 9200, "scheme": "http"}]
        )

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
        log_file.write_text("\n".join(json.dumps(log) for log in sample_logs) + "\n")
        return log_file

    @patch(
        "airflow.providers.opensearch.log.os_task_handler.TASK_LOG_FIELDS",
        ["message"],
    )
    def test_upload_and_read(self, tmp_json_log_file, ti):
        self.opensearch_io.upload(tmp_json_log_file, ti)
        self.opensearch_io.client.indices.refresh(index=self.target_index)

        log_source_info, log_messages = self.opensearch_io.read("", ti)

        assert log_source_info[0] == OS_HOST
        assert len(log_messages) == 3

        expected_messages = ["start", "processing", "end"]
        for expected, log_message in zip(expected_messages, log_messages):
            log_entry = json.loads(log_message)
            assert "event" in log_entry
            assert log_entry["event"] == expected

    @patch(
        "airflow.providers.opensearch.log.os_task_handler.TASK_LOG_FIELDS",
        ["message"],
    )
    def test_read_returns_all_logs_when_exceeding_page_size(self, ti, tmp_path):
        log_file = tmp_path / "large.log"
        sample_logs = [{"message": f"log line {i}"} for i in range(1500)]
        log_file.write_text("\n".join(json.dumps(log) for log in sample_logs) + "\n")

        self.opensearch_io.upload(log_file, ti)
        self.opensearch_io.client.indices.refresh(index=self.target_index)

        _, log_messages = self.opensearch_io.read("", ti)

        assert len(log_messages) == 1500
        assert json.loads(log_messages[0])["event"] == "log line 0"
        assert json.loads(log_messages[-1])["event"] == "log line 1499"

    def test_read_missing_log(self, ti):
        self.opensearch_io.client.indices.create(index=self.target_index)

        log_source_info, log_messages = self.opensearch_io.read("", ti)

        assert log_source_info == []
        assert len(log_messages) == 1
        assert "not found in Opensearch" in log_messages[0]

    def test_read_error_detail_integration(self, ti):
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
            "log_id": _render_log_id(self.opensearch_io.log_id_template, ti, ti.try_number),
            "offset": 1,
            "error_detail": error_detail,
        }
        self.opensearch_io.client.index(index=self.target_index, body=body)
        self.opensearch_io.client.indices.refresh(index=self.target_index)

        log_source_info, log_messages = self.opensearch_io.read("", ti)

        assert log_source_info[0] == OS_HOST
        assert len(log_messages) == 1
        log_entry = json.loads(log_messages[0])
        assert "error_detail" in log_entry
        assert log_entry["error_detail"] == error_detail

    @pytest.mark.parametrize(
        ("written", "reader", "expected"),
        [
            pytest.param(["legacy"], "x", ["legacy"], id="pre-upgrade-logs-stay-readable"),
            pytest.param(["x", "y"], "x", ["x"], id="post-upgrade-reader-sees-only-its-try"),
            pytest.param(["x", "y"], "y", ["y"], id="post-upgrade-other-try"),
            pytest.param(["legacy", "x", "y"], "x", ["legacy", "x"], id="mixed-index"),
        ],
    )
    @patch(
        "airflow.providers.opensearch.log.os_task_handler.TASK_LOG_FIELDS",
        ["message"],
    )
    def test_read_scopes_to_try_and_keeps_pre_upgrade_logs(self, tmp_path, written, reader, expected):
        """Tries sharing one log_id stay separate, and entries without ti_id stay readable."""
        try_ids = {"x": uuid.uuid4(), "y": uuid.uuid4()}
        log_id = _render_log_id(self.opensearch_io.log_id_template, _MockTI(), 1)
        for name in written:
            if name == "legacy":
                legacy_entry = {"message": name, "log_id": log_id, "offset": 1}
                self.opensearch_io.client.index(index=self.target_index, body=legacy_entry)
                continue
            log_file = tmp_path / f"{name}.log"
            log_file.write_text(json.dumps({"message": name}) + "\n")
            self.opensearch_io.upload(log_file, _MockTI(id=try_ids[name]))
        self.opensearch_io.client.indices.refresh(index=self.target_index)

        _, log_messages = self.opensearch_io.read("", _MockTI(id=try_ids[reader]))

        assert sorted(json.loads(message)["event"] for message in log_messages) == sorted(expected)
