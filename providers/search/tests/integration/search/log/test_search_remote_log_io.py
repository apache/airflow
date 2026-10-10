#
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

import contextlib
import json
import uuid
from dataclasses import dataclass

import pytest

from airflow.providers.search.client import NotFoundError, SearchClient
from airflow.providers.search.log.search_task_handler import SearchRemoteLogIO


@dataclass
class FakeTI:
    dag_id: str = "integration_dag"
    task_id: str = "integration_task"
    run_id: str = "integration_run"
    map_index: int = -1
    try_number: int = 1


class _RemoteLogIORoundTrip:
    """Write task logs through ``_bulk`` and read them back, against a real cluster."""

    # Service URL from scripts/ci/docker-compose/integration-<engine>.yml
    host: str

    @pytest.fixture
    def client(self):
        with SearchClient(self.host) as client:
            yield client

    @pytest.fixture
    def index(self, client):
        index = f"airflow-logs-{uuid.uuid4()}"
        yield index
        with contextlib.suppress(NotFoundError):
            client.request("DELETE", f"/{index}")

    def test_round_trip(self, tmp_path, client, index):
        lines = [
            {"event": f"line {n}", "level": "info", "timestamp": "2026-10-04T00:00:00Z"} for n in range(5)
        ]
        log_file = tmp_path / "attempt=1.log"
        log_file.write_text("\n".join(json.dumps(line) for line in lines) + "\n")
        io = SearchRemoteLogIO(
            base_log_folder=tmp_path,
            client=client,
            write_to_search=True,
            target_index=index,
            index_patterns=index,
            page_size=2,
        )

        io.upload(log_file, FakeTI())
        client.request("POST", f"/{index}/_refresh")
        sources, logs = io.read("attempt=1.log", FakeTI())

        assert len(sources) == 1
        assert [json.loads(line) for line in logs] == lines

    def test_missing_index(self, tmp_path, client):
        io = SearchRemoteLogIO(
            base_log_folder=tmp_path, client=client, index_patterns=f"missing-{uuid.uuid4()}"
        )

        sources, logs = io.read("attempt=1.log", FakeTI())

        assert logs == []
        assert "not found" in sources[0]


@pytest.mark.integration("elasticsearch")
class TestElasticsearch(_RemoteLogIORoundTrip):
    host = "http://elasticsearch:9200"


@pytest.mark.integration("opensearch")
class TestOpenSearch(_RemoteLogIORoundTrip):
    host = "http://opensearch:9200"
