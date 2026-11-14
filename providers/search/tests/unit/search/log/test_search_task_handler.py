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

import base64
import json
from dataclasses import dataclass
from unittest import mock

import httpx2
import pytest

from airflow.providers.search.client import BulkIndexError, SearchClient
from airflow.providers.search.log.search_task_handler import (
    SearchRemoteLogIO,
    SearchTaskHandler,
    create_client_from_config,
)

from tests_common.test_utils.config import conf_vars

LOG_ID = "dag-task-run-1-1"


@dataclass
class FakeTI:
    dag_id: str = "dag"
    task_id: str = "task"
    run_id: str = "run"
    map_index: int = 1
    try_number: int = 1


def index_patterns_for(ti):
    return f"logs-{ti.dag_id}"


class FakeCluster:
    """Transport handler serving ``_bulk`` and ``_search`` from an in-memory list of documents."""

    def __init__(self, documents=(), *, missing_index=False):
        self.documents = list(documents)
        self.missing_index = missing_index
        self.requests: list[httpx2.Request] = []

    def __call__(self, request: httpx2.Request) -> httpx2.Response:
        self.requests.append(request)
        if request.url.path == "/_bulk":
            lines = [json.loads(line) for line in request.content.decode().splitlines()]
            self.documents.extend(lines[1::2])
            return httpx2.Response(200, json={"items": [{"index": {"status": 201}}] * (len(lines) // 2)})
        if self.missing_index:
            return httpx2.Response(404, json={"error": {"type": "index_not_found_exception"}})
        body = json.loads(request.content)
        after = body["query"]["bool"]["filter"][0]["range"]["offset"]["gt"]
        hits = [d for d in self.documents if d["offset"] > after][: body["size"]]
        return httpx2.Response(200, json={"hits": {"hits": [{"_source": d} for d in hits]}})


def make_io(tmp_path, cluster, **kwargs) -> SearchRemoteLogIO:
    client = SearchClient("http://search:9200", transport=httpx2.MockTransport(cluster), retry_backoff=0)
    return SearchRemoteLogIO(base_log_folder=tmp_path, client=client, **kwargs)


def write_log(tmp_path, lines) -> str:
    relative = "dag_id=dag/run_id=run/task_id=task/attempt=1.log"
    path = tmp_path / relative
    path.parent.mkdir(parents=True)
    path.write_text("\n".join(lines) + "\n")
    return relative


class TestCreateClientFromConfig:
    @conf_vars({("search", "host"): "https://node-a:9200, https://node-b:9200", ("search", "api_key"): "k"})
    def test_reads_hosts_and_api_key(self):
        client = create_client_from_config()
        assert client.hosts == ["https://node-a:9200", "https://node-b:9200"]
        assert client._client.headers["Authorization"] == "ApiKey k"

    @conf_vars({("search", "host"): "node-a:9200", ("search", "username"): "u", ("search", "password"): "p"})
    def test_reads_basic_auth(self):
        auth = create_client_from_config()._client.auth
        assert isinstance(auth, httpx2.BasicAuth)

    @conf_vars(
        {
            ("search", "cloud_id"): "name:" + base64.b64encode(b"example.com$abc$kb").decode(),
            ("search", "host"): "",
        }
    )
    def test_cloud_id_replaces_host(self):
        assert create_client_from_config().hosts == ["https://abc.example.com:443"]

    @conf_vars({("search", "host"): "", ("search", "cloud_id"): ""})
    def test_requires_host(self):
        with pytest.raises(ValueError, match=r"\[search\] host"):
            create_client_from_config()


@conf_vars(
    {
        ("logging", "base_log_folder"): "/tmp/airflow-logs",
        ("logging", "delete_local_logs"): "True",
        ("search", "host"): "http://search:9200",
        ("search", "write_to_search"): "True",
        ("search", "target_index"): "task-logs",
        ("search", "index_patterns"): "task-logs-*",
        ("search", "max_lines_per_page"): "50",
    }
)
def test_from_config():
    io = SearchRemoteLogIO.from_config()
    assert str(io.base_log_folder) == "/tmp/airflow-logs"
    assert io.delete_local_copy is True
    assert io.write_to_search is True
    assert io.target_index == "task-logs"
    assert io.index_patterns == "task-logs-*"
    assert io.page_size == 50
    assert io.client.hosts == ["http://search:9200"]


class TestUpload:
    def test_indexes_lines_with_log_id_and_offset(self, tmp_path):
        cluster = FakeCluster()
        io = make_io(tmp_path, cluster, write_to_search=True, target_index="task-logs")
        relative = write_log(tmp_path, ['{"event": "start", "level": "info"}', "plain text", "", "[1, 2]"])

        io.upload(relative, FakeTI())

        assert cluster.documents == [
            {"event": "start", "level": "info", "log_id": LOG_ID, "offset": 1},
            {"event": "plain text", "log_id": LOG_ID, "offset": 2},
            {"event": "[1, 2]", "log_id": LOG_ID, "offset": 3},
        ]
        bulk_action = json.loads(cluster.requests[0].content.decode().splitlines()[0])
        assert bulk_action == {"index": {"_index": "task-logs"}}

    def test_deletes_local_log_and_empty_folders(self, tmp_path):
        io = make_io(tmp_path, FakeCluster(), write_to_search=True, delete_local_copy=True)
        relative = write_log(tmp_path, ['{"event": "x"}'])

        io.upload(relative, FakeTI())

        assert list(tmp_path.iterdir()) == []

    def test_keeps_local_log_when_indexing_fails(self, tmp_path):
        io = make_io(tmp_path, FakeCluster(), write_to_search=True, delete_local_copy=True)
        relative = write_log(tmp_path, ['{"event": "x"}'])

        with mock.patch.object(SearchClient, "bulk", autospec=True, side_effect=BulkIndexError([{}])):
            io.upload(relative, FakeTI())

        assert (tmp_path / relative).is_file()

    def test_writes_stdout_without_indexing(self, tmp_path, capsys):
        cluster = FakeCluster()
        io = make_io(tmp_path, cluster, write_stdout=True)
        relative = write_log(tmp_path, ['{"event": "x"}'])

        io.upload(relative, FakeTI())

        assert json.loads(capsys.readouterr().out) == {"event": "x", "log_id": LOG_ID, "offset": 1}
        assert cluster.requests == []

    def test_skips_without_task_instance(self, tmp_path):
        cluster = FakeCluster()
        io = make_io(tmp_path, cluster, write_to_search=True)

        io.upload(write_log(tmp_path, ['{"event": "x"}']), None)

        assert cluster.requests == []


class TestRead:
    def test_pages_through_documents_in_offset_order(self, tmp_path):
        documents = [
            {
                "@timestamp": "2026-10-04T00:00:00Z",
                "message": "one",
                "levelname": "INFO",
                "host": "w1",
                "offset": 1,
            },
            {"event": "two", "level": "info", "host": "w1", "offset": 2, "log_id": LOG_ID},
            {"event": "three", "host": "w1", "offset": 3, "error_detail": []},
        ]
        cluster = FakeCluster(documents)
        io = make_io(tmp_path, cluster, page_size=2, index_patterns="task-logs-*")

        sources, logs = io.read("unused", FakeTI())

        assert sources == [f"{LOG_ID} from w1"]
        assert [json.loads(line) for line in logs] == [
            {"timestamp": "2026-10-04T00:00:00Z", "event": "one", "level": "INFO"},
            {"event": "two", "level": "info"},
            {"event": "three"},
        ]
        assert len(cluster.requests) == 2
        first_query = json.loads(cluster.requests[0].content)
        assert cluster.requests[0].url.path == "/task-logs-*/_search"
        assert first_query["query"]["bool"]["must"] == [{"match_phrase": {"log_id": LOG_ID}}]
        assert first_query["sort"] == [{"offset": "asc"}]

    def test_stream_is_lazy(self, tmp_path):
        cluster = FakeCluster([{"event": str(n), "offset": n} for n in range(1, 4)])
        io = make_io(tmp_path, cluster, page_size=1)

        _, streams = io.stream("unused", FakeTI())

        assert len(cluster.requests) == 1
        assert [json.loads(line)["event"] for line in streams[0]] == ["1", "2", "3"]

    def test_nested_host_field(self, tmp_path):
        cluster = FakeCluster([{"event": "x", "offset": 1, "host": {"name": "w2"}}])
        io = make_io(tmp_path, cluster, host_field="host.name")

        sources, _ = io.read("unused", FakeTI())

        assert sources == [f"{LOG_ID} from w2"]

    @pytest.mark.parametrize("missing_index", [False, True])
    def test_no_documents(self, tmp_path, missing_index):
        io = make_io(tmp_path, FakeCluster(missing_index=missing_index), index_patterns="task-logs")

        sources, logs = io.read("unused", FakeTI())

        assert sources == [f"Log {LOG_ID} not found in task-logs"]
        assert logs == []

    def test_index_patterns_callable(self, tmp_path):
        cluster = FakeCluster()
        io = make_io(tmp_path, cluster, index_patterns_callable=f"{__name__}.index_patterns_for")

        io.read("unused", FakeTI())

        assert cluster.requests[0].url.path == "/logs-dag/_search"


class TestSearchTaskHandler:
    @pytest.mark.parametrize(
        ("frontend", "expected"),
        [
            ("kibana.example.com/{log_id}", f"https://kibana.example.com/{LOG_ID}"),
            ("http://kibana:5601/?q={log_id}", f"http://kibana:5601/?q={LOG_ID}"),
        ],
    )
    def test_external_log_url(self, tmp_path, frontend, expected):
        handler = SearchTaskHandler(base_log_folder=str(tmp_path), frontend=frontend)

        assert handler.supports_external_link is True
        assert handler.get_external_log_url(FakeTI(), 1) == expected

    def test_no_link_without_frontend(self, tmp_path):
        assert SearchTaskHandler(base_log_folder=str(tmp_path)).supports_external_link is False
