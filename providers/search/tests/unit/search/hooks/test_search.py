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

import httpx2
import pytest

from airflow.models.connection import Connection
from airflow.providers.search.client import SearchClient
from airflow.providers.search.hooks.search import SearchHook, SearchSQLCursor


def make_hook(create_connection_without_db, **kwargs) -> SearchHook:
    extra = kwargs.pop("extra", None)
    create_connection_without_db(
        Connection(
            conn_id="search_default",
            conn_type="search",
            extra=json.dumps(extra) if extra is not None else None,
            **kwargs,
        )
    )
    return SearchHook()


class TestClientFromConnection:
    @pytest.mark.parametrize(
        ("connection", "expected"),
        [
            ({"host": "search.local"}, "http://search.local:9200"),
            (
                {"host": "search.local", "port": 9201, "extra": {"scheme": "https"}},
                "https://search.local:9201",
            ),
            ({"host": "https://search.example.com"}, "https://search.example.com"),
            ({"host": "https://search.example.com", "port": 9243}, "https://search.example.com:9243"),
            ({"host": "https://proxy.local", "extra": {"url_prefix": "/es/"}}, "https://proxy.local/es"),
            (
                {"extra": {"cloud_id": "n:" + base64.b64encode(b"example.com$abc$kb").decode()}},
                "https://abc.example.com:443",
            ),
        ],
    )
    def test_host(self, create_connection_without_db, connection, expected):
        assert make_hook(create_connection_without_db, **connection).client.hosts == [expected]

    def test_basic_auth_and_options(self, create_connection_without_db):
        hook = make_hook(
            create_connection_without_db,
            host="search.local",
            login="user",
            password="secret",
            extra={"request_timeout": "30", "max_retries": "1", "headers": {"X-Tenant": "a"}},
        )

        client = hook.client

        assert isinstance(client._client.auth, httpx2.BasicAuth)
        assert client._client.timeout.read == 30
        assert client.max_retries == 1
        assert client._client.headers["X-Tenant"] == "a"

    def test_api_key(self, create_connection_without_db):
        hook = make_hook(create_connection_without_db, host="search.local", extra={"api_key": "k"})
        assert hook.client._client.headers["Authorization"] == "ApiKey k"


class FakeCluster:
    def __init__(self, *responses):
        self.responses = list(responses)
        self.requests: list[httpx2.Request] = []

    def __call__(self, request):
        self.requests.append(request)
        return httpx2.Response(200, json=self.responses.pop(0))

    def bodies(self):
        return [json.loads(r.content) if r.content else None for r in self.requests]


def use_cluster(hook: SearchHook, cluster: FakeCluster) -> None:
    hook.client = SearchClient("http://search:9200", transport=httpx2.MockTransport(cluster))


class TestRestMethods:
    @pytest.fixture
    def hook(self, create_connection_without_db):
        return make_hook(create_connection_without_db, host="search")

    def test_search(self, hook):
        cluster = FakeCluster({"hits": {"hits": []}})
        use_cluster(hook, cluster)

        hook.search({"query": {"match_all": {}}}, "orders", size=5)

        assert cluster.requests[0].url.path == "/orders/_search"
        assert cluster.requests[0].url.params["size"] == "5"

    @pytest.mark.parametrize(
        ("doc_id", "method", "path"), [(None, "POST", "/orders/_doc"), ("a/1", "PUT", "/orders/_doc/a%2F1")]
    )
    def test_index_document(self, hook, doc_id, method, path):
        cluster = FakeCluster({"result": "created"})
        use_cluster(hook, cluster)

        hook.index_document({"n": 1}, "orders", doc_id=doc_id)

        assert cluster.requests[0].method == method
        assert cluster.requests[0].url.raw_path == path.encode()
        assert cluster.bodies() == [{"n": 1}]

    def test_delete_by_id(self, hook):
        cluster = FakeCluster({"result": "deleted"})
        use_cluster(hook, cluster)

        hook.delete("orders", doc_id=3)

        assert (cluster.requests[0].method, cluster.requests[0].url.path) == ("DELETE", "/orders/_doc/3")

    def test_delete_by_query(self, hook):
        cluster = FakeCluster({"deleted": 2})
        use_cluster(hook, cluster)

        hook.delete("orders", query={"query": {"term": {"status": "old"}}})

        assert cluster.requests[0].url.path == "/orders/_delete_by_query"
        assert cluster.bodies() == [{"query": {"term": {"status": "old"}}}]

    def test_delete_needs_query_or_id(self, hook):
        with pytest.raises(ValueError, match="query or doc_id"):
            hook.delete("orders")

    def test_create_index(self, hook):
        cluster = FakeCluster({"acknowledged": True})
        use_cluster(hook, cluster)

        hook.create_index("orders", {"mappings": {}})

        assert (cluster.requests[0].method, cluster.requests[0].url.path) == ("PUT", "/orders")

    @pytest.mark.parametrize(
        ("version", "engine"),
        [
            ({"number": "2.19.1", "distribution": "opensearch"}, "opensearch"),
            ({"number": "9.1.3"}, "elasticsearch"),
        ],
    )
    def test_test_connection(self, hook, version, engine):
        use_cluster(hook, FakeCluster({"version": version}, {"version": version}))
        assert hook.test_connection() == (True, f"Connected to {engine} {version['number']}")

    def test_get_uri_is_unsupported(self, hook):
        with pytest.raises(NotImplementedError):
            hook.get_uri()


class TestSQLCursor:
    def make_cursor(self, engine, *responses, fetch_size=2):
        cluster = FakeCluster(*responses)
        client = SearchClient("http://search:9200", transport=httpx2.MockTransport(cluster))
        return SearchSQLCursor(client, engine, fetch_size), cluster

    def test_elasticsearch_pages_through_cursor(self):
        cursor, cluster = self.make_cursor(
            "elasticsearch",
            {"columns": [{"name": "status", "type": "keyword"}], "rows": [["a"], ["b"]], "cursor": "c1"},
            {"rows": [["c"]]},
        )

        cursor.execute("SELECT status FROM orders WHERE n > ?", [1])

        assert cursor.description[0][:2] == ("status", "keyword")
        assert cursor.fetchall() == [["a"], ["b"], ["c"]]
        assert [r.url.path for r in cluster.requests] == ["/_sql", "/_sql"]
        assert cluster.requests[0].url.params["format"] == "json"
        assert cluster.bodies() == [
            {"query": "SELECT status FROM orders WHERE n > ?", "fetch_size": 2, "params": [1]},
            {"cursor": "c1"},
        ]

    def test_opensearch_jdbc_response(self):
        cursor, cluster = self.make_cursor(
            "opensearch",
            {"schema": [{"name": "n", "type": "long"}], "datarows": [[1]], "cursor": "c1", "total": 2},
            {"datarows": [[2]]},
        )

        cursor.execute("SELECT n FROM orders")

        assert cursor.rowcount == 2
        assert cursor.fetchone() == [1]
        assert cursor.fetchmany(5) == [[2]]
        assert cursor.fetchone() is None
        assert [r.url.path for r in cluster.requests] == ["/_plugins/_sql", "/_plugins/_sql"]

    def test_opensearch_rejects_parameters(self):
        cursor, _ = self.make_cursor("opensearch")
        with pytest.raises(ValueError, match="Only Elasticsearch SQL takes parameters"):
            cursor.execute("SELECT 1", [1])

    def test_close_releases_open_cursor(self):
        cursor, cluster = self.make_cursor(
            "elasticsearch", {"columns": [], "rows": [["a"]], "cursor": "c1"}, {"succeeded": True}
        )

        cursor.execute("SELECT 1")
        cursor.close()

        assert cluster.requests[-1].url.path == "/_sql/close"
        assert cluster.bodies()[-1] == {"cursor": "c1"}

    def test_get_records_through_hook(self, create_connection_without_db):
        hook = make_hook(create_connection_without_db, host="search")
        use_cluster(
            hook,
            FakeCluster(
                {"version": {"number": "9.1.3"}},
                {"columns": [{"name": "n", "type": "long"}], "rows": [[1], [2]]},
            ),
        )

        assert hook.get_records("SELECT n FROM orders") == [[1], [2]]
