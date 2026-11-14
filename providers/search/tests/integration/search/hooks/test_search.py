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

import pytest

from airflow.models.connection import Connection
from airflow.providers.search.client import NotFoundError
from airflow.providers.search.hooks.search import SearchHook


class _HookAgainstCluster:
    """Exercise ``SearchHook`` against a real cluster."""

    # Service URL from scripts/ci/docker-compose/integration-<engine>.yml
    host: str
    connection_extra: dict = {}
    engine: str

    @pytest.fixture
    def hook(self, create_connection_without_db):
        create_connection_without_db(
            Connection(
                conn_id="search_default",
                conn_type="search",
                host=self.host,
                extra=json.dumps(self.connection_extra),
            )
        )
        return SearchHook()

    @pytest.fixture
    def index(self, hook):
        # Identifier quoting differs between the two SQL dialects, so the name needs none.
        index = f"orders_{uuid.uuid4().hex}"
        hook.create_index(
            index, {"mappings": {"properties": {"status": {"type": "keyword"}, "n": {"type": "long"}}}}
        )
        yield index
        with contextlib.suppress(NotFoundError):
            hook.client.request("DELETE", f"/{index}")

    def test_engine(self, hook):
        assert hook.engine == self.engine
        assert hook.test_connection()[0] is True

    def test_documents_and_sql(self, hook, index):
        for n, status in enumerate(["ok", "failed", "failed"]):
            hook.index_document({"n": n, "status": status}, index, doc_id=n, refresh="true")

        hits = hook.search({"query": {"term": {"status": "failed"}}, "sort": ["n"]}, index)["hits"]["hits"]
        rows = hook.get_records(f"SELECT n FROM {index} WHERE status = 'failed' ORDER BY n")
        hook.delete(index, doc_id=0)
        hook.client.request("POST", f"/{index}/_refresh")

        assert [hit["_source"]["n"] for hit in hits] == [1, 2]
        assert rows == [[1], [2]]
        assert hook.count(None, index) == 2


@pytest.mark.integration("elasticsearch")
class TestElasticsearch(_HookAgainstCluster):
    host = "http://elasticsearch:9200"
    engine = "elasticsearch"


@pytest.mark.integration("opensearch")
class TestOpenSearch(_HookAgainstCluster):
    host = "http://opensearch:9200"
    engine = "opensearch"
