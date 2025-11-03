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

from typing import Any

import pytest

from airflow.models import Connection
from airflow.providers.common.compat.sdk import BaseHook

try:
    from opensearchpy import OpenSearch

    from airflow.providers.opensearch.hooks.opensearch import OpenSearchHook
except ImportError:
    OpenSearch = None  # type: ignore[assignment, misc]
    OpenSearchHook = BaseHook  # type: ignore[assignment,misc]

# TODO: FIXME - those Mocks have overrides that are not used but they also do not make Mypy Happy
# mypy: disable-error-code="override"

MOCK_RETURN = {"status": "test"}


class MockSearch(OpenSearchHook):
    # Mock class to override the Hook for monkeypatching
    def client(self) -> OpenSearch:
        return OpenSearch()

    def search(self, query: dict, index_name: str, **kwargs: Any) -> Any:
        return MOCK_RETURN

    def index(self, document: dict, index_name: str, doc_id: int, **kwargs: Any) -> Any:
        return doc_id


class MockClient:
    def count(self, index: Any = None, body: Any = None):
        return {"count": 1, "_shards": {"total": 1, "successful": 1, "skipped": 0, "failed": 0}}

    def search(self, index=None, body=None, sort=None, size=None, from_=None):
        return self.sample_log_response()

    def sample_log_response(self):
        return {
            "_shards": {"failed": 0, "skipped": 0, "successful": 7, "total": 7},
            "hits": {
                "hits": [
                    {
                        "_id": "jdeZT4kBjAZqZnexVUxk",
                        "_source": {
                            "dag_id": "example_bash_operator",
                            "execution_date": "2023_07_09T07_47_32_000000",
                            "levelname": "INFO",
                            "message": "Some Message 1",
                            "event": "Some Message 1",
                            "task_id": "run_after_loop",
                            "try_number": "1",
                            "offset": 0,
                        },
                        "_type": "_doc",
                    },
                    {
                        "_id": "qteZT4kBjAZqZnexVUxl",
                        "_source": {
                            "dag_id": "example_bash_operator",
                            "execution_date": "2023_07_09T07_47_32_000000",
                            "levelname": "INFO",
                            "message": "Another Some Message 2",
                            "event": "Another Some Message 2",
                            "task_id": "run_after_loop",
                            "try_number": "1",
                            "offset": 1,
                        },
                        "_type": "_doc",
                    },
                ],
                "max_score": 2.482621,
                "total": {"relation": "eq", "value": 36},
            },
            "timed_out": False,
            "took": 7,
        }


@pytest.fixture
def mock_hook(monkeypatch):
    monkeypatch.setattr(OpenSearchHook, "search", MockSearch.search)
    monkeypatch.setattr(OpenSearchHook, "client", MockSearch.client)
    monkeypatch.setattr(OpenSearchHook, "index", MockSearch.index)


@pytest.fixture(autouse=True)
def setup_connection(create_connection_without_db):
    create_connection_without_db(
        Connection(
            conn_id="opensearch_default",
            conn_type="opensearch",
            host="myopensearch.com",
            login="test_user",
            password="test",
        )
    )
