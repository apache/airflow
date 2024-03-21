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
from opensearchpy import OpenSearch

from airflow.models import Connection
from airflow.providers.opensearch.hooks.opensearch import OpenSearchHook
from airflow.utils import db

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


@pytest.fixture
def mock_hook(monkeypatch):
    monkeypatch.setattr(OpenSearchHook, "search", MockSearch.search)
    monkeypatch.setattr(OpenSearchHook, "client", MockSearch.client)
    monkeypatch.setattr(OpenSearchHook, "index", MockSearch.index)


@pytest.fixture(autouse=True)
def setup_connection():
    # We need to set up a Connection into the database for all tests.
    db.merge_conn(
        Connection(
            conn_id="opensearch_default",
            conn_type="opensearch",
            host="myopensearch.com",
            login="test_user",
            password="test",
        )
    )
