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

import pytest

from airflow.models import Connection
from airflow.providers.amazon.aws.hooks.opensearch import OpenSearchHook
from airflow.utils import db


class TestOpenSearchHook:
    def setup_method(self):
        db.merge_conn(
            Connection(
                conn_id="open_search_conn",
                conn_type="open_search",
                host="myhost.opensearch.com",
                login="MyAWSSecretID",
                password="MyAccessKey",
            )
        )

    @pytest.fixture()
    def mock_search(self, monkeypatch):
        def mock_return():
            return {"status": "test"}

        monkeypatch.setattr(OpenSearchHook, "search", mock_return)

    def test_hook_search(self, mock_search):
        hook = OpenSearchHook(open_search_conn_id="open_search_conn", log_query=True)

        result = hook.search(
            index_name="testIndex",
            query={"size": 1, "query": {"multi_match": {"query": "test", "fields": ["testField"]}}},
        )

        assert result
